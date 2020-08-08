#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
from contextlib import closing

import psycopg2
import psycopg2.extensions
import psycopg2.extras

import pandas as pd

from airflow.hooks.dbapi_hook import DbApiHook


class PostgresHook(DbApiHook):
    """
    Interact with Postgres.

    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    Also you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
    psycopg2.extras for more details.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    For Redshift, also use redshift in the extra connection parameters and
    set it to true. The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``
    """
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.connection = kwargs.pop("connection", None)
        self.conn = None

    def _get_cursor(self, raw_cursor):
        _cursor = raw_cursor.lower()
        if _cursor == 'dictcursor':
            return psycopg2.extras.DictCursor
        if _cursor == 'realdictcursor':
            return psycopg2.extras.RealDictCursor
        if _cursor == 'namedtuplecursor':
            return psycopg2.extras.NamedTupleCursor
        raise ValueError('Invalid cursor passed {}'.format(_cursor))

    def get_conn(self):

        conn_id = getattr(self, self.conn_name_attr)
        conn = self.connection or self.get_connection(conn_id)

        # check for authentication via AWS IAM
        if conn.extra_dejson.get('iam', False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port)
        raw_cursor = conn.extra_dejson.get('cursor', False)
        if raw_cursor:
            conn_args['cursor_factory'] = self._get_cursor(raw_cursor)
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in ['sslmode', 'sslcert', 'sslkey',
                            'sslrootcert', 'sslcrl', 'application_name',
                            'keepalives_idle']:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def copy_expert(self, sql, filename):
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        if not os.path.isfile(filename):
            with open(filename, 'w'):
                pass

        with open(filename, 'r+') as file:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, file)
                    file.truncate(file.tell())
                    conn.commit()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        self.copy_expert("COPY {table} FROM STDIN".format(table=table), tmp_file)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        self.copy_expert("COPY {table} TO STDOUT".format(table=table), tmp_file)

    # pylint: disable=signature-differs
    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.
    
        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
 
        if cell is None or pd.isnull(cell):
            return None

        if isinstance(cell, datetime):
            return cell.isoformat()
  
        return str(cell)
            
    def get_iam_token(self, conn):
        """
        Uses AWSHook to retrieve a temporary password to connect to Postgres
        or Redshift. Port is required. If none is provided, default is used for
        each service
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        redshift = conn.extra_dejson.get('redshift', False)
        aws_conn_id = conn.extra_dejson.get('aws_conn_id', 'aws_default')
        aws_hook = AwsBaseHook(aws_conn_id, client_type='rds')
        login = conn.login
        if conn.port is None:
            port = 5439 if redshift else 5432
        else:
            port = conn.port
        if redshift:
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get('cluster-identifier', conn.host.split('.')[0])
            client = aws_hook.get_client_type('redshift')
            cluster_creds = client.get_cluster_credentials(
                DbUser=conn.login,
                DbName=self.schema or conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False)
            token = cluster_creds['DbPassword']
            login = cluster_creds['DbUser']
        else:
            token = aws_hook.conn.generate_db_auth_token(conn.host, port, conn.login)
        return login, token, port

    @staticmethod
    def _generate_insert_sql(table, values, target_fields, replace, **kwargs):
        """
        Static helper method that generate the INSERT SQL statement.
        Since the The REPLACE variant is specific to MySQL syntax, the below
        is an adpatation to the postre syntax

        :param table: Name of the target table
        :type table: str
        :param values: The row to insert into the table
        :type values: list or tuple of values to insert
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :type replace_index: str or list
        :return: The generated INSERT or REPLACE SQL statement
        :rtype: str
        """
        placeholders = ["%s", ] * len(values)
        replace_index = kwargs.get("replace_index", None)

        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = "({})".format(target_fields_fragment)
        else:
            target_fields_fragment = ''

        sql = "INSERT INTO {0} {1} VALUES ({2})".format(
            table,
            target_fields_fragment,
            ",".join(placeholders))

        if replace:
            if target_fields is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if replace_index is None:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(replace_index, str):
                replace_index = [replace_index]
            replace_index_set = set(replace_index)

            replace_target = [
                "{0} = excluded.{0}".format(col)
                for col in target_fields
                if col not in replace_index_set
            ]
            sql += " ON CONFLICT ({0}) DO UPDATE SET {1}".format(
                ", ".join(replace_index),
                ", ".join(replace_target),
            )
        return sql
    
    def insert_df(self, df, table, source_cols=None, index=False, target_fields=None,
                  replace=False, replace_index=None, commit_every=1000, **kwargs):
        """
        Function to directly insert a dataframe into the target table

        :param df: dataframe to insert
        :type df: pd.DataFrame
        :param table: name of the db table
            Add the prefix 'schema_name.' if inserting into a schema
        :type table: str
        :param source_cols:
        :type source_cols: list
        :param index: if True will insert the index vals
        :type index: Bool
        :param target_fields:
        :type target_fields:
        :param replace:
        :type replace: Bool
        :param replace_index:
        :type replace_index: list
        """
  
        rows = []

        if source_cols:
            _df = df.loc[:, source_cols].copy()
        else:
            _df = df.copy()
 
        if isinstance(_df.index[0], tuple):
            idx_len = len(_df.index[0])
        else:
            idx_len = 1

        for row in _df.itertuples():
            if index:
                idx_val = row[0]
                if isinstance(idx_val, tuple):
                    flat_row = row[0] + row[1:]
                else:
                    flat_row = row
                rows.append(flat_row)
            else:
                rows.append(row[idx_len:])
    
        return self.insert_rows(table, rows, target_fields, commit_every=commit_every,
                         replace=replace, replace_index=replace_index)
