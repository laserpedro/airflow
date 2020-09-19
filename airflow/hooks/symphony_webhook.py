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



from datetime import datetime

import json
import socket

from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException


class SymphonyWebHook(HttpHook):
    """
    This hooks allow you to post message to Symphony using webhooks.
    Takes both the Symphony webhook or a conn id. If the http conn id is provided then
    the hook will use the webhook token provided in the extra.
    You can pass the token of the chat room directly either.

    :param http_conn_id: connection that has the Symphony webhook on its extra field
    :type hhtp_conn_id: str
    :param webhook_token: Symphony webhook token
    :type webhook_token: str
    :param symph_type: default to 'com.symphony.integration.test.event'
    :type symph_type: str
    :param symph_version: default to '2.0'
    :type symh_version: str
    """

    def __init__(self,
                 http_conn_id,
                 webhook_token=None,
                 symph_type='com.symphony.integration.test.event',
                 symph_version='2.0',
                 *args, **kwargs
                 ):

        super(SymphonyWebHook, self).__init__(method='POST',
                                              http_conn_id=http_conn_id,
                                              *args,
                                              **kwargs)

        self.webhook_token = self._get_token(http_conn_id, webhook_token)
        self.symph_type = symph_type
        self.symph_version = symph_version

    def _get_token(self, http_conn_id, token=None):
        """
        Return either the complete route to the chat room if provided or
        search for the webhook token stored in the extra of the connection.

        :param http_conn_id: connection that has the Symphony webhook on its extra field
        :type http_conn_id: str
        :param token: token to the Symphony chat room
        :type token: str
        """

        if token:
            return token

        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson

            try:
                return extra['webhook_token']
            except KeyError:
                raise AirflowException('You indicated to use the webhook in the '
                                       'connection extras but passed none')
        else:
            raise AirflowException('Cannot get token: no Symphony '
                                   'webhook token nor conn_id provided')

    @property
    def _msg_pattern(self):
        return """<messageML>
                 <span>${entity['Message'].message.body}</span>
                 </messageML>"""

    def _build_task_message(self,
                            emoji,
                            task_status,
                            task_id,
                            dag_id,
                            execution_date,
                            log_url):
        """
        Build a standard log message for a task instance:
        :param emoji: emoji to display
        :type emoji: str
        :param task_status: the status of the task
        :type task_status: str
        :param task_id:
        :type task_id:
        :param dag_id:
        :type dag_id:
        :param execution_date: date of task instance execution
        :type execution_date: str
        :param log_url: url to access the task instance log
        :type log_url: str
        """

        msg_body = """<emoji shortcode="{}"></emoji>
        <b>Task Status: </b>{}<br/>
        <b>Task Id: </b>{}<br/>
        <b>Dag Id: </b>{}<br/>
        <b>Execution Date: </b>{}<br/>
        <b>Log Url: </b><a href="{}"/>
        <br/>
        """.format(emoji, task_status, task_id, dag_id, execution_date, log_url)

        payload = {'Message':
                      {'type': self.symph_type,
                       'version': self.symph_version,
                       'message': {'type': "com.symphony.integration.test.message",
                                   'version': self.symph_version,
                                   'header': 'Airflow',
                                   'body': msg_body}
                       }
                  }

        return json.dumps(payload)

    def _build_symphony_message(self,
                                msg,
                                header=None,
                                emoji=None,
                                username=None,
                                date=False,
                                time=False,
                                hostname=None,
                                date_fmt='%Y-%m-%d %H:%M:%S.%f'):
        """
        Send a simple symphony message

        :param msg: message to be sent
        :type msg: str
        :param header: header to be added to the message
        :type header: str
        :param emoji: emoji to be added to the message
        :type emoji: str
        :param username: name that will be used to identify who sent the
        message
        :type username: str
        :param date: if True, will add the current date in the message
        :type date: bool
        :param time: if True, will add the current date and time in the
        message
        :type time: bool
        :param hostname: if True, will add the name of the machine sending the
        message
        :type hostname: bool
        :param date_fmt: used to format date and time information
        """

        msg_body = msg

        if sender:
            msg_body = f'{username}: {msg_body}'

        if emoji:
            msg_body = f"<emoji shortcode='{emoji}'></emoji> " + msg_body

        if user:
            msg_body += f' - by {user}'

        if all((date, time)):
            msg_body += f' @ {datetime.now().strftime(date_fmt)}'
        elif date and not time:
            msg_body += f' @ {datetime.now().date().strftime(date_fmt)}'

        if hostname:
            msg_body += ' on {}'.format(socket.gethostname())

        payload = {'Message':
                      {'type': self.symph_type,
                       'version': self.symph_version,
                       'message': {'type': "com.symphony.integration.test.message",
                                   'version': self.symph_version,
                                   'header': header,
                                   'body': msg_body}
                       }
                  }

        return json.dumps(payload)

    def send_simple(self,
                    msg,
                    header=None,
                    emoji=None,
                    username=None,
                    user=None,
                    date=True,
                    time=True,
                    hostname=None,
                    **kwargs):
        """
        Send a simple message to the chat room
        """

        symph_msg = self._build_symphony_message(msg,
                                                 header=header,
                                                 emoji=emoji,
                                                 username=sender,
                                                 date=user,
                                                 time=date,
                                                 hostname=hostname,
                                                 **kwargs)

        self.run(endpoint=self.webhook_token,
                 files=dict(data=symph_msg,
                 message=self._msg_pattern))

    def send_task_log(self,
                      emoji,
                      task_status,
                      task_id,
                      dag_id,
                      execution_date,
                      log_url):
        """
        Send a task log pre-formatted message to the chat room

        :param emoji: emoji to display
        :type emoji: str
        :param task_status: the status of the task
        :type task_status: str
        :param task_id: id of the airflow task
        :type task_id: str
        :param dag_id: id of the airflow dag
        :type dag_id: str
        :param execution_date: date of task instance execution
        :type execution_date: str
        :param log_url: url to access the task instance log
        :type log_url: str
        """

        task_msg = self._build_task_message(emoji,
                                            task_status,
                                            task_id,
                                            dag_id,
                                            execution_date,
                                            log_url)

        self.run(endpoint=self.webhook_token,
                 files=dict(data=task_msg, message=self._msg_pattern))
