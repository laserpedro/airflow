"""
author: pnouhaud
Created on 2020-09-08 11.16.0
"""


from datetime import datetime

import json
import socket

from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException


class SymphonyWebHook(HttpHook):

    _valid_status = ('success', 'failed', 'warning')

    def __init__(self,
                 http_conn_id,
                 webhook_token=None,
                 symph_type='com.symphony.integration.test.event',
                 symph_version='2.0',
                 method='POST',
                 *args, **kwargs
                 ):

        super(SymphonyWebHook, self).__init__(method=method,
                                              http_conn_id=http_conn_id,
                                              *args,
                                              **kwargs)
        self.webhook_token = self._get_token(webhook_token, http_conn_id)
        self.symph_type = symph_type
        self.symph_version = symph_version

    def _get_token(self, token, http_conn_id):

        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get('webhook_token', '')
        else:
            raise AirflowException('Cannot get token: no Valid Symphony '
                                   'webhook token nor conn_id provided')

    @property
    def _msg_pattern(self):
        return """<messageML>
                 <span>${entity['Message'].message.body}</span>
                 </messageML>"""

    def _build_symphony_message(self,
                                msg,
                                table=None,
                                header=None,
                                emoji=None,
                                sender=None,
                                user=None,
                                date=None,
                                time=None,
                                hostname=None,
                                date_fmt='%Y-%m-%d %H:%M:%S.%f'):

        msg_body = msg

        if sender:
            msg_body = f'{sender}: {msg_body}'

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


    def execute(self,
                msg,
                table=None,
                header=None,
                emoji=None,
                sender=None,
                user=None,
                date=None,
                time=None,
                hostname=None):
        """
        Execute the Symphony webhook call 
        """

        symph_msg = self._build_symphony_message(msg,
                                                 table=table,
                                                 header=header,
                                                 emoji=emoji,
        	                                     sender=sender,
        	                                     user=user,
        	                                     date=date,
        	                                     time=time,
        	                                     hostname=hostname)

        self.run(endpoint=self.webhook_token,
                 files=dict(data=symph_msg, message=self._msg_pattern))
