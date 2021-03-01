import datetime
import json
import logging

import requests
from slack import WebClient
from slack.errors import SlackApiError

from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)


def post_message_to_slack(message, channel=None):
    """Отсылает уведомление в Slack в канал, указанный в настройках."""
    assert channel is not None

    # if not settings.SLACK_BOT_TOKEN:
    #     return

    slack_token = 'xoxb-346828257569-1526195790369-qBG7p6fgIOTLwjenLQLq1RpJ'  # settings.SLACK_BOT_TOKEN
    client = WebClient(token=slack_token)

    try:
        client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        logger.exception(e)


def slack_notify_about_selectel_cert_expired():
    """Отсылает уведомление в Slack о скором истечении suplbiz wildcard сертификата,
    необходимого для ссылок на статику на наш поддомен (static.supl.biz)."""
    selectel = SelectelStorageAPI()
    cert_info = selectel.get_certificate_info(cert_name='')

    expired_at = cert_info.get('expired_at')
    if not expired_at:
        post_message_to_slack('Не найден ssl сертификат', channel='#test_notification')
        return

    expired_at = datetime.datetime.strptime(expired_at, '%Y-%m-%d').date()

    if (expired_at - timezone.timedelta(days=7)) < timezone.now().date():
        post_message_to_slack(
            message='Истекает срок действия SSL сертификата для облачного хранилища '
                    'на Selectel. Загрузите новый wildcard сертификат',
            channel='#test_notification',
        )


class SelectelStorageAPI:
    """API для доступа к хранилищу."""

    def __init__(self):
        self.token = None

    def auth(self):
        if self.token:
            return

        try:
            r = requests.post(
                url='https://api.selcdn.ru/v2.0/tokens',
                data=json.dumps(
                    {
                        'auth': {
                            'passwordCredentials': {
                                'username': '21069',
                                'password': 's*F~!03t^&',
                            }
                        }
                    }
                ).encode('utf-8'),
                headers={'Content-Type': 'application/json', 'charset': 'utf-8'},
            )
            r.raise_for_status()

            response = r.json()
            self.token = response.get('access', {}).get('token', {}).get('id')
        except requests.exceptions.RequestException as e:
            logger.error('SelectelAPI: auth', extra={'exception': e})

    def get_certificates(self):
        self.auth()
        try:
            r = requests.get(
                url='https://api.selcdn.ru/v1/ssl', headers={'X-Auth-Token': self.token}
            )
            r.raise_for_status()

            response = r.json()
            print(response)
        except requests.exceptions.RequestException as e:
            logger.error('SelectelAPI: get_certificates', extra={'exception': e})

    def get_certificate_info(self, cert_name):
        self.auth()
        try:
            r = requests.get(
                url=f'https://api.selcdn.ru/v1/ssl/{cert_name}',
                headers={'X-Auth-Token': self.token},
            )
            r.raise_for_status()

            response = r.json()
            print(response)

            return {
                'name': '',
                'expired_at': '',
            }
        except requests.exceptions.RequestException as e:
            logger.error('SelectelAPI: get_certificate_info', extra={'exception': e})
