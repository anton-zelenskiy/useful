def unload():
    """
    cat /var/log/syslog | grep -E -oh ", args=\['Return-Path:(.*)\], kwargs"
    :return:

    - софты почти не ставятся
    - если ошибка на одно из писем при отправке, то хард проставится всем!
    """
    import csv
    from django.utils import timezone
    from datetime import date
    from tqdm import tqdm
    from project.apps.users.models import EmailNotificationSettings
    from project.apps.notifications.models import Notification, NotificationMeta, NotificationEmailStatusEnum
    from django.db.models.functions import TruncDate
    from itertools import groupby

    settings = EmailNotificationSettings.objects.filter(
        hard_bounces_last__date__gt=timezone.now().date() - timezone.timedelta(days=30),
    ).annotate(dt=TruncDate('hard_bounces_last')).order_by('dt')

    with open(f'/tmp/stats_hard_bounces.csv', 'w+') as f:
        print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/stats_hard_bounces.csv ~/stats_hard_bounces.csv')
        w = csv.writer(f)
        w.writerow([
            'date',
            'count',
        ])

        grouped = groupby(settings, key=lambda x: x.dt)

        for ts, sett in grouped:
            w.writerow([
                ts,
                len(list(sett))
            ])


def unload():
    """
    cat /var/log/syslog | grep -E -oh ", args=\['Return-Path:(.*)\], kwargs"
    :return:

    - софты почти не ставятся
    - если ошибка на одно из писем при отправке, то хард проставится всем!
    """
    import csv
    from django.utils import timezone
    from datetime import date
    from tqdm import tqdm
    from project.apps.users.models import EmailNotificationSettings
    from project.apps.notifications.models import Notification, NotificationMeta, NotificationEmailStatusEnum

    dt = date(2020, 10, 8)
    settings = EmailNotificationSettings.objects.filter(
        hard_bounces_last__date=dt,
    ).select_related('settings__user')

    hard_emails = list(settings.values_list('settings__user__email', flat=True))
    print('hard bounced count: ', len(hard_emails))

    data = parse_bounce_emails()
    all_emails = list(data.keys())
    soft_emails = [i for i in data if data[i]['soft']]
    print('file: ', len(all_emails), len(set(all_emails) - set(hard_emails)),  len(soft_emails))

    users = User.objects.filter(
        email__in=hard_emails
    ).select_related('settings__email_notification_settings')

    users_soft = User.objects.filter(
        email__in=soft_emails
    ).select_related('settings__email_notification_settings')

    with open(f'/tmp/bounced_users.csv', 'w+') as f:
        print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/bounced_users.csv ~/bounced_users.csv')
        w = csv.writer(f)
        w.writerow([
            'url',
            'email',
            'hard_bounces_date',
            'hard_bounces',
            'soft_bounces',
            'soft (temporary fail, not permanent)',
            'has_sent_notifications',
            'error'
        ])

        w.writerow([f'HARD BOUNCED IN {dt}'])
        for user in tqdm(users):
            s = user.settings.email_notification_settings
            has_sent_notifications = Notification.objects.filter(
                user=user, email_status=NotificationEmailStatusEnum.SENT
            ).first()
            if has_sent_notifications:
                n_meta = NotificationMeta.objects.filter(notification=has_sent_notifications).first()
            else:
                n_meta = None

            item = data.get(user.email)
            if item:
                error = item['error']
                soft = item['soft']
            else:
                # print('not in file')
                error = soft = ''

            w.writerow([
                f'https://admin.supl.biz/profiles/{user.id}/',
                user.email,
                s.hard_bounces_last,
                s.hard_bounces,
                s.soft_bounces,
                '+' if soft else '',
                n_meta.sent_at if n_meta else '',
                error,
            ])

        w.writerow(['SOFT FROM FILE'])
        for user in tqdm(users_soft):
            s = user.settings.email_notification_settings
            has_sent_notifications = Notification.objects.filter(
                user=user, email_status=NotificationEmailStatusEnum.SENT
            ).first()
            if has_sent_notifications:
                n_meta = NotificationMeta.objects.filter(notification=has_sent_notifications).first()
            else:
                n_meta = None

            item = data.get(user.email)
            if item:
                error = item['error']
                soft = item['soft']
            else:
                # print('not in file')
                error = soft = hard = temporaries = permanents = ''

            w.writerow([
                f'https://admin.supl.biz/profiles/{user.id}/',
                user.email,
                s.hard_bounces_last,
                s.hard_bounces,
                s.soft_bounces,
                '+' if soft else '',
                n_meta.sent_at if n_meta else '',
                error,
            ])


def parse_bounce_emails():
    """Парсит из bounced emails текст причины отклонения. """
    from flufl.bounce import all_failures
    from funcy import first

    result = {}

    with open('/tmp/bounces.txt', mode='r') as file:
        for line in file:
            line = line[8:]
            raw_email = line[:-10]
            raw_email = raw_email.encode('utf-8').decode('unicode_escape')

            mail = extract_email_message(raw_email)

            err = mail.get_payload()[0]
            try:
                err_text = err.get_payload()
            except AttributeError:
                continue

            temporaries, permanents = all_failures(mail)

            temporaries = [
                User.objects.normalize_email(t.decode('utf-8')) for t in temporaries
            ]
            permanents = [
                User.objects.normalize_email(p.decode('utf-8')) for p in permanents
            ]
            email = first(temporaries) or first(permanents)

            result.update({email: {
                'soft': bool(temporaries),
                'hard': bool(permanents),
                'error': err_text,
                'temporaries': temporaries,
                'permanents': permanents,
            }})

    return result


def extract_email_message(raw_email):
    import email

    mail = email.message_from_string(raw_email)

    first_payload = mail.get_payload()
    if isinstance(first_payload, str):
        mail = email.message_from_string(first_payload)

    return mail
