# PostgreSql
create database suplbiz_db_events;
create user suplbiz_events_user with encrypted password 'suplbiz_events_password';
grant all privileges on database suplbiz_db_events to suplbiz_events_user;
docker exec -it suplbiz-root-repo_db_1 psql -U suplbiz_user -d suplbiz_db

# Rabbitmq
docker exec -it suplbiz-root-repo_rabbitmq_1 bash
rabbitmqctl add_vhost clickstream
rabbitmqctl set_permissions -p clickstream suplbiz ".*" ".*" ".*"

python3 project/libs/microservices/consumer.py  --srcmicroservice monolith --maxevents 1

# Redis
docker exec -it redis sh
redis-cli -p 6379 -a 7CO9kINfMjCIfDPEH7MMO7PCb9oLAKFX7D7aZEcfX6ftpF5iZJ info | egrep "used_memory_human|total_system_memory_human"

# CI
secret_deploy_key:
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEII4kKEvu2kx47d2fzaqet3W73yLu5Bz0iA2OoGkKfYsyoAoGCCqGSM49
AwEHoUQDQgAE8CAEnDDBX6pkTFqvQetNbrNrMYQBM8sQZVkTEmJujzccc/3LSM+U
7ap55XoD/3ad5tvp/GDFLOv9riUBulqwYw==
-----END EC PRIVATE KEY-----

# Clickstream


# Создать контейнер руками (надо сначала запушить образ в docker.supl.biz; docker login ... -> docker build ... -> docker push)
# затем спуллить его на сервере (docker login -> docker pull)
# И затем руками создать контейнер
docker login -u suplbiz -p W3wpR0tJPlDcqf7asOXreU8o218JyChXL0e8skWvjt4PWKmGbI7v8Wp9SzMn docker.supl.biz
docker pull docker.supl.biz/front-administration:9eec48bf



docker run -d --restart=always \
    --sysctl net.core.somaxconn=4096 \
    --name front-administration-3100 \
    --publish 3100:3000 \
    --log-driver=syslog \
    --log-opt tag=suplbiz/front-administration-3100 \
    -e BRANCH=sandbox \
    -e SERVICE_NAME=front-administration \
    -e SERVICE_TAGS=front,urlprefix-admin.sandbox.supl.biz/ \
    -e SERVICE_CHECK_TCP=true \
    -e SERVICE_CHECK_INTERVAL=10s \
    -e SERVICE_CHECK_TIMEOUT=3s \
    -e API_HOST=https://sandbox.supl.biz \
    -e SENTRY_DSN=https://2ee154e846b04f46b10a7efd4ebbecd2@sentry.supl.biz/12 \
    -e SENTRY_DSN_PRIVATE=https://2ee154e846b04f46b10a7efd4ebbecd2:dcde543412a24765b6a0bd849c5aeff7@sentry.supl.biz/12 \
    -v /tmp:/tmp \
    --memory="8g" --memory-swap=0 --memory-swappiness=0 \
    docker.supl.biz/front-administration:9eec48bf

# Production
docker run -d --restart=always \
    --sysctl net.core.somaxconn=4096 \
    --name front-administration-3101 \
    --publish 3101:3000 \
    --log-driver=syslog \
    --log-opt tag=suplbiz/front-administration-3101 \
    -e BRANCH=master \
    -e SERVICE_NAME=front-administration \
    -e SERVICE_TAGS=front,urlprefix-admin.supl.biz/ \
    -e SERVICE_CHECK_TCP=true \
    -e SERVICE_CHECK_INTERVAL=10s \
    -e SERVICE_CHECK_TIMEOUT=3s \
    -e API_HOST=https://supl.biz \
    -e SENTRY_DSN=https://a086ec65aa7e487cbec8b984b20d151d@sentry.supl.biz/13 \
    -e SENTRY_DSN_PRIVATE=https://a086ec65aa7e487cbec8b984b20d151d:d5ac8bca9a46475c8560baf7b9d8e7e4@sentry.supl.biz/13 \
    -v /tmp:/tmp \
    --memory="8g" --memory-swap=0 --memory-swappiness=0 \
    docker.supl.biz/front-administration:803e59f9


def test_order_notifications(order_id=None):
    """Проверяет, создаются ли нотификации при публикации заказа. """
    ids = []

    if order_id:
        order = Order.objects.get(id=order_id)
        activity = Activity.objects.get(object_id=order.id, verb='new_order')

        print('real: ', activity.notification_set.count())
        manager = OrderSubscribersManager(order, activity)
        print('expected: ', manager._create_notifications_for_all_subscribers())
    else:
        activities = Activity.objects.filter(
            verb='new_order',
            created_at__date=timezone.now()
        )

        for a in activities:
            if (a.notification_set.count() == 0):
                print(a.created_at)
                print('real: ', a.notification_set.count())
                if a.notification_set.count() == 0:
                    ids.append(a.object_id)
    return ids
            # manager = OrderSubscribersManager(a.object, a)
            # print('expected: ', manager._create_notifications_for_all_subscribers())


def unload_user_comments(user_id):
    """Выгружает все комменты профиля. """
    import csv

    with open(f'/tmp/comments_{user_id}.csv', 'w+') as f:
        w = csv.writer(f)

        data = get_all_comments(user_id)
        for item in data:
            w.writerow([
                item['user']['title'],
                item['text'],
                item['created_at'],
                item['type_description'],
            ])
def get_all_comments(user_id):
    from project.apps.users.helpers.comments import UserCommentAggregator
    from project.apps.users.models import User
    import datetime

    user = User.objects.get(id=user_id)
    helper = UserCommentAggregator(
        user=user,
        for_admin=True,
        min_date=datetime.datetime(1970, 1, 1)
    )

    return helper.get_comments()


def generate_events():
    """Генерит события clickstream. """
    from random import choice
    for i in range(20):
        tpl = choice(('order_published', 'come_back_if_last_order_is_old',))
        track_event({
            'actor_id': i,
            'email': f'aa{i}@mail.ru',
            'url': '/test-1/',
            'qs': 'utm_source=notice&utm_medium=email',
            'tpl': tpl,
            'ab_test': choice(('A', 'B',)),
            'created_at': str(timezone.now()),
        }, 'notifications.emailurlclick')



def get_dates_list(date_from, date_to):
    """Возвращает все дыты от date_from до date_to"""
    if date_from > date_to:
        raise ValueError('date_to should be more or equal that date_from')
    while date_from <= date_to:
        yield date_from
        date_from += timedelta(days=1)

def test_order_notifications(order_id=None):
    """Проверяет, создаются ли нотификации new_order. """

    dt1 = timezone.now() - timezone.timedelta(days=90)
    dt2 = timezone.now()

    dates_list = get_dates_list(dt1, dt2)

    for dt in dates_list:
        activities = Activity.objects.filter(
            verb='new_order',
            created_at__date=dt,
        )

        act_with_notifications = activities.annotate(nn_count=Count('notification')).filter(nn_count__gt=0).count()
        act_without_notifications = activities.annotate(nn_count=Count('notification')).filter(nn_count=0).count()

        print(dt.date(), '; ', f'активити с уведомлениями: {act_with_notifications}, без: {act_without_notifications}')
        print('\n')


def test_send_email_template():
    """Отправляет тестовое письмо, чтобы проверить корректность шаблона на локалке. """
    from project.apps.notifications.utils import get_fake_user_for_notification
    from project.apps.notifications.models import Activity, Notification
    from project.apps.notifications.handlers import do_send_activity
    from django.contrib.contenttypes.models import ContentType

    fake_user = get_fake_user_for_notification()

    instance = get_instance()

    activity = Activity.objects.create(
        verb='callback',
        actor=fake_user,
        object_type=ContentType.objects.get_for_model(instance),
        object_id=instance.id,
    )
    Notification.objects.create(
        activity=activity,
        user_id=fake_user.id,
    )
    do_send_activity(activity.id)


def get_instance():
    return Callback.objects.get(id=3680)


def test_invoice_emails():
    """Тестирует отправку писем со счетами. """

    from project.apps.notifications.utils import notification_invoice_paid
    from project.apps.invoices.factories import PaymentFactory, InvoiceFactory
    from project.apps.tariffs.models import Tariff
    import random

    user = User.objects.get(id=1031096) # Кому
    # Коды влияют на выбор шаблона для отправки письма
    tariff_codes = [
        'kontur-check-company',
        '1contact',
        'my-verification',
        'check-company',
        'provider-1y',
    ]

    tariff_factory = Tariff.objects.filter(
        code=tariff_codes[0])
    ).first()
    invoice_factory = InvoiceFactory(
        tariff=tariff_factory,
        user=user,
        target_user=user,
    )
    payment = PaymentFactory(
        invoice=invoice_factory
    )

    notification_invoice_paid(payment)

##############################
# REMOVE DIPLICATES unique_together
from django.db.models import Count, Max

unique_fields = ['category_id', 'region_id']

duplicates = CategoryCityDescription.objects.values(
    *unique_fields
).order_by().annotate(
    max_id=Max('id'), count_id=Count('id')
).filter(count_id__gt=1)

for duplicate in duplicates:
    print(duplicate)
    CategoryCityDescription.objects.filter(
        **{x: duplicate[x] for x in unique_fields}
    ).exclude(id=duplicate['max_id']).delete()

#############################
