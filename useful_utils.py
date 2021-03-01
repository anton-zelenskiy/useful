# PostgreSql
create database suplbiz_db_events;
create user suplbiz_events_user with encrypted password 'suplbiz_events_password';
grant all privileges on database suplbiz_db_events to suplbiz_events_user;
docker exec -it suplbiz-root-repo_db_1 psql -U suplbiz_user -d suplbiz_db

# Rabbitmq
docker exec -it suplbiz-root-repo_rabbitmq_1 bash
rabbitmqctl add_vhost microservices
rabbitmqctl set_permissions -p microservices suplbiz ".*" ".*" ".*"

# Purge queue
rabbitmqadmin -u suplbiz -p fAuE94t9lZ3vJ5T7CV7JPsJaTRgb1MOMzIRAL7136uA2gRA2sZ list queues name | awk '{ print $2 }' | xargs -L1  rabbitmqctl --vhost=suplbiz purge_queue

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


# DB dump / restore
dump: docker exec -t db pg_dump -c -U suplbiz_user -d suplbiz_db > /home/anton/dddd.sql
restore: cat /home/anton/dddd.sql | docker exec -i db psql -U suplbiz_user -d suplbiz_db


# Pylint
pylint -E --load-plugins pylint_plugin --disable=all --enable=non-gettext-string --ignore=migrations,tests project/apps/orders


# Wildcard cert
docker run -it --rm --name certbot \
-v /etc/letsencrypt:/etc/letsencrypt:rw \
--log-driver=syslog \
--log-opt tag=docker/letsencrypt \
--network=host \
certbot/certbot certonly \
--manual \
--agree-tos \
--preferred-challenges=dns \
-w /etc/letsencrypt \
--email tech@supl.biz \
-d *.supl.biz

# Curl post file
curl -i -X POST -F "image=@/home/anton/ep.jpg" localhost:8000/proposals/images/

# Git remove file from history
git filter-branch --force --index-filter \
    "git rm --cached --ignore-unmatch project/config.py" \
    --prune-empty --tag-name-filter cat -- --all


def test():
    """Возвращает популярные домены. """
    from project.core.db.raw_sql import RawSQLManager
    sql = """
    select substring(email from '@(.*)$') as domain, count(*) as cnt
    from users_user
    group by domain HAVING count(*) >= 15
    -- order by cnt DESC
    """
    res = RawSQLManager(sql).execute()
    # sorted_res = sorted(res, key=lambda x: x['count'])
    return res


# Subquery
def get_queryset(self):
    region_id = self.request.query_params.get('region_id')
    proposals_count = CategoryCapacity.objects.filter(
        category_id=OuterRef('id'),
        region_id=region_id,
    ).values_list('proposals_count')[:1]
    qs = Category.objects.filter(level=0).annotate(
        count_of_proposals_inside=Coalesce(
            Subquery(proposals_count),
            Value(0),
            output_field=IntegerField(),
        )
    )
    return qs


def enrich_url_params(url, params: dict):
    """Добавляет к ссылке query_params. """
    from urllib.parse import urlparse, urlencode, parse_qsl
    if not url:
        return None
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    query.update(params)
    query = urlencode(query)
    return f'{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}'


def test():
    """Raw query"""
    from django.db.models import prefetch_related_objects
    s = """
    select
        *,
        parent.id, parent.name,
        COALESCE(categories_capacity.proposals_count, 0) as proposals_count
    from categories
    left join categories_capacity
    on (
        categories.id = categories_capacity.category_id and 
        categories_capacity.region_id = 1
    )
    left join categories as parent on (categories.parent_id = parent.id)
    where categories.level = 1
    """
    queryset = Category.objects.filter(level=1).raw(s)
    return queryset

# Починка react hot loading
echo fs.inotify.max_user_watches=524288 | sudo tee -a /etc/sysctl.conf && sudo sysctl -p

# Стата по долгим запросам
pgbadger --prefix ' %t [%p]: [%l-1] ' /var/log/docker/postgres.log


# ssh config file
Host prod
    HostName prod.srv.supl.biz
    User root
    IdentityFile ~/.ssh/suplbiz
Host prod2
    HostName prod2.srv.supl.biz
    User ubuntu
    IdentityFile ~/.ssh/suplbiz

# ssh tunnel to db
ssh -oStrictHostKeyChecking=no -nNT -L 5433:db.srv.supl.biz:5433 -i ~/.ssh/id_ed25519 ubuntu@db.srv.supl.biz


# xargs bash
docker ps | grep 'proposals-shadow-tasks-elastic_heavy-primary-' | awk '{print $1}' | xargs docker stop


# bash aliases
alias run_vpn='sudo openvpn --config client.ovpn --daemon'
alias stop_vpn='sudo killall openvpn'
alias source_monolith='source ~/virtualenvs/monolith/bin/activate; cd ~/suplbiz-root-repo/django-backend; export DJANGO_SETTINGS_MODULE=project.settings.dev; export PYTHONPATH=$PWD'
alias source_proposals='source ~/virtualenvs/proposals/bin/activate; cd ~/suplbiz-root-repo/proposals; export DJANGO_SETTINGS_MODULE=project.settings; export PYTHONPATH=$PWD'
check_email_ () { ssh root@supl.biz "cat /var/log/mail.log | grep '$1'"; }
alias shell_plus="python manage.py shell_plus --use-pythonrc"

# ORM Coalesce
from django.db.models import Value, IntegerField, Case, When
user = User.objects.get()
logs = User.objects.filter(
    id__in=[],
).annotate(
    has_parent=Case(
        When(
            parent_id=user.id,
            then=Value(2)
        ),
        When(
            children__in=[user.id],
            then=Value(1)
        ),
        default=Value(0),
        output_field=IntegerField(max_length=255)
    ),
    has_children=Case(
        When(
            parent_id=user.id,
            then=Value(2)
        ),
        When(
            children__in=[user.id],
            then=Value(1)
        ),
        default=Value(0),
        output_field=IntegerField(max_length=255)
    ),
)

def get_user_ids_from_es():
    """Выгружает id всех профилей из эластика. """
    from project.core.elasticsearch.connect import EsConnector
    client = EsConnector.get_connection()
    es_container = EsUserContainer()
    query = {
        'query': {
            "match_all": {}
        },
        '_source': ['id'],
        "size": 1000,
        "sort": [
            {
                "id": {
                    "order": "asc"
                }
            }
        ]
    }
    user_ids = []
    # Init scroll
    page = client.search(
        index=es_container.index,
        scroll='1m',
        size=10000,
        body=query
    )
    user_ids.extend([int(i['_id']) for i in page['hits']['hits']])
    scroll_id = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    # Start scrolling
    while (scroll_size > 0):
        print("Scrolling...")
        page = client.scroll(scroll_id=scroll_id, scroll='1m')
        user_ids.extend([int(i['_id']) for i in page['hits']['hits']])
        scroll_id = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
    return user_ids

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
        print(f'Команда1 (если prod2): scp ubuntu@prod2.srv.supl.biz:/tmp/comments_{user_id}.csv ~/comments_{user_id}.csv')
        print(f'Команда2 (если prod): scp root@prod.srv.supl.biz:/tmp/comments_{user_id}.csv ~/comments_{user_id}.csv')

def get_all_comments(user_id):
    from project.apps.users.helpers.comments import UserCommentAggregator
    import datetime

    helper = UserCommentAggregator(
        user_id=user_id,
        for_admin=True,
        date_from=datetime.datetime(1970, 1, 1),
        date_to=datetime.datetime(2020, 12, 12),
    )

    return helper.get_comments()


def get_dates_list(date_from, date_to):
    """Возвращает все даты от date_from до date_to"""
    if date_from > date_to:
        raise ValueError('date_to should be more or equal that date_from')
    while date_from <= date_to:
        yield date_from
        date_from += timedelta(days=1)


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

# Docker-compose restore dump
docker-compose run --rm db bash -c "psql -U suplbiz_user -h db -d suplbiz_db < /home/supldev/develop/py-backends/suplbiz_db.dump"

# Ansible
ansible-playbook -i inventories/production main.yml --limit db2 --tags "postgres"



# вытащить такие SourceLogs, в которых они являются последними логами с  источником вважный у пользователя
with a as (
        select id, user_id, source, created_at, rank() OVER (partition by user_id order by created_at desc) as rank_number
from source_log where created_at::date = '2021-02-01'
)
select id, user_id, source, created_at, rank_number from a where rank_number = 1
and source
IN ('anothersiteorder', 'enticed', 'cold_customer', 'repeatable_customer',
    'YandexDirect', 'yandex', 'GoogleAdWords', 'google', 'VKads', 'FacebookAds',
    'TargetMail', 'mytarget')