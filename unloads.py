
# Просмотры за неделю, сгруппированные по пользователям
def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    from tqdm import tqdm
    from itertools import groupby
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    u = User.objects.get(id=1299571)
    pp = u.proposals.all()

    from itertools import groupby
    views = InstanceView.objects.filter(
        created_at__range=(datetime.date(2019, 6, 17), datetime.date(2019, 6, 24)),
        object_type=ContentType.objects.get_for_model(Proposal)
    ).order_by('object_owner_id')

    grouped = groupby(views, key=lambda x: x.object_owner_id)

    for uid, views_ in grouped:
        data = [
            f'https://admin.supl.biz/profiles/{uid}/',
            len(list(views_))
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)




    import datetime
    from tqdm import tqdm
    from project.apps.orders.models import OrderSourceEnum

    rubric = Rubric.objects.get(id=1809)
    rub_desc = rubric.get_descendants(include_self=True)
    months = (4, 5, 6, 7, 8, 9)

    data = []
    for month in tqdm(months):
        month_name = datetime.date(2019, month, 1).strftime('%B')

        orders = Order.objects.filter(
            created_at__year=2019,
            created_at__month=month,
            rubrics__in=rub_desc
        )
        data.append([
            month_name,
            orders.filter(source=OrderSourceEnum.COLD_CUSTOMER).count(),
            orders.filter(source=OrderSourceEnum.REPEATABLE_CUSTOMER).count(),
            orders.filter(source=OrderSourceEnum.WARM_CUSTOMER).count(),
        ])

    return data




from django.db import connections
class RawSQLManager:
    """
    Класс для выполнения сырых SQl-запросов через cursor и
    возврата результата в правильной форме
    """

    def __init__(self, sql_query, params=None, db='default'):
        self.sql_query = sql_query
        self.params = params
        self.db = db

    def execute(self, flat=False, without_result=False):
        """ Executes SQL query """

        with connections[self.db].cursor() as cursor:
            cursor.execute(self.sql_query, self.params)

            if not without_result:
                if flat:
                    rows = cursor.fetchall()
                    return [row[0] for row in rows]
                return self.dictfetchall(cursor)

            return []

    def explain(self):
        """ Executes SQL query """
        with connections[self.db].cursor() as cursor:
            cursor.execute(
                f'EXPLAIN (FORMAT JSON, ANALYZE) {self.sql_query}',
                params=self.params
            )

            return cursor.fetchall()

    def dictfetchall(self, cursor):
        """ Returns all rows from a cursor as a dict """
        description = cursor.description
        return [
            dict(zip([col[0] for col in description], row))
            for row in cursor.fetchall()
        ]




def grouper(n, iterable):
    import itertools
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def unload():
    import csv
    from tqdm import tqdm
    from project.back_office.suppliers.models import SupplierSettingsGroup
    from project.apps.users.helpers.similar_profile_searcher import SameProfileSearcher

    sg = SupplierSettingsGroup.objects.get(id=10)

    f_rubs = sg.rubrics.all().get_descendants(include_self=True)
    ex_rubs = sg.rubrics_for_exclude_in_orders.all().get_descendants(include_self=True)

    with_offers = User.objects.filter(offers__isnull=False).distinct().values_list('id', flat=True)

    filtered_user_ids = User.objects.filter(rubrics__in=f_rubs, id__in=with_offers)

    users = User.objects.filter(
        id__in=filtered_user_ids.values('id'),
    ).exclude(
        rubrics__in=ex_rubs
    ).distinct()

    all_same_user_ids = set()

    for i, users_chunk in enumerate(grouper(30000, users)):
        with open(f'/tmp/users_suppliers__{i}.csv', 'w+') as f:
            print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/users_suppliers__{i}.csv ~/users_suppliers__{i}.csv')
            w = csv.writer(f)
            w.writerow([
                'профиль',
                'продавец',
                'тариф',
                'похожие',
            ])

            for user in tqdm(users_chunk):
                if user.id in all_same_user_ids:
                    continue
                lead = SalesLead.objects.filter(user=user).first()
                sub = user.get_active_subscription()
                w.writerow([
                    f'https://admin.supl.biz/profiles/{user.id}/',
                    lead.caller.name if lead and lead.caller else '',
                    sub.tariff.title if sub else '',
                ])
                same_ids = SameProfileSearcher(user).get()
                all_same_user_ids.update(same_ids)

                for uid in same_ids:
                    w.writerow([
                        '', '', '',
                        f'https://admin.supl.biz/profiles/{uid}/',
                    ])



def set_active_payers():
    from supl_shared.redis.redis import get_redis, RedisDBEnum
    users = list(User.objects.active_payers().values_list('id', flat=True))

    redis = get_redis(db=RedisDBEnum.DEFAULT)

    redis.delete('unload:active_payers')
    redis.rpush('unload:active_payers', *users)
    redis.expire('unload:active_payers', 60 * 60 * 4)


def get_active_payers():
    from supl_shared.redis.redis import get_redis, RedisDBEnum

    redis = get_redis(db=RedisDBEnum.DEFAULT, decode_responses=True)

    return [int(i) for i in redis.lrange('unload:active_payers', 0, -1)]


def update_proposal_categories():
    import re
    import csv
    from tqdm import tqdm
    from project.apps.proposals.tasks import async_index_proposal
    from project.apps.proposals.elasticsearch.common import index_proposal

    f = open('/tmp/proposals-paid.csv', 'r', encoding='utf-8')
    file = csv.reader(f)
    next(file)  # header

    for row in tqdm(file):
        link, category_id = row

        if not category_id:
            continue

        proposal_id = int(re.findall(r'\d+', link)[0])
        proposal = Proposal.objects.filter(id=proposal_id).first()
        category = Category.objects.filter(id=category_id).first()

        proposal.categories.add(category)
        async_index_proposal.delay(proposal_id)


def unload():
    import csv
    import datetime
    from tqdm import tqdm
    from supl_shared.phones.formatting import format_phones
    from funcy import first
    from collections import defaultdict
    from django.db.models.functions import TruncMonth
    from itertools import groupby
    from collections import OrderedDict

    with open(f'/tmp/session_stats.csv', 'w+') as f:
        print('scp ubuntu@prod2.srv.supl.biz:/tmp/session_stats.csv ~/session_stats.csv')
        w = csv.writer(f)

        w.writerow([
            'счетчик',
            'сессий с недозвоном сейчас',
            'звонков по недозвонам с 22.07.2020 по 22.10.2020',
        ])

        call_counter = [1, 2, 3, 4, 5]

        now_stats = {}
        ms = list(ModerationSession.objects.filter(empty_call_counter__gt=0).values('empty_call_counter'))
        for c in call_counter:
            now_stats[c] = len(list(filter(lambda x: x['empty_call_counter'] == c, ms)))

        logs = ModerationSessionLog.objects.filter(
            created_at__range=(datetime.date(2020, 7, 22), datetime.date(2020, 10, 22)),
            log_type=ModerSessionLogTypeEnum.PLANNED_CALL_CREATED,
            comment__contains='недозвонов',
        )

        counter = defaultdict(int)
        logs = list(logs.values('customer', 'id', 'created_at'))
        logs = sorted(logs, key=lambda x: x['customer'])

        grouped = groupby(logs, key=lambda x: x['customer'])

        for customer, ll in grouped:
            l = len(list(ll))
            counter[l] += 1

        counter = OrderedDict(counter)

        for c in counter:
            w.writerow([
                c,
                now_stats.get(c, ''),
                counter.get(c, '')
            ])


def unload():
    import csv
    from tqdm import tqdm
    from datetime import datetime

    with open(f'/tmp/sl.csv', 'w+') as f:
        print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/sl.csv ~/sl.csv')
        w = csv.writer(f)

        w.writerow([
            'профиль',
            'дата',
            'оплачен',
            'сумма',
        ])

        for id_, ts in tqdm(l):
            w.writerow([
                id_,
                datetime.fromtimestamp(ts),
            ])



def unload():
    import csv
    from tqdm import tqdm
    from datetime import datetime
    from django.db.models import Q
    from functools import reduce
    from operator import __or__ as OR

    with open(f'/tmp/customers_mailru.csv', 'w+') as f:
        print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/customers_mailru.csv ~/customers_mailru.csv')
        w = csv.writer(f)

        domains = ['@mail.ru', '@list.ru', '@bk.ru', '@inbox.ru']
        filters_ = [Q(email__endswith=e) for e in domains]

        # suppliers = User.objects.filter(reduce(OR, filters_)).filter(offers__isnull=False).distinct()
        customers = User.objects.filter(reduce(OR, filters_)).filter(orders__isnull=False).distinct()

        for u in tqdm(customers):
            w.writerow([
                u.email
            ])


def delete_images(iterations=1):
    """Удаляет файлы изображений из контейнера static."""
    from tqdm import tqdm
    selectel_api = SelectelStorageAPI()

    container = 'static'

    for i in tqdm(range(iterations)):
        response = selectel_api.list_files(
            container=container, prefix='media/cache/', limit=100
        )
        file_names = [f"{container}/{img['name']}" for img in response]
        files_str = '\n'.join(file_names)

        response = selectel_api.bulk_delete(files_str)
        print(response)



def unload():
    import csv
    from tqdm import tqdm
    from datetime import date
    from itertools import product
    from django.db.models.functions import TruncMonth, TruncYear

    with open(f'/tmp/payments.csv', 'w+') as f:
        print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/payments.csv ~/payments.csv')
        w = csv.writer(f)

        payers_ids = list(Subscription.objects.values_list('user', flat=True))
        users = User.objects.filter(id__in=set(payers_ids))

        header = ['ссылка на профиль', 'кол-во оплат (с 2015 по сегодня)']
        years = range(2015, 2021 + 1)
        months = range(1, 12 + 1)
        years_months = list(product(years, months))
        months_map = {
            1: 'январь',
            2: 'февраль',
            3: 'март',
            4: 'апрель',
            5: 'май',
            6: 'июнь',
            7: 'июль',
            8: 'август',
            9: 'сентябрь',
            10: 'октябрь',
            11: 'ноябрь',
            12: 'декабрь',
        }
        header.extend([f'{months_map.get(m)} {y}' for y, m in years_months])

        dt_from = date(2015, 1, 1)
        dt_to = date(2021, 1, 1)

        w.writerow(header)
        for user in tqdm(users):
            payments_count = Payment.objects.filter(
                user=user,
                created_at__date__range=(dt_from, dt_to)
            ).count()
            row = [
                f'https://admin.supl.biz/profiles/{user.id}/',
                payments_count,
            ]

            contacts = list(UserClickOrderContactsLog.objects.filter(
                user=user,
            ).values('created_at', 'id', 'user'))

            for y, m in years_months:
                contacts_count = len(list(filter(
                    lambda x: x['created_at'].year == y and x['created_at'].month == m,
                    contacts
                )))
                row.append(contacts_count)

            w.writerow(row)


def unload():
    """
    Берем профили, которые оплачивали тариф (без одноразовых открытий) за 2020 год:
        - ссылка на профиль
        - дата оплаты
        - сумма оплаты
        - рубрики юзера (только не все в одну строчку, а разделить - одна ячейка=одна рубрика)
        - рубрики заказов, в которых юзер оставил предложения за 60 дней до момента
        покупки тарифа (так же по отдельности)
        - тексты предложений на эти заказы (опционально)
    """
    import csv
    from tqdm import tqdm
    from datetime import date
    from django.utils import timezone
    from project.apps.invoices.models import Payment

    from project.apps.orders.models import Order, OrderRubric

    file1 = open(f'/tmp/payments_users.csv', 'w+')
    file2 = open(f'/tmp/payments_orders.csv', 'w+')
    print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/payments_users.csv ~/payments_users.csv')
    print(f'scp ubuntu@prod2.srv.supl.biz:/tmp/payments_orders.csv ~/payments_orders.csv')
    w1 = csv.writer(file1)
    w2 = csv.writer(file2)

    dt_from = date(2020, 1, 1)
    dt_to = date(2021, 1, 1)

    payments = (
        Payment.objects.filter(
            payment_date__range=(dt_from, dt_to),
            amount__gt=1500,
            user__staff_status__isnull=True,
        )
            .select_related('user')
            .order_by('created_at')
    )

    header = [
        'ссылка на профиль',
        'дата оплаты',
        'сумма оплаты',
    ]

    all_rubric_ids = []
    for payment in tqdm(payments):
        user_rubrics = payment.user.rubrics.all()
        for rub in user_rubrics:
            all_rubric_ids.append(rub.id)

        orders = Order.objects.filter(
            offers__user=payment.user,
            offers__created_at__date__range=(
                payment.payment_date - timezone.timedelta(days=60),
                payment.payment_date,
            ),
        ).select_related('offer').distinct()
        order_rubrics = OrderRubric.objects.filter(
            order__in=orders
        ).select_related('rubric')
        for rub in order_rubrics:
            all_rubric_ids.append(rub.rubric.id)

    rubrics = list(Rubric.objects.filter(id__in=all_rubric_ids).order_by('title'))
    header.extend([r.title for r in rubrics])
    w1.writerow(header)
    w2.writerow(header)

    print('UNLOAD')
    for payment in tqdm(payments):
        user = payment.user
        user_rubrics = user.rubrics.all()
        orders = Order.objects.filter(
            offers__user=user,
            offers__created_at__date__range=(
                payment.payment_date - timezone.timedelta(days=60),
                payment.payment_date,
            ),
        ).select_related('offer').distinct()
        order_rubrics = OrderRubric.objects.filter(
            order__in=orders
        ).select_related('rubric')
        # offer_texts = [o.offer.description for o in orders]

        row = [
                  f'https://admin.supl.biz/profiles/{user.id}/',
                  payment.payment_date,
                  payment.amount,
              ] + [''] * len(rubrics)

        for r in user_rubrics:
            index = rubrics.index(r)
            row[index + 3] = r.title

        row2 = [
                   f'https://admin.supl.biz/profiles/{user.id}/',
                   payment.payment_date,
                   payment.amount,
               ] + [''] * len(rubrics)

        for or_ in order_rubrics:
            r = or_.rubric
            index = rubrics.index(r)
            row2[index + 3] = r.title

        w1.writerow(row)
        w2.writerow(row2)

    file1.close()
    file2.close()


def unload(prefix='/tmp'):
    import csv
    from tqdm import tqdm
    # from project.rpc_services.orders.utils import get_orders_by_search_phrase

    write_file = open(f'{prefix}/proposals_out.csv', 'w+')
    writer = csv.writer(write_file, delimiter=';')

    with open(f'{prefix}/proposals.csv', 'r') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        writer.writerow(header)

        proposal_titles = {}

        for row in tqdm(reader):
            proposal_title = row[1]

            # Есть одинаковые названия товаров
            if not (orders := proposal_titles.get(proposal_title)):
                orders = get_orders_by_search_phrase(proposal_title)
                proposal_titles[proposal_title] = orders

            writer.writerow(row + [f'https://supl.biz/orders/{o["id"]}/' for o in orders])

    write_file.close()


def unload(prefix='/tmp'):
    import csv
    from tqdm import tqdm
    from project.rpc_services.orders.utils import get_orders_by_search_phrase

    write_file = open(f'{prefix}/proposals_out.csv', 'w+')
    writer = csv.writer(write_file, delimiter=';')

    with open(f'{prefix}/proposals.csv', 'r') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        writer.writerow(header)

        proposal_titles = set()

        for row in tqdm(reader):
            proposal_title = row[1]

            if proposal_title in proposal_titles:
                continue
            orders = get_orders_by_search_phrase(proposal_title)
            proposal_titles.add(proposal_title)

            writer.writerow(row + [f'https://supl.biz/orders/{o["id"]}/' for o in orders])

    write_file.close()


from project.apps.main.elasticsearch.management import EsSearch
from project.apps.orders.elastic.elastic_search import EsOrderContainer


def get_orders_by_search_phrase(search_phrase, size=5):
    """Возвращает последние добавленные заказы, подходящие под поисковую фразу.
    """
    from django.utils import timezone

    search_query = {
        'from': 0,
        'size': size,
        'sort': [{'published_at': {'order': 'desc'}}],
        'query': {
            'bool': {
                'must': {
                    'multi_match': {
                        'query': search_phrase.lower(),
                        'type': 'phrase_prefix',
                        'fields': ['description'],
                    }
                },
                "filter": [
                    {"range": {
                        "actualized_at": {
                            "gte": timezone.now() - timezone.timedelta(days=100),
                        }
                    }}
                ]
            }
        }
    }

    es_container = EsOrderContainer()
    result = EsSearch(es_container=es_container).search(query=search_query)

    # Отбросим лишнюю информацию
    result = [item['_source']['_meta'] for item in result['hits']['hits']]

    result = [{
        'id': item.get('id'),
        'url': item.get('url'),
        'description': item.get('description'),
        'actualized_at': item.get('actualized_at'),
        'title': item.get('title'),
    } for item in result]

    return result
