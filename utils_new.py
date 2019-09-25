def unsubscribe_users(ids):
    """ Отписывает пользователей от рассылки """
    users = User.objects.filter(id__in=ids)
    for u in users:
        notifications = u.settings.notifications or {}

        distribution = {'distribution': {
            'email': 'none',
            'push': 'none'
        }}
        notifications.update(distribution)

        u.settings.notifications = notifications
        u.settings.save()


def time_exec(func):
    """ Декоратор, позволяющий определить время выплнения функции """
    import time

    def inner(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print('Time execution: {} seconds.'.format(end_time - start_time))

        return result

    return inner


def copy_proposals_from_to():
    """Скопировать предложения от П1 на П2"""
    old_user = User.objects.get(id=1303827)
    new_user = User.objects.get(id=1303839)
    pp = old_user.proposals.all()

    for p in pp:
        old_p = Proposal.objects.get(id=p.id)
        p.pk = None
        p.user = new_user
        p.need_index = True
        p.save()

        for r in old_p.regions.all():
            p.regions.add(r)

        for ru in old_p.rubrics.all():
            p.rubrics.add(ru)

        p.save()


def generate_users_with_reg_rub():
    """генерация пользователей"""
    from project.apps.users.factories import UserFactoryWithCustomAreas
    origin = Region.objects.get(title_ru='Томск')
    regions = Region.objects.filter(title_ru__in=['Томск'])
    rubrics = Rubric.objects.filter(title_ru__in=['Зеленые куличики'])
    UserFactoryWithCustomAreas(regions=regions, rubrics=rubrics, origin=origin)


def generate_warm():
    """Генерация данных для теплых"""
    from project.apps.warm_customers.factories.leads import WarmLeadUserFactory
    from project.apps.warm_customers.factories.orders import UserWarmOrderCandidateFactory
    from project.apps.warm_customers.factories.groups import WarmCallerGroupSingletonFactory, WarmCallerGroupMemberFactory
    from project.apps.orders.factories import SimpleOrderFactory

    g = WarmCallerGroupSingletonFactory()
    wcm = WarmCallerGroupMemberFactory(user_id=4, group=g)

    lead = WarmLeadUserFactory(creator_id=4, caller_id=4)
    orders = UserWarmOrderCandidateFactory(lead=lead)
    SimpleOrderFactory(user=lead.user)


def send_custom_order_to_custom_user():
    """Отправить письмо пользователю руками"""
    from project.apps.notifications.channels.email.concrete_senders.new_order import NewOrderEmailSender
    order_id = 587187
    order_ct = ContentType.objects.get_by_natural_key('orders', 'order')
    admin = User.objects.get(id=1031096)

    activity, _ = Activity.objects.get_or_create(
        verb='new_order',
        actor_id=admin.id,
        object_type=order_ct,
        object_id=order_id
    )

    notification, _ = Notification.objects.get_or_create(
        activity=activity,
        email_status=EmailStatusEnum.NOT_SENT,
        user_id=1031096
    )

    NewOrderEmailSender(activity).send(notification)


def update_utc_offset(file_name):
    """Обновляет смещение относительно UTC для регионов из файла"""
    f = open(file_name, 'r', encoding='utf-8')
    file = csv.reader(f)
    for row in file:
        region = Region.objects.filter(title_ru=row[0]).first()
        if region:
            region.utc_offset = row[1]
            region.save()
        else:
            print(row[0])


def update_sequence_ids():
    """Обновить sequence for PK (id с максимального текущего id таблицы)"""
    from project.utils.raw_sql import RawSQLManager

    all_tables_sql = """

    SELECT c.column_name, c.table_name
    FROM information_schema.key_column_usage AS c
    LEFT JOIN information_schema.table_constraints AS t
    ON t.constraint_name = c.constraint_name
    WHERE t.constraint_type = 'PRIMARY KEY' AND c.table_name NOT IN (
        'proposals_proposalcategoryretargetinglink', 'django_session',
        'callerspanel_ro_group', 'callerspanel_lettertemplate',
        'callerspanel_letter', 'callerspanel_leadactionlog',
        'callerspanel_lead','authtoken_token', 'callerspanel_caller',
        'callerspanel_cc_group', 'callerspanel_cc_group_rubrics'
    );
    """
    response = RawSQLManager(sql_query=all_tables_sql).execute()

    for table_dict in response:
        table = table_dict['table_name']
        field = table_dict['column_name']
        max_value_sql = "select Max({field}) as max from {table}".format(table=table, field=field)
        max_value = response = RawSQLManager(sql_query=max_value_sql).execute()[0]['max']
        if max_value is None:
            value = 1
        else:
            value = max_value + 1
        print(table_dict, value)
        query_pattern = "ALTER SEQUENCE {table}_{field}_seq RESTART with {max_value};".format(
            table=table,
            max_value=value,
            field=field
        )
        RawSQLManager(sql_query=query_pattern).execute(without_result=True)
        print(table)

from project.utils.raw_sql import RawSQLManager
restart_with = "select Max(id) as max from users_user"
pk = RawSQLManager(sql_query=restart_with).execute()[0]['max']
RawSQLManager(sql_query='ALTER SEQUENCE users_user_id_seq RESTART with 147;').execute(without_result=True)



Чтобы каждый раз не запускать вручную pycharm и локальный стэнд:
#!/bin/bash

tab="--tab"
foo=""

foo+=($tab -e "bash -c '
    cd ~/suplbiz &&
    docker stop \$(docker ps -a -q) &&
    docker-compose up
';bash")

foo+=($tab -e "bash -c 'cd ~/suplbiz';bash")
foo+=($tab -e "bash -c 'cd ~/suplbiz/django-backend';bash")
foo+=($tab -e "bash -c 'cd ~/suplbiz/front-facade-react';bash")
foo+=($tab -e "bash -c 'cd ~/suplbiz/django-backend && htop';bash")
foo+=($tab -e "bash -c 'cd ~/suplbiz/django-backend';bash")

gnome-terminal --maximize "${foo[@]}"

/home/michael/apps/pycharm/bin/pycharm.sh ~/suplbiz &

exit 0


def get_ro_leads_count():
	# Количество лидов повторных для группы
    settingss = ROGroup.objects.get(id=2)
    caller = User.objects.get(id=1031096)

    # get_lead_who_has_planned_calls
    now_plus_three_minutes = timezone.now() + timedelta(minutes=3)

    # Получаем перезвоны, непривязанные к звонку
    lead1 = Lead.objects.filter(
        planned_call_dt__lte=now_plus_three_minutes,
        status=LeadStatusEnum.HAS_PLANNED_CALL,
        crm_type=CRMTypeEnum.REPEATABLE_ORDERS
    ).count()

    # get_lead_who_wait_after_specification_reply
    lead2 = Lead.objects.filter(
        status=LeadStatusEnum.WAIT_AFTER_SPECIFICATION_REPLY,
        crm_type=CRMTypeEnum.REPEATABLE_ORDERS,
    ).count()

    # get_lead_who_was_returned_from_moderation
    lead3 = Lead.objects.filter(
        status=LeadStatusEnum.RETURNED_FROM_MODERATION_BY_HEAD_CALLER,
        crm_type=CRMTypeEnum.REPEATABLE_ORDERS
    ).count()

    # get_lead_who_has_empty_calls
    from project.settings.base import TIME_ZONE
    local_tz = timezone.pytz.timezone(TIME_ZONE)
    one_day_ago = (
        timezone.localtime(timezone=local_tz) - timezone.timedelta(hours=24)
    )

    lead4 = Lead.objects.filter(
        status=LeadStatusEnum.HAS_EMPTY_CALL,
        updated_at__lte=one_day_ago,
        updated_at__hour=one_day_ago.hour,
        crm_type=CRMTypeEnum.REPEATABLE_ORDERS
    ).count()

    # get_lead_who_has_refusal
    updated_at_ago = timezone.now() - timedelta(days=settingss.period * 30)
    published_days_ago = timezone.now() - timedelta(
        days=settingss.week_lte * 7)

    lead5 = Lead.objects.filter(
        updated_at__lte=updated_at_ago,
        status__in=(LeadStatusEnum.DENIED_BY_CALLER,
                    LeadStatusEnum.DENIED_BY_EMPTY_CALLS_LIMIT),
        crm_type=CRMTypeEnum.REPEATABLE_ORDERS,
        user_id__in=Subquery(
            Order.objects.filter(
                published_at__lte=published_days_ago,
                published_at__gt=OuterRef('updated_at'),
                user_id=OuterRef('user_id'),
            ).values_list('user_id', flat=True)
        )
    ).count()

    # get_new_lead
    selector = LeadCandidateSelector(
        settings=settingss,
        caller=caller
    )
    lead6 = len(selector.retrieve())

    print(lead1+lead2+lead3+lead4+lead5+lead6)


# Причины возражений звонящих
def reasons(file):
    import dropbox, os, csv
    from collections import OrderedDict
    client = dropbox.Dropbox(
        'qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    header = ['Манагер'] + [i for i in range(1, 22 + 1)]
    w.writerow(header)

    leads = SalesLead.objects.filter(objection_reasons__len__gt=0)

    callers = User.objects.filter(id__in=leads.values_list('caller', flat=True))

    data = []
    for caller in callers:
        reasons = OrderedDict()
        choices_keys = [r[0] for r in ObjectionReasonsEnum.CHOICES]
        for ch in choices_keys:
            reasons[ch] = 0

        caller_leads = leads.filter(caller=caller)
        for lead in caller_leads:
            objection_reasons = lead.objection_reasons
            for r in objection_reasons:
                reasons[r] += 1

        data.append({
            'caller': caller.name,
            'data': reasons
        })

    writable_data = []
    for row in data:
        writable_data.append(
            [row['caller']] + [row['data'][key] for key in row['data'].keys()]
        )

    w.writerows(writable_data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)




    def get_recommended_orders(self, queryset, verb):
        """Возвращает количество заказов по рекомендованным рубрикам. """

        # order_rubric_subquery = OrderRubric.objects.filter(
        #     order_id=OuterRef('target_id')
        # ).values('rubric')
        #
        # return queryset.filter(
        #     verb=verb,
        #     recommended_rubrics__overlap=Array(Subquery(order_rubric_subquery))
        # ).count()

        log_order_rubrics_desc = """
        -- Рубрики заказа
        with order_rubrics as (
            select rubrics.id, rubrics.tree_id, rubrics.lft, rubrics.rght
            from rubrics_rubric as rubrics
            inner join orders_orderrubric on (
                rubrics.id = orders_orderrubric.rubric_id
                and orders_orderrubric.order_id = callerspanel_leadactionlog.target_id
            )
        )

        -- Массив рубрик заказа по дереву вниз
        select array(
            select distinct rub.id from rubrics_rubric as rub
            inner join order_rubrics on (
                rub.tree_id = order_rubrics.tree_id
                and rub.lft >= order_rubrics.lft
                and rub.rght <= order_rubrics.rght
            )
        )
        """

        queryset = queryset.annotate(
            order_rubrics_desc=RawSQL(log_order_rubrics_desc, [])
        ).filter(
            verb=verb,
            recommended_rubrics__overlap=F('order_rubrics_desc')
        ).distinct()

        return queryset.count()


# Рекоменд рубрики
from project.utils.raw_sql import RawSQLManager
ss = """
SELECT logs.id
FROM callerspanel_leadactionlog AS logs
WHERE (
    -- есть пересечение рекомендованных рубрик и рубрик заказа по дереву вниз
    logs.recommended_rubrics && (array((
        -- рубрики заказа
        with order_rubrics as (
            select * from rubrics_rubric as rubrics
            inner join orders_orderrubric ON (
                rubrics.id = orders_orderrubric.rubric_id
                and orders_orderrubric.order_id = logs.target_id
            )
        )

        -- рубрики заказа по дереву вниз
        select distinct rub.id from rubrics_rubric as rub
        inner join order_rubrics on (
            rub.tree_id = order_rubrics.tree_id
            AND rub.lft >= order_rubrics.lft
            AND rub.rght <= order_rubrics.rght
        )
    )))::integer[]
    AND logs.verb = {verb}
)
"""
ss = ss.format(verb=LeadActionLogVerbEnum.ORDER_WAS_CREATED)
result = RawSQLManager(sql_query=ss).execute(flat=True)


def unload(file):
    import dropbox, os, csv, datetime
    from django.db.models.functions import TruncDate, ExtractMonth
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'Месяц/Заказов',
        '0-6',
        '6-10',
        '11-20',
        '21-50',
        '50-inf'
    ])

    dt_from = datetime.date(2018, 8, 1)
    dt_to = datetime.date(2018, 12, 1)

    raw_lead_logs = SalesStatusLog.objects.filter(
        status=SalesStatusTypesEnum.RAW,
        created_at__range=(dt_from, dt_to)
    ).select_related('lead')

    raw_lead_logs = raw_lead_logs.annotate(
        date=TruncDate('created_at')
    ).annotate(
        month=ExtractMonth('created_at')
    ).annotate(
        orders_count=F('lead__user__sales_orders_count_full')
    ).order_by(
        'month'
    )

        months = [8, 9, 10, 11]

        ranges = [
            (0, 6),
            (6, 10),
            (11, 20),
            (21, 50),
            (50, 100000)
        ]
        result = {
            8: {},
            9: {},
            10: {},
            11: {}
        }
        for m in months:
            for r in ranges:
                c = raw_lead_logs.filter(
                    month=m,
                    orders_count__range=r
                )
                result[m].update({
                    r: c.count()
                })

        print(result)

        for key in result:
            counts = result[key]
            row = [key]

            for c in counts.values():
                row.append(c)

            w.writerow(row)

        f.close()

        f = open(file_name, 'rb')
        client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
        f.close()
        os.remove(file_name)


def unload(file):
    """Стандарт Премиум"""
    import dropbox, os, csv, datetime
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'продавец',
        'эффективность',
    ])

    w.writerow(['Необработанные (события, которые могут выпасть)', '', '', ''])

    events = SalesEvent.objects.filter(
        type__in=[
            SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
            SalesEventTypesEnum.TARIFF_PREMIUM_VIEW,
        ],
        created_at__gt=datetime.date(2019, 1, 31)
    )

    print(events.count())

    for event in events:
        user = event.lead.user
        lead = event.lead
        w.writerow([
            host + user.get_absolute_url(),
            lead.caller.name if lead.caller else '',
            lead.sales_productivity,
            event.get_type_display(),
        ])

    w.writerow(['Необработанные из истории (по факту обработанные)', '', '', ''])

    events = SalesEventHistory.objects.filter(
        type__in=[
            SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
            SalesEventTypesEnum.TARIFF_PREMIUM_VIEW,
        ],
        created_at__gt=datetime.date(2019, 1, 31),
        action_on_lead__isnull=True
    )

    print(events.count())

    for event in events:
        user = event.lead.user
        lead = event.lead
        w.writerow([
            host + user.get_absolute_url(),
            lead.caller.name if lead.caller else '',
            lead.sales_productivity,
            event.get_type_display(),
        ])

    w.writerow(['Обработанные (им звонили)', '', '', ''])
    asl = ActionOnSalesLead.objects.filter(
        type__in=[
            SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
            SalesEventTypesEnum.TARIFF_PREMIUM_VIEW,
        ],
        processed_at__gt=datetime.date(2019, 1, 31)
    )

    print(asl.count())

    for event in asl:
        user = event.lead.user
        lead = event.lead
        w.writerow([
            host + user.get_absolute_url(),
            lead.caller.name if lead.caller else '',
            lead.sales_productivity,
            event.get_type_display(),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


# Обновить всем юзерам нотификации
def clear_sn():
    for key in NotificationTypesEnum.CHOICES.keys():
        f = {f'settings__notifications__{key}__email': ''}
        print(User.objects.filter(**f).count())


    uu = User.objects.filter(
        settings__notifications__new_order__email=''
    )

    ss = Settings.objects.filter(user__in=uu)

    print(ss.count())
    for s in ss:
        notifications = s.notifications

        for key in NotificationTypesEnum.CHOICES.keys():

            if notifications[key]['email'] == '' and notifications[key]['push'] == '':
                del notifications[key]

            else:
                if key in notifications and 'email' in notifications[key] and 'push' in notifications[key]:
                    if notifications[key]['email'] == '':
                        notifications[key]['email'] = 'immediately'
                    if notifications[key]['push'] == '':
                        notifications[key]['push'] = 'immediately'

        # Settings.objects.filter(id=s.id).update(notifications=notifications)


def test():
    """Тест id_sequence"""
    from django.db import transaction
    from project.apps.cold_callers.factories.leads import UserLeadFactory

    u = User.objects.last()

    Lead.objects.last().id  # id = 19
    l = UserLeadFactory(user=u)
    l = UserLeadFactory(user=u)  # id = 20

    # Пытаемся создать еще лида с таким user
    with transaction.atomic():
        l = UserLeadFactory(user=u)

    # и еще
    with transaction.atomic():
        l = UserLeadFactory(user=u)

    # Создаем без IntegrityError
    l = UserLeadFactory()  # id = 23


# Стастика по выручке по филиалам
def get_revenue_statistic(date_: date) -> dict:

    users = UserGroupMembership.objects.values_list('user')
    departments = Department.objects.filter(
        user__in=users
    ).order_by('title').distinct()

    subscriptions = Subscription.objects.filter(
        payment_date__year=date_.year,
        payment_date__month=date_.month,
        actual_payment__isnull=False
    ).annotate(
        department=F('seller__department')
    ).values('id', 'actual_payment', 'seller', 'department')

    result = {}

    all_sum = 0

    for department in departments:
        payments = list(filter(
            lambda x: x['department'] == department.id,
            subscriptions
        ))
        ps = round(float(sum(p['actual_payment'] for p in payments)),
                   2) if payments else 0
        result[department.title] = ps
        all_sum += ps

    nobody_payments = list(filter(
        lambda x: x['department'] is None,
        subscriptions
    ))
    n_ps = round(float(sum(p['actual_payment'] for p in nobody_payments)),
                 2) if nobody_payments else 0
    result['without_dep'] = n_ps
    all_sum += n_ps

    result['all'] = all_sum

    return result


def get_origin_for_users():
    """Проставляет origin пользователям с рег руб из логов ip"""
    from project.apps.logs.models import UserIPLog
    users = User.objects.filter(
        rubrics__isnull=False,
        regions__isnull=False,
        origin__isnull=True
    ).distinct()

    big_cities_request = {
        'St Petersburg': 'saint-petersburg',
        'Yekaterinburg': 'ekaterinburg',
        'Nizhniy Novgorod': 'nizhny-novgorod',
        'Karagandy': 'karaganda',
        'Kazanâ': 'kazan'
    }

    has_city_log = 0
    has_matched_region = 0

    updated_user_ids = []
    for user in users:
        log = UserIPLog.objects.filter(
            user=user
        ).order_by('ip_changed_at').first()

        if log:
            has_city_log += 1

            city = log.city
            if city in big_cities_request:
                city_slug = big_cities_request[city]
            else:
                city_slug = '-'.join(city.split()).lower()

            region = Region.objects.filter(slug=city_slug).first()

            if region:
                updated_user_ids.append(user.id)
                User.objects.filter(id=user.id).update(origin=region)
                has_matched_region += 1

    print(f'has_city_log = {has_city_log};'
          f'has_matched_region = {has_matched_region}')

    cache.set('get_origin_for_users', updated_user_ids, 60 * 60 * 24)

    return updated_user_ids



# Тест выборки поставщиков
from project.apps.suppliers_main.helpers.selectors.raw_user_and_candidate_selector import UserAndCandidateRawSelector
from project.apps.suppliers_main.helpers.selectors.user_and_candidate import UserAndCandidateSelector
from project.apps.suppliers_main.helpers.selectors.raw_orders import SuitableOrderRawSelector
from project.apps.suppliers_main.helpers.selectors.raw_sql_base import RawSQLBase
from project.apps.suppliers_main.helpers.selectors.orders import *


class PatchedOrderSelectorByGroup(OrderSelectorByGroup):
    @staticmethod
    def get_filter_by_statuses():
        """Заказ должен быть опубликован, его нужно разослать и
        он должен быть подходящим для поставщиков"""
        return Q(
            status=OrderStatusEnum.PUBLISHED,
            need_send=True,
            suitable_for_suppliers=True,
        )


class PatchedSuitableOrderRawSelector(SuitableOrderRawSelector):

    LIST_JOINS_METHODS = [
        'get_regions_join',
        'get_rubrics_join',
    ]

    LIST_FILTERS_METHODS = [
        'get_filter_by_ids',
        'get_filter_by_from_supplier_logs',
        'get_filter_by_rubrics',
        'get_filter_by_regions',
        'get_filter_by_supply_city',
        'get_filter_by_creator',
    ]

    def get_filter_by_ids(self):
        """Возвращает фильтр по id для заказов"""

        helper = PatchedOrderSelectorByGroup(group=self.group)

        order_ids = helper.get_ids()

        print(f'SuitableOrderRawSelector: order_ids = {len(order_ids)}')

        if not order_ids:
            return " (FALSE) "

        order_ids = ','.join(map(str, order_ids))

        return """

           ( "orders_order"."id" IN ({order_ids}))
        """.format(order_ids=order_ids)


# тест, сколько поставщиков подходят под фильтры
def test_suppliers_count():
    group = SupplierSettingsGroup.objects.get(id=10)  # Томск
    model = User

    all_instance_ids = UserAndCandidateSelector(
        model=model,
        group=group
    ).get_ids()
    print('all instance ids: ', len(all_instance_ids))

    helper = UserAndCandidateRawSelector(
        model=model,
        group=group,
        instance_ids=all_instance_ids
    )
    # print(helper.get_query())
    helper.order_selector_class = PatchedSuitableOrderRawSelector
    helper.order_selector = helper.order_selector_class(model=model, group=group)

    ids = helper.get(flat=True)
    count = len(set(ids))

    print(f'count: {count}')


def test_utm_marks():
    import json
    # users = User.objects.filter(id__in=[1468463, 1469172, 1472527, 1468877])
    users = User.objects.filter(
        date_joined__gt=timezone.now() - timezone.timedelta(days=1),
        creator__staff_status__isnull=True,
    ).filter(Q(utm_marks__isnull=False) | Q(initial_utm_marks__isnull=False)).distinct()

    for user in users:
        orders = user.orders.filter(
            created_at__date=user.date_joined.date()
        )
        ou = orders.values('utm_marks', 'created_at')
        if orders:
            print(
                f'user_date_joined: {user.date_joined}\n'
                f'initial_utm_marks: {json.loads(user.initial_utm_marks) if user.initial_utm_marks else None}\n'
                f'user_utm_marks: {user.utm_marks}\n'
                f"order_utm_marks_and_created_at: {ou}\n\n"
            )


def generate_landings():
    from project.apps.regions.models import Region
    from project.core.morphology import slugify
    from copy import copy

    regions = Region.objects.filter(
        title__in=[
            'Москва',
            'Санкт-Пертербург',
            'Новосибирск',
            'Екатеринбург',
            'Нижний Новгород',
            'Казань',
            'Челябинск',
            'Омск',
            'Самара',
            'Ростов-на-Дону',
        ]
    )

    supplier_landings = SuppliersLanding.objects.all()

    for land in supplier_landings:
        for region in regions:
            try:
                landing = copy(land)

                new_title = landing.title.replace('?', f' в {region.name_loct}?')
                new_content = (
                    f'Клиенты из {region.name_gent} на вашу продукцию здесь. '
                    f'Посмотрите заказы, оставьте предложения с вашей ценой или '
                    f'запросите обратный звонок нашего менеджера для консультации.'
                )
                new_meta_title = landing.meta_title.replace(
                    '?', f' в {region.name_loct}?')
                new_meta_description = landing.meta_description.replace(
                    ', но', f' в {region.name_loct}, но'
                )
                new_slug = slugify(new_title)

                landing.pk = None
                landing.title = new_title
                landing.content = new_content
                landing.meta_title = new_meta_title
                landing.meta_description = new_meta_description
                landing.slug = new_slug[:-1]  # Удаляем дефис в конце

                landing.save()
            except Exception:
                pass


def check_indexing():
    from project.apps.proposals.elastic_search.proposal import EsProposalContainer
    from project.libs.elastic.builder import EsBuilderLogger

    es_container = EsUserContainer()
    i = EsBuilderLogger(es_container.index).get_info()
    print(i.key_hash)
    print(i)

curl -X GET "10.100.0.4:9200/profiles/_search" -H 'Content-Type: application/json' -d' { "query": { "multi_match": { "query": "880055535", "fields": [ "phone", "olden_phones", "email", "olden_emails", "additional_emails", "name", "company_name", "inn", "site" ] }}} '

curl -X GET "10.100.0.4:9200/profiles/_search" -H 'Content-Type: application/json' -d' { "query": { "bool": { "should": [ {"multi_match": { "query": "880055535", "fields": [ "phone", "olden_phones", "email", "olden_emails", "additional_emails", "name", "company_name", "inn", "site" ] }}]}}} '

curl -X POT "10.100.0.4:9200/_aliases" -H 'Content-Type: application/json' -d' {"actions" : [{ "add" : { "index" : "profiles_iyr2qv5378", "alias" : "profiles" } }]} '
{"actions" : [{ "add" : { "index" : "profiles_iyr2qv5378", "alias" : "profiles" } }]}



docker run -d --name reindex-2 --env-file /etc/suplbiz-gunicorn.conf -e DJANGO_SETTINGS_MODULE=project.settings.prod -e PYTHONPATH=/home/suplbiz/project/ -v /tmp:/tmp  docker.supl.biz/suplbiz-gunicorn:1ee04e18


docker run -d --name reindex-profiles-5 \
    --env-file /etc/suplbiz-gunicorn.conf \
    -v /tmp:/tmp \
    --entrypoint=django-admin.py \
    docker.supl.biz/suplbiz-gunicorn:995dc73c \
    es_reindex_partial_profiles 80 100


# Вытащить из эластика все id сущностей
def scroll_all():
    import elasticsearch
    from time import sleep

    es = elasticsearch.Elasticsearch('10.100.0.100')
    query = {
        'size': 10000,
        'query': {
            'match_all': {}
        }
    }

    ids = []

    res = es.search(
        index='profiles', doc_type='profile', body=query, scroll='1m'
    )
    ids.extend([i['_id'] for i in res['hits']['hits']])
    scroll_id = res['_scroll_id']

    for i in range(120):
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
        ids.extend([int(i['_id']) for i in res['hits']['hits']])
        scroll_id = res['_scroll_id']
        sleep(0.1)

    return ids


def cache_ids_for_reindex():
    users_db_ids = list(User.objects.only('id').values_list('id', flat=True))
    users_es_ids = scroll_all()

    ids = set(users_db_ids) - set(users_es_ids)
    ids = list(ids)
    ids.sort()
    cache.set('post_indexing', ids, 60*60*48)


from project.apps.index.tasks import index_profile
ids = cache.get('post_indexing')
for i in ids[400000:500000]:
    index_profile(i)


def invalidate_cache(path='/api/v1.0/rubrics/tree/ru/'):
    import socket
    from django.core.cache import cache
    from django.http import HttpRequest
    from django.utils.cache import get_cache_key

    request = HttpRequest()
    domain = 'supl.biz'
    request.META = {'SERVER_NAME': socket.gethostname(), 'SERVER_PORT': 80, "HTTP_HOST": domain, 'HTTP_ACCEPT_ENCODING': 'gzip, deflate, br'}
    request.LANGUAGE_CODE = 'ru-RU'
    request.path = path
    print(request.META)

    try:
        cache_key = get_cache_key(request)
        if cache_key :
            if cache.has_key(cache_key):
                cache.delete(cache_key)
                return (True, 'successfully invalidated')
            else:
                return (False, 'cache_key does not exist in cache')
        else:
            raise ValueError('failed to create cache_key')
    except (ValueError, Exception) as e:
        return (False, e)


from project.apps.regions.utils.utils import get_regions_for_default_popup
from project.apps.statistics.helpers.home_stats.orders import actualize_orders_count_stats
from project.apps.statistics.helpers.home_stats.proposals import actualize_proposals_count_stats
def update_main_orders_statistic():
    regions = get_regions_for_default_popup()

    for region in regions[10:]:
        actualize_orders_count_stats(region.id)
        actualize_proposals_count_stats(region.id)


# Создать новый контейнер на песке
docker run -d --name copy_settings --env-file /etc/suplbiz-gunicorn.conf -e DJANGO_SETTINGS_MODULE=project.settings.prod -e PYTHONPATH=/home/suplbiz/project/ -v /tmp:/tmp  docker.supl.biz/suplbiz-gunicorn:5fc65504



def replace_rubrics_in_orders():
    # удалить рубрики у заказов, добавить новые
    rubs = Rubric.objects.filter(title__in=(
        'Автозапчасти для грузовых автомобилей',
        'Оптом: Автозапчасти для грузовых автомобилей'
    ))

    desc = rubs.get_descendants()

    orders = Order.objects.filter(rubrics__in=desc)
    print(orders.count())
    cache.set('order_remove_rubrics', orders.values_list('id', flat=True), timeout=60*60*24)

    deleted_count = 0
    created_count = 0
    for order in orders:
        o_rub = order.rubrics.all()
        for r in o_rub:
            if r in desc:
                OrderRubric.objects.get(order=order, rubric=r).delete()
                deleted_count += 1
                o, created = OrderRubric.objects.get_or_create(order=order, rubric=r.parent)
                if created:
                    created_count += 1
    return deleted_count, created_count



def proposal_views():
    """Просмотры товаров. """
    proposal_ct = ContentType.objects.get_for_model(Proposal)

    step = 10
    for i in range(0, 100, step):
        views = PageView.objects.filter(
            content_type=proposal_ct,
        ).annotate(
            total=F('amount_users') + F('amount_robots') + F('amount_emails') + F('views')
        ).filter(total__range=(i, i + step))

        proposals = Proposal.objects.filter(id__in=views.values_list('object_id'))
        print(f'{i}-{i + step}: {proposals.count()}')

    step = 50
    for i in range(100, 1000, step):
        views = PageView.objects.filter(
            content_type=proposal_ct,
        ).annotate(
            total=F('amount_users') + F('amount_robots') + F('amount_emails') + F('views')
        ).filter(total__range=(i, i + step))

        proposals = Proposal.objects.filter(id__in=views.values_list('object_id'))
        print(f'{i}-{i + step}: {proposals.count()}')



def pp():
    """Товары с топ просмотров по рубрике"""
    rub = Rubric.objects.get(title='Строительные и отделочные материалы')

    proposals = Proposal.objects.filter(
        rubrics__in=[rub.id],
        created_at__gt=timezone.now() - timezone.timedelta(days=180),
    )

    proposal_ct = ContentType.objects.get_for_model(Proposal)
    views = PageView.objects.filter(
        content_type=proposal_ct,
        object_id__in=proposals.values_list('id')
    ).annotate(
        total=F('amount_users') + F('amount_robots') + F('amount_emails') + F('views')
    ).order_by('-total')

    for view in views[:20]:
        print(f'https://supl.biz/proposals/{view.object_id}/', view.total)


def proposals_from_demo_users():
    return Proposal.objects.filter(
        status='published',
        user__subscription__isnull=True,
        user__parent__subscription__isnull=True
    ).count()


def proposals_from_was_on_tariff_users():
    today = timezone.now().date()
    ss = Subscription.objects.exclude_demo().exclude(
        finish_date__gte=today,
        start_date__lte=today
    )

    qs = User.objects.filter(id__in=ss.values_list('user'))

    uids = list(qs.values_list('id', flat=True))

    return Proposal.objects.filter(
        status='published',
        user_id__in=uids
    ).count()


def proposals_from_tariff_users():
    today = timezone.now().date()
    ss = Subscription.objects.exclude_demo().filter(
        finish_date__gte=today,
        start_date__lte=today
    )

    qs = User.objects.filter(id__in=ss.values_list('user'))

    uids = list(qs.values_list('id', flat=True))

    return Proposal.objects.filter(
        status='published',
        user_id__in=uids
    ).count()


# Запуск скрипта в окнтейнере
docker run -d --name page_view_set_owner_id \
--env-file /etc/suplbiz-gunicorn.conf \
-v /tmp:/tmp \
--entrypoint=django-admin.py \
docker.supl.biz/suplbiz-gunicorn:7a20af12 \
runscript page_view_set_owner_id


def parse():
    """Парсит region_key с api Яндекса. """
    from urllib.parse import quote

    region = 'Москва'
    region = quote(region)

    url = (
        f'https://suggest-maps.yandex.ru/suggest-geo'
        f'?search_type=tune&'
        f'v=9&'
        f'results=15&'
        f'lang=ru&'
        f'callback=jQuery183024443942322682188_1559192475463'
        f'&part={region}'
    )
    r = requests.get(url=url)
    r.raise_for_status()

    result = r.content
    print(result)


# docker cp /home/anton/Downloads/ya_region_keys.csv ef1d61e4b265:/tmp/ya_region_keys.csv
# docker cp ef1d61e4b265:/tmp/ya_region_keys_new.csv /home/anton/Downloads/ya_region_keys_new.csv


def extract():
    """ """
    import csv
    import re

    input_fn = '/tmp/ya_region_keys.csv'
    in_f = open(input_fn, 'r', encoding='utf-8')
    in_file_reader = csv.reader(in_f)

    output_fn = '/tmp/ya_region_keys_new.csv'
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    for row in in_file_reader:
        string = row[0]
        digit = re.search(r'(\d+)', string).group(1)
        region = string.replace(digit, '')

        our_writer.writerow([
            digit,
            region,
        ])

    in_f.close()
    out_f.close()



def views():
    """Просмотры товаров за ввчера """
    from project.apps.statistics.models import InstanceView, InstanceViewSourcesEnum
    from django.contrib.contenttypes.models import ContentType
    from project.apps.proposals.models import Proposal
    ct = ContentType.objects.get_for_model(Proposal)

    yesterday = timezone.now() - timezone.timedelta(days=1)
    views = InstanceView.objects.filter(
        object_type=ct,
        created_at__date=yesterday
    )

    auto_fitted = InstanceView.objects.filter(
        object_type=ct,
        created_at__date=yesterday,
        source=InstanceViewSourcesEnum.ORDER
    )
    print(views.count())
    print(auto_fitted.count())




# SQl RANK
from project.core.db.raw_sql import RawSQLManager
sql = """

WITH user_scores AS (
    SELECT U.id, SUM(US.score) as total_score
    FROM users_user as U
    LEFT JOIN scores_score as US
    ON (US.user_id = U.id)
    GROUP BY U.id
)

SELECT * from (
    SELECT users_user.id, US.total_score, rank() over(
        ORDER BY US.total_score DESC NULLS LAST
    ) as position
    FROM users_user
    INNER JOIN user_scores AS US
    ON US.id = users_user.id
) result
WHERE id = 1031096

"""
res = RawSQLManager(sql_query=sql).execute()



from django.db.models.expressions import Window
from django.db.models.functions import RowNumber, DenseRank
from django.db.models import F

results = User.objects.annotate(row_number=Window(
    expression=RowNumber(),
    order_by=F('date_joined').desc())
).order_by('row_number', 'name')



def update_source():
    import re
    users = ['https://admin.supl.biz/profiles/1271753', ]
    ids = []
    for u in users:
        uid = re.findall(r'\d+', u)[0]
        ids.append(int(uid))

    userss = User.objects.filter(id__in=ids)
    for u in userss:
        u.source = UserSourceEnum.YANDEX_DIRECT
        u.source_date = timezone.now()
        u.save(update_fields=['source', 'source_date'])



class DeficitCustomersMatrixCell(MatrixCellBase):
    """Ячейка матрицы заказов
    Имеет одно поле

    customers_deficit_coefficient
        Показывает:
             Насколько по выбранной рубрике и региону наблюдается дефицит
             заказов у нас на площадке

        Рассчитывается следующим образом:
            Сначала линейно возрастает с 0 заказов до 19
            y = 0.05 count_orders + 2

            Затем небольшой обрыв и экспоненциальное падение

            y = e^(0.816439 + 0.008219 * count_orders)

            Коэффициенты подобраны следующим образом:
            Для того, что бы y(15) = 2, y(50) = 1.5

    count_suppliers - Количество поставщиков в данной ячейке(рубрике регионе)
    """

    customers_deficit_coefficient = MatrixCellArtifact(
        initial_value=INITIAL_CUSTOMER_COEFFICIENT_FOR_SUPPLIERS
    )
    count_suppliers = MatrixCellArtifact(initial_value=0)
    count_paid_suppliers = MatrixCellArtifact(initial_value=0)

    def calculate_customers_deficit_coefficient(self) -> float:
        """Рассчитывает дефицит заказов у нас на площадке"""
        count_orders = self.matrix.stats_orders_count[self.matrix_key]

        if count_orders < 15:
            value = 2 + count_orders * 0.05
        else:
            value = exp(0.816439 - 0.008219 * count_orders)

        return round(value, 2)

    def calculate_count_suppliers(self) -> int:
        """Рассчитывает количество поставщиков"""
        rubrics = self.matrix_key.rubric.get_descendants(include_self=True)
        regions = self.matrix_key.region.get_descendants(include_self=True)

        suppliers_count = User.objects.filter(
            rubrics__in=rubrics,
            regions__in=regions
        ).count()
        return suppliers_count

    def calculate_count_paid_suppliers(self) -> int:
        """Рассчитывает количество поставщиков"""

        rubrics = self.matrix_key.rubric.get_descendants(include_self=True)
        regions = self.matrix_key.region.get_descendants(include_self=True)

        suppliers_count = User.objects.filter(
            id__in=self.paid_user_ids,
            rubrics__in=rubrics,
            regions__in=regions
        ).count()
        return suppliers_count

    def get_value(self) -> int:
        """Возвращает значение, которые записывается в поля
        as_real_customers_priority и as_potential_customers_priority

        Рассчитываем таким образом для достижения фильтрации сначала по
        customers_deficit_coefficient, а затем по count_suppliers
        """
        return int(
            self.customers_deficit_coefficient * 10**6 + self.count_suppliers
        )

    @cached_property
    def paid_user_ids(self):
        from project.apps.tariffs.models import Subscription

        return list(Subscription.objects.active().values_list('user', flat=True))


false_event_data = {
    'date_from': datetime.date(2019, 7, 10).isoformat(),
    'date_to': datetime.date(2019, 7, 20),
    'tpl': 'order_published',
    'tpl_display': 'Заказ опубликован',
    'actor_id': 1031096,
    'actor_email': 'antonfewwt@gmail.com',
}

def test_send_report():
    import datetime

    event_data = {
        'date_from': datetime.date(2019, 7, 10).isoformat(),
        'date_to': datetime.date(2019, 7, 20).isoformat(),
        'tpl': 'order_published',
        'tpl_display': 'Заказ опубликован',
        'actor_id': 5,
        'actor_email': 'asdf@asdf.ru',
    }

    send_report_request(event_data)


def test_send_email_template():
    """Отправляет тестовое письмо, чтобы проверить корректность шаблона на локалке. """
    from project.apps.notifications.utils import get_fake_user_for_notification
    from project.apps.notifications.models import Activity, Notification
    from project.apps.notifications.handlers import do_send_activity
    from django.contrib.contenttypes.models import ContentType

    fake_user = get_fake_user_for_notification()

    order_ct = ContentType.objects.get_by_natural_key('orders', 'order')
    order = Order.objects.first()
    user = User.objects.get(email='testik.supl.biz@mail.ru')

    activity, created = Activity.objects.get_or_create(
        verb='new_order',
        actor_id=order.user_id,
        object_type=order_ct,
        object_id=order.id
    )

    Notification.objects.create(
        activity=activity,
        user_id=user.id,
    )
    do_send_activity(activity.id)
