def extract_digit_from_string(str, start_substr, end_substr):
    """Извлекает число из подстрок. """
    import re

    digit = re.search(
        r'{0}(\d+){1}'.format(start_substr, end_substr), str
    ).group(1)

    return digit


def upload_file():
    """Скачивае файл, записываем в tempfile, а затем в storage. """
    import requests
    import tempfile
    from django.core.files import File
    from project.apps.contactus.models import PdfFile
    from django.core.files.storage import FileSystemStorage

    url = 'https://suplbiz-a.akamaihd.net/uploads/Shablondogovorsuplbiz.pdf'

    tmp = tempfile.NamedTemporaryFile(delete=True)
    r = requests.get(url)
    tmp.write(r.content)
    tmp.flush()

    fs = FileSystemStorage()
    storage_file = fs.save('contract_template/Shablondogovorsuplbiz.pdf', File(tmp))

    # pdf_file = PdfFile(
    #     type='contract_template'
    # )
    # pdf_file.file.name = storage_file.name
    # pdf_file.save()



def unload(file):
    import dropbox, os, csv, datetime
    from project.apps.orders.models.order import OrderSourceEnum

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'link',
        'дата создания',
        'дата лога',
        'статус',
    ])
    local_tz = timezone.pytz.timezone('Asia/Tomsk')

    date_start = datetime.date(2018, 10, 1)

    orders = Order.objects.filter(
        created_at__gt=date_start,
        source__in=OrderSourceEnum.ADS_SOURCES
    ).order_by('id')

    for order in orders:
        log = ModerationSessionLog.objects.filter(
            customer=order.user,
            created_at__gt=order.created_at,
        ).order_by('-created_at').first()
        w.writerow([
            host + order.get_absolute_url() + 'admin/',
            timezone.localtime(order.created_at, timezone=local_tz).strftime('%d-%m-%Y %H:%M'),
            timezone.localtime(log.created_at, timezone=local_tz).strftime('%d-%m-%Y %H:%M') if log else '',
            order.get_status_display(),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



# Применить squahed миграции
from django.db.migrations.recorder import MigrationRecorder

MigrationModel = MigrationRecorder.Migration

for app, name in [
    ('backend_sales', '0001_squashed_0033_auto_20181220_1119'),
    ('proposals', '0001_squashed_0017_auto_20181130_0644',),
    ('suppliers_main', '0001_squashed_0018_auto_20181127_0912'),
    ('logs', '0001_squashed_0005_auto_20180816_1440'),
    ('faq', '0001_squashed_0004_auto_20180927_1244'),
    ('cold_callers', '0001_squashed_0035_leadactionlog_recommended_rubrics'),

]:
    if MigrationModel.objects.filter(app=app, name=name).exists():
        continue

    MigrationModel.objects.create(app=app, name=name)


# Статистика по запросам
cat /var/log/docker/postgres.log | sed 's/^\s*./0/g' | pgbadger -f '%t'


def copy_template_content():
    templates = Template.objects.all()

    locales = ['kk', 'en', 'zh']

    fields = [
        'email_a_subj',
        'email_a_text',
        'email_a_html',

        'email_b_subj',
        'email_b_text',
        'email_b_html',
    ]
    for t in templates:
        for locale in locales:
            for field in fields:
                root_field = f'{field}_ru'
                filled_field = f'{field}_{locale}'

                if not getattr(t, filled_field):
                    setattr(t, filled_field, getattr(t, root_field))
        t.save()

# Test raw sql_query
def test_j2():
    query = f"""
        WITH with_users_with_subscription AS (
            SELECT tariffs_subscription.user_id FROM tariffs_subscription
            WHERE tariffs_subscription.finish_date >=
                (tariffs_subscription.start_date + INTERVAL '7' day)
        )

        SELECT DISTINCT users_user.id FROM users_user
        LEFT JOIN with_users_with_subscription
            ON (users_user.id = with_users_with_subscription.user_id)
        WHERE with_users_with_subscription.user_id IS NULL
    """
    res = RawSQLManager(sql_query=query).execute(flat=True)
    print(len(res))


def test_j3():
    query = f"""
        WITH with_users_has_asl as (
            SELECT sales_lead.user_id
            FROM action_on_sales_lead as asl
            INNER JOIN sales_lead
                ON (asl.lead_id = sales_lead.id)
            WHERE asl.created_at >=
                CURRENT_TIMESTAMP - INTERVAL '30' DAY
        )

        SELECT DISTINCT users_user.id FROM users_user
        LEFT JOIN with_users_has_asl
            ON (users_user.id = with_users_has_asl.user_id)
        WHERE with_users_has_asl.user_id IS NULL
    """
    res = RawSQLManager(sql_query=query).execute(flat=True)
    print(len(res))


    ids = ActionOnSalesLead.objects.filter(
        created_at__gte=get_days_ago(30)
    )
    qs = User.objects.filter(~Q(id__in=ids.values_list('lead__user_id'))).distinct()
    print(qs.count())


def test_asl():
    query = f"""
        WITH with_users_has_asl_duration as (
            SELECT sales_lead.user_id
            FROM action_on_sales_lead as asl
            INNER JOIN sales_lead
                ON (asl.lead_id = sales_lead.id)
            WHERE asl.processed_at >= CURRENT_TIMESTAMP - INTERVAL '28' DAY
                AND asl.duration IS NOT NULL
            GROUP BY sales_lead.user_id, asl.processed_at
            HAVING SUM (asl.duration) >= 3
        )

        SELECT DISTINCT users_user.id FROM users_user
        LEFT JOIN with_users_has_asl_duration
            ON (users_user.id = with_users_has_asl_duration.user_id)
        WHERE with_users_has_asl_duration.user_id IS NULL
    """
    query2 = f"""
            SELECT distinct sales_lead.user_id
            FROM action_on_sales_lead as asl
            INNER JOIN sales_lead
                ON (asl.lead_id = sales_lead.id)
            WHERE asl.processed_at >= CURRENT_TIMESTAMP - INTERVAL '28' DAY
                AND asl.duration IS NOT NULL
            GROUP BY sales_lead.user_id, asl.processed_at
            HAVING SUM (asl.duration) >= 3
    """

    res = RawSQLManager(sql_query=query).execute(flat=True)
    res2 = RawSQLManager(sql_query=query2).execute(flat=True)
    print(len(res))
    print(len(res2))

    # --------------------
    date_for_duration = get_days_ago(28)
    ids = ActionOnSalesLead.objects.filter(
        processed_at__gte=date_for_duration,
        duration__isnull=False
    ).values('lead__user_id').annotate(
        sum_duration=Sum('duration')
    ).filter(
        sum_duration__gte=3
    )
    print(len(set(ids.values_list('lead__user_id'))))
    qs = User.objects.exclude(
        id__in=ids.values_list('lead__user_id')
    ).distinct()
    print(qs.count())


def test_se():
    query = f"""
        WITH with_users_has_planned_call as (
            SELECT sales_lead.user_id
            FROM sales_event as se
            INNER JOIN sales_lead
                ON (se.lead_id = sales_lead.id)
            WHERE se.planned_datetime > CURRENT_TIMESTAMP
                AND se.type = {SalesEventTypesEnum.PLANNED_CALL}
        )

        SELECT DISTINCT users_user.id FROM users_user
        LEFT JOIN with_users_has_planned_call
            ON (users_user.id = with_users_has_planned_call.user_id)
        WHERE with_users_has_planned_call.user_id IS NULL
    """
    res = RawSQLManager(sql_query=query).execute(flat=True)
    print(len(res))

    ids = SalesEvent.objects.filter(
        type=SalesEventTypesEnum.PLANNED_CALL,
        planned_datetime__gt=timezone.now(),
    )
    qs = User.objects.filter(
        ~Q(id__in=ids.values_list('lead__user_id'))
    ).distinct()
    print(qs.count())



# imports
import random
from functools import reduce
from operator import and_
from django.db import transaction
from funcy import partition, first, pluck, str_join
from project.apps.suppliers_main.helpers.sales_status import *
from project.apps.suppliers_main.utils import *
from project.core.timezones import *
from project.core.db import *
from project.apps.suppliers_main.helpers.selectors.user_and_candidate import *
from project.apps.suppliers_main.helpers.selectors.raw_user_and_candidate_selector import *
from project.apps.suppliers_main.helpers.selectors.raw_sql_base import *
from project.apps.suppliers_main.helpers.selectors.raw_orders import *
from project.apps.suppliers_main.helpers import delete_users_from_hot_leads


class CommonSelector(RawSQLBase2):
    LIST_JOINS_METHODS = [
        'get_origins_join',
        'get_regions_join',
        'get_rubrics_join',
        'get_supplier_lead_join',
        'get_company_profile_join',
        'get_with_users_has_offers_join',
        'get_with_users_has_subscriptions_join',
        'get_with_users_has_asl_join',
        'get_with_users_has_asl_duration_join',
        'get_with_users_has_planned_call_join',
    ]

    LIST_FILTERS_METHODS = [
        'get_filter_by_orders',
        'get_filter_by_origin',
        'get_filter_by_rubrics',
        'get_filter_by_orders_count',
        'get_filter_by_crm_url',
        'get_filter_by_user_id',
        'get_filter_by_stop_word_in_company_name',
        'get_filter_by_supplier_caller',
        'get_filter_by_supplier_status',
        'get_filter_by_user_phone_and_site',
        'get_filter_by_created_offers',
        'get_filter_by_tariff_statuses',
        'get_filter_by_asl',
        'get_filter_by_asl_duration',
        'get_filter_by_sales_planned_calls',
    ]

    def __init__(self, model, group):
        assert issubclass(model, (User, PotentialSupplier)), model
        assert isinstance(group, SupplierSettingsGroup), group

        self.group = group
        self.model = model
        self.is_user = issubclass(self.model, User)
        self.user_table = (
            User._meta.db_table
            # if self.is_user else PotentialSupplier._meta.db_table
        )
        self.order_selector = SuitableOrderRawSelector(
            group,
            model,
        )
        self.ignore_orders_count = False
        super().__init__()

    def get_with_clause(self):
        return f"""
            -- Пользователи, недавно оставлявшие предложения
            WITH with_users_has_offers AS (
                SELECT users_user.id FROM users_user
                LEFT JOIN offers_offer ON users_user.id = offers_offer.user_id
                WHERE offers_offer.created_at >=
                    CURRENT_TIMESTAMP - INTERVAL '{self.group.period}' DAY
            ),
            -- Пользователи, у которых была реальная подписка
            with_users_has_subscriptions AS (
                SELECT tariffs_subscription.user_id FROM tariffs_subscription
                WHERE tariffs_subscription.finish_date >=
                    (tariffs_subscription.start_date + INTERVAL '7' DAY)
            ),
            -- Пользователи, которым недавно звонил отдел продаж
            with_users_has_asl AS (
                SELECT sales_lead.user_id
                FROM action_on_sales_lead AS asl
                INNER JOIN sales_lead
                    ON (asl.lead_id = sales_lead.id)
                WHERE asl.processed_at >=
                    CURRENT_TIMESTAMP - INTERVAL '{self.group.period}' DAY
            ),
            -- Пользователи, которым отдел продаж звонил за 4 последних недели
            -- 3 и более минут
            with_users_has_asl_duration as (
                SELECT sales_lead.user_id
                FROM action_on_sales_lead as asl
                INNER JOIN sales_lead
                    ON (asl.lead_id = sales_lead.id)
                WHERE asl.processed_at >= CURRENT_TIMESTAMP - INTERVAL '28' DAY
                    AND asl.duration IS NOT NULL
                GROUP BY sales_lead.user_id, asl.processed_at
                HAVING SUM (asl.duration) >=
                    {SalesPositionIdentifier.TALK_DURATION_LIMIT_FOR_FOUR_WEEK}
            ),
            -- Пользователи, у которых есть запланированные звонки отдела продаж
            with_users_has_planned_call as (
                SELECT sales_lead.user_id
                FROM sales_event as se
                INNER JOIN sales_lead
                    ON (se.lead_id = sales_lead.id)
                WHERE se.planned_datetime > CURRENT_TIMESTAMP
                    AND se.type = {SalesEventTypesEnum.PLANNED_CALL}
            )
        """

    def get_select_clause(self):
        return f"""
            SELECT users_user.id,
            {self.is_user} as is_user,
            users_user.sales_orders_count_desc
        """

    def get_from_clause(self):
        return (
            'users_user' if self.is_user
            else 'suppliers_panel_potentialsupplier AS users_user'
        )

    def get_order_by(self):
        return 'ORDER BY users_user.sales_orders_count_desc DESC NULLS LAST'

    def get_origins_join(self):
        """Join на origin и parent origin. """
        if (self.group.searching_order_for_lead_method ==
                SearchingOrderForLeadMethodEnum.BY_REGIONS):
            return f"""
                INNER JOIN {self.region_db_table} AS user_origins
                    ON (users_user.origin_id = user_origins.id)
            """

        if (self.group.searching_order_for_lead_method ==
                SearchingOrderForLeadMethodEnum.BY_OKRUG):
            join_parent_of_parent = f"""
            INNER JOIN {self.region_db_table} AS parent_of_parent_user_origins
                ON (parent_user_origins.parent_id = parent_of_parent_user_origins.id)
            """
        else:
            join_parent_of_parent = ''

        return f"""
            INNER JOIN {self.region_db_table} AS user_origins
                ON (users_user.origin_id = user_origins.id)

            INNER JOIN {self.region_db_table} AS parent_user_origins
                ON (user_origins.parent_id = parent_user_origins.id)
            {join_parent_of_parent}
        """

    def get_regions_join(self):

        if not (self.group.searching_order_for_lead_method ==
                SearchingOrderForLeadMethodEnum.BY_REGIONS):
            return ''

        users_userregion_table = (
            '{}'.format(UserRegion._meta.db_table)
            if self.is_user
            else '{}_regions'.format(PotentialSupplier._meta.db_table)
        )
        user_id_field = 'user_id' if self.is_user else 'potentialsupplier_id'

        return f"""
            INNER JOIN {users_userregion_table} AS users_userregion
                ON (users_user.id = users_userregion.{user_id_field})
            INNER JOIN {self.region_db_table} AS user_regions
                ON (users_userregion.region_id = user_regions.id)
        """

    def get_rubrics_join(self):
        """Join на рубрики"""

        users_userrubric_table = (
            '{}'.format(UserRubric._meta.db_table)
            if self.is_user
            else '{}_rubrics'.format(PotentialSupplier._meta.db_table)
        )
        user_id_field = 'user_id' if self.is_user else 'potentialsupplier_id'

        return f"""
            INNER JOIN {users_userrubric_table} AS users_userrubric
                ON (users_user.id = users_userrubric.{user_id_field})
            INNER JOIN {self.rubric_db_table} AS user_rubrics
                ON (users_userrubric.rubric_id = user_rubrics.id)
        """

    def get_supplier_lead_join(self):
        """Join на SupplierLead. """

        user_id_field = 'user_id' if self.is_user else 'user_candidate_id'

        return f"""
            LEFT JOIN supplier_leads
                ON {self.user_table}.id = supplier_leads.{user_id_field}
        """

    def get_company_profile_join(self):
        if not issubclass(self.model, User):
            return ''
        return """
            INNER JOIN users_companyprofile
                ON (users_user.company_profile_id = users_companyprofile.id)
        """

    def get_with_users_has_offers_join(self):
        return """
            LEFT JOIN with_users_has_offers
                ON (users_user.id = with_users_has_offers.id)
        """

    def get_with_users_has_subscriptions_join(self):
        return """
            LEFT JOIN with_users_has_subscriptions
                ON (users_user.id = with_users_has_subscriptions.user_id)
        """

    def get_with_users_has_asl_join(self):
        return """
            LEFT JOIN with_users_has_asl
                ON (users_user.id = with_users_has_asl.user_id)
        """

    def get_with_users_has_asl_duration_join(self):
        return """
            LEFT JOIN with_users_has_asl_duration
                ON (users_user.id = with_users_has_asl_duration.user_id)
        """

    def get_with_users_has_planned_call_join(self):
        return """
            LEFT JOIN with_users_has_planned_call
                ON (users_user.id = with_users_has_planned_call.user_id)
        """

    def get_filter_by_orders(self):
        """Запрос на существование заказов"""
        query_pattern = """
            (
                exists ({orders_query})
            )
        """
        orders_query = self.order_selector.get_query()
        query = query_pattern.format(orders_query=orders_query)

        return query

    def get_filter_by_origin(self):
        """Возвращает фильтр по origin. """
        return f"""
            user_origins.tree_id = {self.group.country.tree_id}
        """

    def get_filter_by_rubrics(self):
        """Возвращает фильтр по рубрикам. """

        tree_ids = str_join(
            ', ',
            self.group.rubrics.values_list('tree_id', flat=True)
        )
        return f"""
            user_rubrics.tree_id IN ({tree_ids})
        """

    def get_filter_by_orders_count(self):
        """Фильтруем по количеству заказов за последний месяц"""

        # TODO
        if self.ignore_orders_count:
            return ''

        if self.group.orders_count_full_tree:
            field_name = 'sales_orders_count_full'
        else:
            field_name = 'sales_orders_count_desc'

        return f"""
            (
                {self.user_table}.{field_name} BETWEEN
                {self.group.orders_count_min} AND {self.group.orders_count_max}
            )
        """

    def get_filter_by_crm_url(self):
        """Возвращает фильтр по crm_url. """

        if not issubclass(self.model, User):
            return ''

        return f"""
            (
                {self.user_table}.crm_url IS NULL
                OR coalesce(users_user.crm_url, '') = ''
            )
        """

    def get_filter_by_user_id(self):
        """Возвращает фильтр для кандитадов по user_id. """
        if issubclass(self.model, PotentialSupplier):
            return f"""
                {self.user_table}.user_id IS NULL
            """

        return ''

    def get_filter_by_stop_word_in_company_name(self):
        """Убираем пользователей, у которых название компании
        не подходят под настройки группы. """

        stop_words = self.group.stop_words_for_name

        if not stop_words:
            return ''

        # TODO
        list_stop_words = self.group.stop_words_for_name.split(',')
        clause = '\'({0})\''.format('|'.join(list_stop_words))

        return f"""
            NOT (
                {self.user_table}.company_name IS NOT NULL
                AND {self.user_table}.company_name ~* {clause}
            )
        """

    def get_filter_by_supplier_caller(self):
        return """
            supplier_leads.caller_id IS NULL
        """

    def get_filter_by_supplier_status(self):
        """Исключаем поставщиков, у которых статус недозвон,
        отказ или нерабочий лид. """

        # Добавляем rand_hour, чтобы лиды с недозвонами не выпадали подряд
        rand_hour = random.randint(0, 4)

        return f"""
        NOT (
            (
                -- Исключаем лидов с недозвоном
                supplier_leads.status = {SupplierStatusEnum.HAD_EMPTY_CALL}
                AND supplier_leads.updated_at >=
                    CURRENT_TIMESTAMP - INTERVAL
                        '{self.group.hours_pass_lead_emptycall + rand_hour}' HOUR
            ) OR
            (
                -- Исключаем нерабочих лидов
                supplier_leads.status =
                    {SupplierStatusEnum.SET_AS_NOT_WORK_LEAD}
                AND supplier_leads.updated_at >=
                    CURRENT_TIMESTAMP - INTERVAL
                        '{settings.DAYS_NOT_FOR_CALL_AFTER_NOT_WORK_LEAD}' DAY
            ) OR
            (
                -- Исключаем лидов с отказом и обработан
                supplier_leads.status IN (
                    {SupplierStatusEnum.DENIED}, {SupplierStatusEnum.PROCESSED}
                ) AND supplier_leads.updated_at >=
                    CURRENT_TIMESTAMP - INTERVAL '{self.group.period}' DAY
            ) OR
            (
                -- Исключаем лидов со статусом "Заказы не подходят"
                supplier_leads.status =
                    {SupplierStatusEnum.SET_ORDERS_NOT_SUIT}
                AND supplier_leads.updated_at >=
                    CURRENT_TIMESTAMP - INTERVAL
                        '{self.group.hours_pass_lead_notorder}' HOUR
            ) OR
            (
                -- Исключаем только что выпавших лидов | у которых изменён email
                -- | лидов с доменами почты как у платных или бывших платных
                supplier_leads.status IN
                    ({str_join(', ', SupplierStatusEnum.EXCLUDE_STATUSES_FOR_SELECTOR)})
            )
        )
        """

    def get_filter_by_user_phone_and_site(self):
        """Убираем пользователей у которых нет телефона. """

        # У кандидатов телефон обязательное поле
        if not issubclass(self.model, User):
            return ''

        return f"""
            NOT (
                (
                    users_user.phone IS NULL
                    OR (
                        users_user.phone IS NOT NULL
                        AND coalesce(users_user.phone, '') = ''
                    )
                )
                AND (
                    users_companyprofile.site IS NULL
                    OR (
                        users_companyprofile.site IS NOT NULL
                        AND coalesce(users_companyprofile.site, '') = ''
                    )
                )
            )
        """

    def get_filter_by_created_offers(self):
        """Убираем пользователей, которые оставляли недавно предложение. """
        if not issubclass(self.model, User):
            return ''

        return """
            with_users_has_offers.id IS NULL
        """

    def get_filter_by_tariff_statuses(self):
        """Фильтруем пользователей у которых есть подписка более 7 дней. """
        if not issubclass(self.model, User):
            return ''

        return f"""
            with_users_has_subscriptions.user_id IS NULL
        """

    def get_filter_by_asl(self):
        """Убираем пользователей, с которыми недавно разговаривали продажи. """
        if not issubclass(self.model, User):
            return ''

        return f"""
            with_users_has_asl.user_id IS NULL
        """

    def get_filter_by_asl_duration(self):
        """Убираем пользователей которые являются лидами продаж
        (которым за последние 4 недели звонили 3 минуты и более).
        """

        if not issubclass(self.model, User):
            return ''

        return f"""
            with_users_has_asl_duration.user_id IS NULL
        """

    def get_filter_by_sales_planned_calls(self):
        """Убираем пользователей, у которых есть перезвоны отдела продаж. """
        if not issubclass(self.model, User):
            return ''

        return f"""
            with_users_has_planned_call.user_id IS NULL
        """


"""Временные цункции для тестирования выборки. """
# imports
import random
from functools import reduce
from operator import and_
from django.db import transaction
from funcy import partition, first, pluck, str_join
from project.apps.suppliers_main.helpers.sales_status import *
from project.apps.suppliers_main.utils import *
from project.core.timezones import *
from project.core.db import *
from project.apps.suppliers_main.helpers.selectors.user_and_candidate import *
from project.apps.suppliers_main.helpers.selectors.raw_user_and_candidate_selector import *
from project.apps.suppliers_main.helpers.selectors.raw_sql_base import *
from project.apps.suppliers_main.helpers.selectors.raw_orders import *
from project.apps.suppliers_main.helpers import delete_users_from_hot_leads


def test_selectors():
    model = User
    group = SupplierSettingsGroup.objects.get(id=1)

    all_instance_ids = UserAndCandidateSelector(
        model=model,
        group=group
    ).get_ids()

    old_result = []
    for id_part in partition(10000, all_instance_ids):
        old_res = UserRawSelector(
            group=group,
            instance_ids=id_part
        ).get()

        old_result.extend([it['id'] for it in old_res])

    print('old: ', len(set(old_result)))

    new_res = SupplierSelector(model, group).get()
    new_result = set([it['id'] for it in new_res])
    print('new: ', len(new_result))

    print(f'diff: ', len(set(old_result) - set(new_result)))
    print(f'intersect: ', len(set(old_result) & set(new_result)))


def test_old(group_id=1):
    t1 = timezone.now()
    with transaction.atomic():
        sent_count, len_ids = send_users_to_hot_leads(group_id)
        deleted_count = delete_users_from_hot_leads(group_id)
        print(f'sent: {sent_count}, selected: {len_ids}, deleted: {deleted_count}')
    t2 = timezone.now()
    print((t2 - t1).total_seconds())


def test_new(group_id=1):
    t1 = timezone.now()
    with transaction.atomic():
        selected, created, deleted = update_users_hot_leads_table(group_id)
        print(f'selected: {selected}, created: {created}, deleted: {deleted}')
    t2 = timezone.now()
    print((t2 - t1).total_seconds())


def search_user_hot_proposals(proposal_ids: list,
                              user_id: int,
                              size=10000,
                              **kwargs):
    """Выдает популярные товары конкретного поставщика. """
    body = {
        "size": size,
        "sort": [{"_meta.view_count": {"order": "desc"}}],
        "query": {
            "bool": {
                "filter": list(filter(
                    lambda x: bool(x),
                    [
                        {
                            "term": {"user_id": user_id}
                        },
                        {
                            "terms": {"id": proposal_ids}
                        }
                    ]
                ))
            }
        }
    }

    es_client = EsConnector.get_connection()
    result = es_client.transport.perform_request(
        method='GET',
        url=f'/{EsProposalContainer().index}/_search/',
        body=body,
    )

    return {
        'hits': [extract_document(doc) for doc in result['hits']['hits']],
        'total_count': result['hits']['total']['value']
    }


отвалился смонтированный по сети каталог с сертификатами почему то
Примонтировал через команду `sudo mount -t glusterfs monitor.private:/share /mnt/share`, перезапустил контейнер с nginx и поднялся песок


dt1 = datetime.date(2019, 6, 24)
dt2 = datetime.date(2019, 7, 1)
Callback.objects.filter(description='Форма загрузки прайс-листа', created_at__range=(dt1, dt2)).count()


def selectel_():
    from selectel.storage import Container
    container = Container('21069_static', 'dUgIyBOjjT', "static")
    print(container.list('/reports/email_url_click'))
