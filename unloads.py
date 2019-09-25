class BaseUnload:
    host = 'https://supl.biz'

    @property
    def header(self):
        return [
            'Ссылка',
            'Дата регистрации',
            'Эффективность'
        ]

    def unload(self, filename):
        data = self.get_data()
        file_name = self.get_file_name(filename)

        with dropbox_unloader(file_name=file_name) as unloader:
            unloader.write_header(self.header)
            unloader.write_data(data)
            unloader.upload_to_dropbox(dropbox_dir='/Разовые_выгрузки/')

    def get_data(self) -> list:
        res = []

        uu = User.objects.filter(
            creator__is_staff=False,
            date_joined__gt=timezone.now() - timezone.timedelta(days=7),
            sales_lead__caller__isnull=True
        )

        for u in uu:
            if hasattr(u, 'sales_lead'):
                sp = u.sales_lead.sales_productivity
            else:
                sp = ''

            res.append([
                f'{self.host}/profiles/{u.id}/admin/',
                u.date_joined,
                sp
            ])

        return res

    def get_file_name(self, title):
        """Возвращает имя файла для выгрузки. """
        return f'{title}'


# Офферы с метками
def unload(file):
    import dropbox, os, csv, datetime, json
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'utm',
    ])

    offers = Offer.objects.filter(
        created_at__gte=datetime.date(2019, 2, 4),
        utm_marks__icontains='proda'
    ).select_related('user')

    for offer in offers:
        w.writerow([
            host + offer.user.get_absolute_url(),
            json.loads(offer.utm_marks)
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)

# Поставщики, которым звонили и их телефоны
def unload(file):
    import dropbox, os, csv, datetime, json
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'utm',
    ])

    logs = SupplierLog.objects.filter(
        created_at__range=(datetime.date(2019, 2, 7), datetime.date(2019, 2, 8))
    ).distinct('lead').select_related('lead')

    for log in logs:
        if log.lead.user_candidate:
            url = ''
            phone = log.lead.user_candidate.phone
            old_phones = ''
        elif log.lead.user:
            url = f'{host}/profiles/{log.lead.user_id}/admin/'
            phone = log.lead.user.phone
            old_phones = log.lead.user.olden_phones

        else:
            continue
        w.writerow([
            url,
            phone,
            old_phones
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)

def unload(file):
    # Пользователи с utm-метками
    import dropbox, os, csv, datetime, json
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'utm',
    ])

    from_ = datetime.date(2019, 2, 1)
    to_ = datetime.date(2019, 2, 16)

    users = User.objects.filter(
        Q(utm_marks__contains='gdeprodat') |
        Q(utm_marks__contains='dlyapostavshikovall')
    ).filter(
        date_joined__range=(from_, to_)
    )

    for user in users:
        w.writerow([
            f'{host}/profiles/{user.id}/admin/',
            user.utm_marks,
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    # Заказы, размещенные с 22 до 10 по Томскому времени
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.orders.models import Order
    from project.utils.timezones import convert_dt_to_local

    tsk_tz_str = 'Asia/Tomsk'
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'статус',
        'дата',
        'время',
        'день недели',
    ])

    weekday_map = {
        1: 'Понедельник',
        2: 'Вторник',
        3: 'Среда',
        4: 'Четверг',
        5: 'Пятница',
        6: 'Суббота',
        7: 'Воскресенье',
    }

    sixteen_weeks_ago = timezone.now() - timezone.timedelta(weeks=16)

    orders = Order.objects.filter(
        created_at__gt=sixteen_weeks_ago,
        created_at__week_day__in=[6, 7, 1]
    )

    for order in orders:
        created_at_tsk = convert_dt_to_local(order.created_at, tsk_tz_str)
        hour = created_at_tsk.hour

        if 22 <= hour < 24 or 0 <= hour < 10:

            w.writerow([
                f'{host}/orders/{order.id}/admin/',
                order.get_status_display(),
                created_at_tsk.strftime('%d.%m.%Y'),
                created_at_tsk.strftime('%H:%M'),
                weekday_map[created_at_tsk.isoweekday()]
            ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    # ANNA
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.users.models import User
    from project.apps.users.helpers.comments import UserCommentAggregator, CommentTypesEnum
    from funcy import first, second

    tsk_tz_str = 'Asia/Tomsk'
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
    ])

    users = User.objects.filter(comments__user_id=1127419)

    for user in users[:1000]:
        comments = UserCommentAggregator(
            user=user,
            for_admin=True
        ).get_comments()
        comments = list(reversed(comments))

        if len(comments) < 2:
            continue

        f_c = first(comments)
        s_c = second(comments)

        if (
                f_c['user']['id'] == 1127419 and
                f_c['type_description'] == 'Комментарий к профилю.' and
                s_c['type_description'] == 'Комментарий поставщиков.'):

            w.writerow([
                f'{host}/profiles/{user.id}/admin/',
            ])

            # print(f_c['created_at'], f_c['user']['id'], f_c['type_description'])
            # print(s_c['created_at'], s_c['user']['id'], s_c['type_description'])
            # print('\n')

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


# Перенесенные
def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.users.models import User
    from project.apps.users.helpers.comments import UserCommentAggregator, CommentTypesEnum
    from funcy import first, second

    tsk_tz_str = 'Asia/Tomsk'
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'продавец',
        'эффективность',
        'source'
    ])

    se = SalesEvent.objects.filter(
        type_info__has_key='was_copied_after_zero_call'
    )

    leads = SalesLead.objects.filter(id__in=se.values_list('lead'))

    for lead in leads:
        w.writerow([
            f'{host}/profiles/{lead.user_id}/admin/',
            lead.caller.name if lead.caller else '',
            lead.sales_productivity,
            lead.user.get_source_display()
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.users.models import User
    from project.apps.users.helpers.comments import UserCommentAggregator, CommentTypesEnum
    from funcy import first, second

    tsk_tz_str = 'Asia/Tomsk'
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
    ])

    rst = Region.objects.get(title='Ростов-на-Дону')
    users = User.objects.filter(
        origin=rst,
        sales_lead__caller__isnull=True,
        sales_lead__salesevents__type__in=[
            SalesEventTypesEnum.VIEW_ORDER,
            SalesEventTypesEnum.OFFER_FROM_KNOWN_SOURCES,
            SalesEventTypesEnum.OFFER_FROM_UNKNOWN_SOURCES
        ]
    ).exclude(
        rubrics__in=Rubric.objects.filter(title__in=['Строительные и отделочные материалы', 'Строительство. Недвижимость. Ремонт'])
    ).distinct()

    for user in users:
        w.writerow([
            f'{host}/profiles/{user.id}/admin/',
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)




def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.backend_sales.models import UserGroupMembership, SalesEventHistory, SalesEventTypesEnum
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)
    w.writerow([
        'ссылка',
        'seller',
        'planned_datetime'
    ])

    account = UserGroupMembership.objects.filter(
        group_id=6,
        is_active=True
    )

    seh = SalesEventHistory.objects.filter(
        type=SalesEventTypesEnum.PLANNED_CALL,
        action_on_lead__isnull=True,
        lead__caller__in=account.values_list('user', flat=True),
        created_at__gt=datetime.date(2018, 11, 1)
    )

    for s in seh:
        w.writerow([
            f'{host}/profiles/{s.lead.user_id}/admin/',
            s.lead.caller.name if s.lead.caller else '',
            s.planned_datetime
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.orders.models import Order
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    subs = Subscription.objects.exclude_demo().filter(
        payment_date__year=2019,
        payment_date__month=2
    )

    subs.count()

    orders = Order.objects.filter(
        offers__user__in=subs.values_list('user', flat=True)
    ).distinct()

    for order in orders:
        w.writerow([
            f'{host}/orders/{order.id}/admin/',
            order.description,
            ', '.join(list(order.rubrics.values_list('title', flat=True))),
            ', '.join(list(order.regions.values_list('title', flat=True)))
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    dt_from = datetime.date(2019, 1, 1)
    dt_to = datetime.date(2019, 4, 15)

    subs = Subscription.objects.exclude_demo().filter(
        finish_date__range=(dt_from, dt_to)
    )

    for s in subs:
        pc = SalesEvent.objects.filter(
            type=SalesEventTypesEnum.PLANNED_CALL,
            lead__user_id=s.user_id
        ).first()

        sales_lead = getattr(s.user, 'sales_lead') if hasattr(s.user, 'sales_lead') else None
        if sales_lead:
            caller = sales_lead.caller.name if sales_lead.caller else ''
        else:
            caller = ''
        w.writerow([
            f'{host}/profiles/{s.user_id}/admin/',
            caller,
            s.finish_date.strftime('%d.%m.%Y'),
            pc.planned_datetime.strftime('%d.%m.%Y') if pc else ''
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


    def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    templates = Template.objects.all()

    for t in templates:
        w.writerow([
            t.id,
            t.type,
            t.email_a_subj_ru,
            t.email_a_text_ru,
            t.email_a_html_ru,
            t.email_b_subj_ru,
            t.email_b_text_ru,
            t.email_b_html_ru,
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)




def add_subscriptions():
    import csv
    import datetime
    from django.core.cache import cache
    from django.db import transaction

    file_name = '/tmp/subscriptions.csv'
    f = open(file_name, 'r', encoding='utf-8')
    file = csv.reader(f)
    next(file)

    subscriptions_modified = set()
    subscriptions_added = set()

    with transaction.atomic():
        for row in file:
            title, actual_payment, payment_date, start_date, finish_date, user_id = row
            payment_date = datetime.datetime.strptime(payment_date, '%d.%m.%Y')
            start_date = datetime.datetime.strptime(start_date, '%d.%m.%Y')
            finish_date = datetime.datetime.strptime(finish_date, '%d.%m.%Y')
            user_id = int(user_id)

            if title == 'Премиум' or title == 'премиум':
                title = 'Поставщик Премиум'
            if title == 'Стандарт':
                title = 'Поставщик Стандарт'

            if title == 'Закупщик':
                continue

            tariff = Tariff.objects.filter(title=title).first()

            try:
                actual_payment = float(actual_payment)
            except ValueError:
                actual_payment = actual_payment.replace(',', '.')
                actual_payment = float(actual_payment)

            subscription = Subscription.objects.filter(
                user_id=user_id,
                start_date=start_date,
                finish_date=finish_date
            ).first()

            if subscription:
                data = {}
                if not subscription.actual_payment:
                    data.update({'actual_payment': actual_payment})
                if not subscription.payment_date:
                    data.update({'payment_date': payment_date})

                if data:
                    subscriptions_modified.add(subscription.id)
                    Subscription.objects.filter(id=subscription.id).update(**data)

            else:
                new_sub = Subscription.objects.create(
                    user_id=user_id,
                    start_date=start_date,
                    finish_date=finish_date,
                    payment_date=payment_date,
                    actual_payment=actual_payment,
                    tariff=tariff
                )

                subscriptions_added.add(new_sub.id)

    cache.set('subscriptions:modified', subscriptions_modified, 60 * 60 * 48)
    cache.set('subscriptions:added', subscriptions_added, 60 * 60 * 48)


order_ct = ContentType.objects.get_for_model(Order)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.orders.models import Order
    from tqdm import tqdm
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    from dateutil.relativedelta import relativedelta
    date_from = datetime.datetime(2015, 1, 1).replace(tzinfo=timezone.utc)

    date_ranges = []
    for i in range(16):
        from_ = date_from
        to_ = date_from + relativedelta(months=3)
        date_from = to_

        date_ranges.append((from_, to_))

    subscriptions = Subscription.objects.exclude_demo().order_by(
        'payment_date'
    ).select_related('user')

    for s in tqdm(subscriptions[4000:]):
        data = [
            s.user_id,
            s.actual_payment,
            s.payment_date
        ]
        for dt_range in date_ranges:
            try:
                count_for_quarter = get_instances_count(s, dt_range)
                data.append(count_for_quarter)
            except Exception as e:
                pass

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def get_instances_count(subscription, dt_range):
    user = subscription.user
    orders_in_quarter = Order.objects.filter(created_at__range=dt_range)

    past_sub = Subscription.objects.exclude_demo().filter(
        user=user,
        payment_date__lt=subscription.payment_date
    ).order_by('-payment_date').first()

    if past_sub:
        dt_filter = {
            'created_at__range': (
                past_sub.payment_date, subscription.payment_date
            )
        }
    else:
        dt_filter = {'created_at__lt': subscription.payment_date}

    offers = Offer.objects.filter(
        user=user,
        order__in=orders_in_quarter,
    ).filter(**dt_filter)

    order_ids = (
        set(list(orders_in_quarter.values_list('id', flat=True))) -
        set(list(offers.values_list('order', flat=True)))
    )
    viewed_orders = Click.objects.filter(
        user=user,
        content_type=order_ct,
        object_id__in=order_ids
    ).filter(**dt_filter).order_by('object_id').distinct('object_id')

    ofc = offers.count()
    voc = viewed_orders.count()
    return ofc + voc


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    w.writerow([
        'Ссылка',
        'Название компании',
        'email',
        'Кол-во',
        'Дата размещения последнего заказа/предложения',
    ])

    users = User.objects.annotate(
        orders_count=Count('orders')
    ).filter(orders_count__gt=0).select_related('orders')
    print(users.count())

    for u in users:
        latest_order = u.orders.order_by('-actualized_at').first()
        w.writerow([
            f'{host}/profiles/{u.id}/admin/',
            u.company_name,
            u.email,
            u.orders_count,
            latest_order.actualized_at if latest_order else '',
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    w.writerow([
        'Ссылка',
        'Название компании',
        'email',
        'Кол-во',
        'Дата размещения последнего заказа/предложения',
    ])

    users = User.objects.annotate(
        offers_count=Count('offers')
    ).filter(offers_count__gt=0)
    print(users.count())

    for u in users:
        latest_offer = u.offers.order_by('-created_at').first()
        w.writerow([
            f'{host}/profiles/{u.id}/admin/',
            u.company_name,
            u.email,
            u.offers_count,
            latest_offer.created_at if latest_offer else '',
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.orders.models import Order
    from tqdm import tqdm
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    sellers = UserGroupMembership.objects.filter(is_active=True)
    sellers = User.objects.filter(
        id__in=sellers.values_list('user', flat=True).distinct()
    ).distinct().order_by('name')

    for seller in sellers:

        dates = (
            datetime.date(2018, 11, 1),
            datetime.date(2018, 12, 1),
            datetime.date(2019, 1, 1),
            datetime.date(2019, 2, 1),
            datetime.date(2019, 3, 1),
        )

        data = [
            seller.name,
        ]

        for date in dates:
            data.extend(calc_by_date(date, seller))

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def calc_by_date(date, caller):
    unprocessed_leads = SalesStatusLog.objects.filter(
        caller=caller,
        status=SalesStatusTypesEnum.RAW,
        created_at__year=date.year,
        created_at__month=date.month
    ).distinct('lead')

    users = User.objects.filter(
        id__in=unprocessed_leads.values_list('lead__user')
    )
    sump_orders = sum(users.values_list('sales_orders_count_full', flat=True))
    try:
        avg_orders_count = round(sump_orders / users.count())
    except ZeroDivisionError:
        avg_orders_count = 0

    leads_with_50_orders = users.filter(sales_orders_count_full__gt=50)

    return unprocessed_leads.count(), avg_orders_count, leads_with_50_orders.count()


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    local_tz = timezone.pytz.timezone('Asia/Tomsk')
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    from_ = datetime.date(2019, 4, 1)
    to_ = datetime.date(2019, 4, 6)
    logs = LeadActionLog.objects.filter(
        created_at__range=(from_, to_),
        lead__crm_type=CRMTypeEnum.REPEATABLE_ORDERS
    ).order_by('lead', 'created_at').distinct('lead').select_related('lead')

    for log in logs:
        order = Order.objects.filter(
            user=log.lead.user,
            actualized_at__lt=log.created_at
        ).first()
        w.writerow([
            timezone.localtime(log.created_at, timezone=local_tz).strftime('%d-%m-%Y %H:%M'),
            f'{host}/profiles/{log.lead.user_id}/admin/',
            log.get_verb_display(),
            log.author.name,
            (timezone.localtime(order.actualized_at, timezone=local_tz).strftime('%d-%m-%Y %H:%M')) if order else ''
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file, level):
    """Рубрики N уровня. """
    import dropbox, os, csv
    from django.utils import timezone
    from project.apps.sales.models import SalesLead
    from project.apps.users.models import User
    from django.db.models import Sum
    from collections import defaultdict, OrderedDict

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    rubrics = Rubric.objects.filter(level=level).order_by('title')

    for r in rubrics:
        w.writerow([
            r.title
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox, os, csv
    from django.utils import timezone
    from project.apps.sales.models import SalesLead
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from django.db.models import Sum
    from collections import defaultdict, OrderedDict

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    year_ago = timezone.now() - timezone.timedelta(days=365)
    leads = SalesLead.objects.all()
    data = defaultdict(int)
    for lead in leads:
        sum_duration = lead.actiononsalesleads.filter(
            processed_at__gt=year_ago
        ).aggregate(sum_duration=Sum('duration'))['sum_duration'] or 0

        data[lead.user_id] = sum_duration

    grouped_data = defaultdict(list)
    for k, v in data.items():
        grouped_data[v].append(k)

    ordered_data = OrderedDict(sorted(grouped_data.items()))

    for k, v in ordered_data.items():
        user_has_subs_ids = Subscription.objects.exclude_demo().filter(
            user__in=v,
            finish_date__gt=year_ago,
        ).values_list('user')
        w.writerow([
            k,
            len(set(v)),
            len(set(list(user_has_subs_ids))),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    """Количество пользователей по КОЛИЧЕСТВУ ЗВОНКОВ. """
    import dropbox, os, csv
    from django.utils import timezone
    from project.apps.sales.models import SalesLead
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from django.db.models import Sum, Count
    from collections import defaultdict, OrderedDict

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    year_ago = timezone.now() - timezone.timedelta(days=365)
    leads = SalesLead.objects.all()
    data = defaultdict(int)
    for lead in leads:
        calls_count = lead.actiononsalesleads.filter(
            duration__gt=1,
            processed_at__gt=year_ago
        ).count()

        data[lead.user_id] = calls_count

    grouped_data = defaultdict(list)
    for k, v in data.items():
        grouped_data[v].append(k)

    ordered_data = OrderedDict(sorted(grouped_data.items()))

    for k, v in ordered_data.items():
        user_has_subs_ids = Subscription.objects.exclude_demo().filter(
            user__in=v
        ).values_list('user', flat=True)
        w.writerow([
            k,
            len(set(v)),
            len(set(list(user_has_subs_ids))),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox, os, csv
    from django.utils import timezone
    from project.apps.sales.models import SalesLead
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from django.db.models import Sum, Count
    from collections import defaultdict, OrderedDict

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    year_ago = timezone.now() - timezone.timedelta(days=365)
    leads = SalesLead.objects.all()
    data = defaultdict(int)
    for lead in leads:
        calls_count = lead.actiononsalesleads.filter(
            duration__gte=1,
            processed_at__gt=year_ago
        ).count()

        data[lead.user_id] = calls_count

    grouped_data = defaultdict(list)
    for k, v in data.items():
        grouped_data[v].append(k)

    ordered_data = OrderedDict(sorted(grouped_data.items()))

    for k, v in ordered_data.items():
        user_has_subs_ids = Subscription.objects.exclude_demo().filter(
            user__in=v
        ).values_list('user', flat=True)
        w.writerow([
            k,
            len(set(v)),
            len(set(list(user_has_subs_ids))),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox, os, csv
    from django.utils import timezone
    from project.apps.sales.models import SalesLead, ActionOnSalesLead, SalesEventTypesEnum
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from django.db.models import Sum, Count
    from collections import defaultdict, OrderedDict
    from project.apps.users.helpers.comments import UserCommentAggregator

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    leads = SalesLead.objects.filter(actiononsalesleads__duration__gt=0).distinct()
    year_ago = timezone.now() - timezone.timedelta(days=365)
    for lead in leads:
        calls_count = lead.actiononsalesleads.filter(
            duration__gte=1,
            processed_at__gt=year_ago
        ).count()

        se = lead.salesevents.filter(
            type=SalesEventTypesEnum.PLANNED_CALL
        ).first()

        last_comment = lead.actiononsalesleads.order_by('-processed_at').first()

        if calls_count >= 10:

            w.writerow([
                f'https://supl.biz/profiles/{lead.user_id}/admin/',
                lead.caller.name if lead.caller else '',
                se.planned_datetime if se else '',
                last_comment.comment if last_comment else ''
            ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def get_all_invalid_instances():
    """Скрипт чистит метки у пользователей и заказов.
    Удаляет из строк quotation_marks и затем сохраняет.

    Фиксирует пользователей/заказы, у которых невалидные метки.
    """

    ru_char_codes_regex = r'[\u0400-\u04FF]'
    users = User.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')

    users_invalid_marks = set()
    orders_invalid_marks = set()
    offers_invalid_marks = set()
    for u in users:
        try:
            json.loads(u.utm_marks)
        except json.JSONDecodeError:
            users_invalid_marks.add(u.id)

    orders = Order.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')
    for order in orders:
        try:
            json.loads(order.utm_marks)
        except json.JSONDecodeError:
            orders_invalid_marks.add(order.id)

    offers = Offer.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')
    for offer in offers:
        try:
            json.loads(offer.utm_marks)
        except json.JSONDecodeError:
            offers_invalid_marks.add(offer.id)

    return users_invalid_marks, orders_invalid_marks, offers_invalid_marks


def get_all_valid_instances():
    """Скрипт чистит метки у пользователей и заказов.
    Удаляет из строк quotation_marks и затем сохраняет.

    Фиксирует пользователей/заказы, у которых невалидные метки.
    """

    ru_char_codes_regex = r'[\u0400-\u04FF]'
    users = User.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')

    users_invalid_marks = set()
    orders_invalid_marks = set()
    offers_invalid_marks = set()
    for u in users[:1000]:
        try:
            json.loads(u.utm_marks)
            users_invalid_marks.add(u.id)
        except json.JSONDecodeError:
            pass

    orders = Order.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')
    for order in orders[:1000]:
        try:
            json.loads(order.utm_marks)
            orders_invalid_marks.add(order.id)
        except json.JSONDecodeError:
            pass

    offers = Offer.objects.filter(
        utm_marks__iregex=ru_char_codes_regex).only('id', 'utm_marks')
    for offer in offers[:1000]:
        try:
            json.loads(offer.utm_marks)
            offers_invalid_marks.add(offer.id)
        except json.JSONDecodeError:
            pass

    return users_invalid_marks, orders_invalid_marks, offers_invalid_marks



def unload(file):
    import dropbox, os, csv

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    uu, _or, _of = get_all_invalid_instances()

    users = User.objects.filter(id__in=uu)
    orders = Order.objects.filter(id__in=_or)
    offers = Offer.objects.filter(id__in=_of)

    w.writerow(['Юзеры'])
    for u in users:
        new_utm = jsonify_utm_marks(u, u.utm_marks)
        w.writerow([
            u.id,
            u.utm_marks,
            new_utm
        ])

    w.writerow(['Заказы'])
    for order in orders:
        new_utm = jsonify_utm_marks(order, order.utm_marks)
        w.writerow([
            order.id,
            order.utm_marks,
            new_utm
        ])

    w.writerow(['Предложения'])
    for offer in offers:
        new_utm = jsonify_utm_marks(offer, offer.utm_marks)
        w.writerow([
            offer.id,
            offer.utm_marks,
            new_utm
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)

def ff():
    from project.apps.suppliers_main.models import HotLead, \
        SupplierLogStatusEnum, SupplierLead
    from collections import defaultdict
    from django.db.models import Q
    hl = HotLead.objects.filter(group_id=1)

    choices_dict = dict(SupplierLogStatusEnum.CHOICES)
    data = defaultdict(int)

    leads = SupplierLead.objects.filter(
        Q(user__in=hl.values_list('user')) |
        Q(user_candidate__in=hl.values_list('candidate'))
    )
    for l in leads:
        last_log = l.logs.order_by('-created_at').first()
        if last_log:
            data[choices_dict[last_log.status]] += 1
        else:
            data['Не звонили'] += 1

    return dict(data)


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox, os, csv
    from funcy import pluck_attr
    from project.apps.users.models import User
    from project.apps.orders.helpers.interesting_orders import (
        InterestingOrdersSelector,
    )

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    users = User.objects.filter(
        rubrics__isnull=False,
        regions__isnull=False,
        sales_orders_count_full__gte=15,
    ).order_by('?').distinct()

    for user in users[:10000]:
        rus = Region.objects.get(slug='russia')

        user_regions = UserRegion.objects.filter(
            user=user,
        ).select_related('region')

        user_rubrics = UserRubric.objects.filter(
            user=user,
        ).select_related('rubric')

        selector = InterestingOrdersSelector(
            rubrics=list(pluck_attr('rubric', user_rubrics)),
            regions=list(pluck_attr('region', user_regions)),
        )

        orders_full = selector.get_orders_with_rubrics_families()
        orders_full_count = orders_full.count()
        orders_full_minus_count = orders_full.exclude(supply_city=rus).count()

        w.writerow([
            f'https://supl.biz/profiles/{user.id}/admin/',
            orders_full_count,
            orders_full_minus_count
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox, os, csv
    from project.apps.users.models import User


    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    ss = Subscription.objects.active()
    users = User.objects.filter(
        id__in=ss.values_list('user'),
        is_staff=False,
        is_superuser=False
    )

    for user in users:
        rubrics = user.rubrics.filter(level=0).values_list('title', flat=True)
        w.writerow([
            user.company_name,
            f'https://admin.supl.biz/profiles/{user.id}/',
            user.origin,
            ', '.join(list(rubrics)) if rubrics else ''
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



# Подтянуть к заказам доп популярные, ссылки заказов из файла
# scp order_urls.csv root@supl.biz:/tmp/order_urls.csv
def extract_digit_from_string(string, start_substr, end_substr):
    """Извлекает число из подстрок. """
    import re

    digit = re.search(
        r'{0}(\d+){1}'.format(start_substr, end_substr), string
    ).group(1)

    return digit


def unload(file):
    """Количество пользователей по минутам разговоров. """
    import dropbox
    import os
    import csv

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')

    input_fn = '/tmp/order_urls.csv'
    in_f = open(input_fn, 'r', encoding='utf-8')
    in_file_reader = csv.reader(in_f)

    output_fn = '/tmp/{}.csv'.format(file)
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    for row in in_file_reader:
        order_id = extract_digit_from_string(row[0], 'orders/', '/')
        order = Order.objects.get(id=int(order_id))

        our_writer.writerow([
            row[0],
            row[1],
            order.description,
            ', '.join(order.rubrics.values_list('title', flat=True)),
            ', '.join(order.regions.values_list('title', flat=True)),
            order.get_source_display(),
            order.user.get_type_display(),
            order.supply_city.title if order.supply_city else ''
        ])

    in_f.close()
    out_f.close()

    f = open(output_fn, 'rb')
    client.files_upload(f.read(), output_fn.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(output_fn)



def unload(file):
    """Оставили предложения - купили тариф. """
    import dropbox
    import os
    import csv
    from django.utils import timezone
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from project.apps.suppliers_main.models import (
        SupplierSettingsGroupMembership, SupplierPaidOffer,
    )

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')

    output_fn = '/tmp/{}.csv'.format(file)
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    callers = SupplierSettingsGroupMembership.objects.all()
    users = User.objects.filter(id__in=callers.values_list('user')).order_by('name')
    months = (
        (9, 2018, 'Сентябрь'),
        (10, 2018, 'Октябрь'),
        (11, 2018, 'Ноябрь'),
        (12, 2018, 'Декабрь'),
        (1, 2019, 'Январь'),
        (2, 2019, 'Февраль'),
        (3, 2019, 'Март'),
        (4, 2019, 'Апрель'),
        (5, 2019, 'Май'),
    )

    our_writer.writerow([
        'Месяц',
        'Манагер',
        'Сколько засчиталось',
        'Сколько купили тариф',
        'Пользователи'
    ])

    paid_users = []

    for month, year, month_name in months:
        for user in users:
            manager = user.name

            offers = Offer.objects.filter(
                supplierpaidoffer__isnull=False,
                created_at__year=year,
                created_at__month=month
            )
            paid_offers = SupplierPaidOffer.objects.filter(
                log__creator_id=user.id
            )

            all_user_offers = offers.filter(
                id__in=paid_offers.values_list('offer_id', flat=True)
            )

            on_tariff = []

            for offer in all_user_offers:
                sub = Subscription.objects.exclude_demo().filter(
                    user=offer.user,
                    start_date__gte=offer.created_at,
                )

                if sub.exists():
                    on_tariff.append(offer.user)
                    paid_users.append(offer.user)

            our_writer.writerow([
                month_name,
                manager,
                all_user_offers.count(),
                len(on_tariff)
            ])

            for u in on_tariff:
                our_writer.writerow([
                    '',
                    '',
                    '',
                    '',
                    f'https://admin.supl.biz/profiles/{u.id}'
                ])

    out_f.close()

    f = open(output_fn, 'rb')
    client.files_upload(f.read(), output_fn.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(output_fn)


def unload(file):
    """Предложений - повторных предложений """
    import dropbox
    import os
    import csv
    from django.utils import timezone

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')

    output_fn = '/tmp/{}.csv'.format(file)
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    months = (
        (1, 'Январь'),
        (2, 'Февраль'),
        (3, 'Март'),
    )

    our_writer.writerow([
        'Месяц',
        'Предложений',
        'Повторных',
    ])

    for month, month_name in months:
        offers = Offer.objects.filter(
            created_at__year=2019,
            created_at__month=month
        ).order_by(
            'user',
            '-created_at',
        ).distinct(
            'user',
        )

        repeated_offers_users = []
        for offer in offers:
            off = Offer.objects.filter(
                user=offer.user,
                created_at__gt=offer.created_at,
                created_at__lt=offer.created_at + timezone.timedelta(days=90)
            ).values_list('user', flat=True)
            repeated_offers_users.extend(list(off))

        our_writer.writerow([
            month_name,
            offers.count(),
            len(set(repeated_offers_users))
        ])

    out_f.close()

    f = open(output_fn, 'rb')
    client.files_upload(f.read(), output_fn.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(output_fn)


def unload(file):
    """Комменты - Оставили предложения - купили тариф. """
    import dropbox
    import os
    import csv
    from project.apps.users.models import User
    from project.apps.tariffs.models import Subscription
    from project.apps.suppliers_main.models import (
        SupplierSettingsGroupMembership, SupplierPaidOffer,
    )
    from project.apps.sales.models import ActionOnSalesLead

    client = dropbox.Dropbox(
        'qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')

    output_fn = '/tmp/{}.csv'.format(file)
    out_f = open(output_fn, 'w', encoding='utf-8')
    our_writer = csv.writer(out_f)

    callers = SupplierSettingsGroupMembership.objects.all()
    users = User.objects.filter(id__in=callers.values_list('user')).order_by(
        'name')
    months = (
        (9, 2018, 'Сентябрь'),
        (10, 2018, 'Октябрь'),
        (11, 2018, 'Ноябрь'),
        (12, 2018, 'Декабрь'),
        (1, 2019, 'Январь'),
        (2, 2019, 'Февраль'),
        (3, 2019, 'Март'),
        (4, 2019, 'Апрель'),
        (5, 2019, 'Май'),
    )

    paid_users = []

    for month, year, month_name in months:
        for user in users:

            offers = Offer.objects.filter(
                supplierpaidoffer__isnull=False,
                created_at__year=year,
                created_at__month=month
            )
            paid_offers = SupplierPaidOffer.objects.filter(
                log__creator_id=user.id
            )

            all_user_offers = offers.filter(
                id__in=paid_offers.values_list('offer_id', flat=True)
            )

            for offer in all_user_offers:
                sub = Subscription.objects.exclude_demo().filter(
                    user=offer.user,
                    start_date__gte=offer.created_at,
                )

                if sub.exists():
                    paid_users.append(offer.user)

    for user in paid_users:
        asl = ActionOnSalesLead.objects.filter(
            lead__user=user,
        )
        our_writer.writerow([
            f'https://admin.supl.biz/profiles/{user.id}/',
        ])

        for a in asl:
            our_writer.writerow([
                '',
                a.comment,
                a.created_at.strftime('%d.%m.%Y %H:%M')
            ])

    out_f.close()

    f = open(output_fn, 'rb')
    client.files_upload(f.read(),
                        output_fn.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(output_fn)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    from tqdm import tqdm
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://admin,supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    subscriptions = Subscription.objects.exclude_demo().select_related('user')

    for s in tqdm(subscriptions):
        data = [
            s.payment_date,
            s.actual_payment,
            s.seller.name if s.seller else '',
            f'{host}/profiles/{s.user_id}/'
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv, datetime, json
    from django.utils import timezone
    from project.apps.tariffs.models import Subscription
    from tqdm import tqdm
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    u = User.objects.get(id=1299571)
    pp = u.proposals.all()

    views = InstanceView.objects.filter(
        object_id__in=pp.values_list('id'),
        object_type=ContentType.objects.get_for_model(Proposal),
        created_at__date=datetime.date(2019, 6, 21)
    )

    for v in views:
        data = [
            f'{host}/proposals/{v.object_id}/',
            f'https://admin.supl.biz/profiles/{v.user_id}/' if v.user_id else '',
            v.get_source_display()
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


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



def check_rating():
    from collections import OrderedDict

    sql = """
        SELECT U.id, COALESCE(SUM(S.score), 0) as score
        FROM users_user as U
        LEFT JOIN scores_score as S
        ON (S.user_id = U.id)
        GROUP BY U.id
    """

    data = RawSQLManager(sql_query=sql).execute()

    user_scores = {str(i['id']): i['score'] for i in data}
    unique_user_scores = {str(int(v)): v for v in user_scores.values()}

    result = {}
    for k, v in unique_user_scores.items():
        users_count = redis.zrevrangebyscore(SCORES_CACHE_KEY, v, v)
        result.update({
            v: len(users_count)
        })

    return OrderedDict(sorted(result.items()))


def unload(file):
    import dropbox, os, csv
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    data = check_rating()

    w.writerow(['Рейтинг', 'Количество пользователей с таким рейтингом'])
    for k, v in data.items():
        data = [
            k,
            v
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    users = User.objects.filter(rubrics__isnull=False, regions__isnull=False, is_superuser=False, is_staff=False).order_by('id').distinct()

    w.writerow(['Рейтинг', 'Количество пользователей с таким рейтингом'])
    for user in users[300000:]:
        data = [
            f'https://supl.biz/profiles/{user.id}/'
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def unload(file):
    import dropbox, os, csv
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    offers = Offer.objects.filter(
        created_at__gt=timezone.now() - timezone.timedelta(days=365)
    ).values_list('user', flat=True)

    users = User.objects.filter(id__in=offers).distinct()
    print(users.count())

    for user in users:
        data = [
            f'https://supl.biz/profiles/{user.id}/'
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def get_proposal_opened_contacts():
    """Возвращает статистику по открытиям телефона в товарах и каталоге."""

    sql_query = f"""
        SELECT
           clicks.content_object_id,
           Count(content_object_id) AS "count"

        FROM {ClickProposalPhone._meta.db_table} AS clicks

        GROUP BY clicks.content_object_id
        """
    result = RawSQLManager(sql_query=sql_query).execute()
    return result


def get_proposal_opened_contacts():
    """Возвращает статистику по открытиям телефона в товарах и каталоге."""

    sql_query = f"""
        SELECT
           clicks.content_object_id,
           Count(content_object_id) AS "count"

        FROM {ClickProposalPhone._meta.db_table} AS clicks

        GROUP BY clicks.content_object_id
        """
    result = RawSQLManager(sql_query=sql_query).execute()
    return result


def unload(file):
    import dropbox, os, csv
    from itertools import groupby

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    stat = get_proposal_opened_contacts()

    sorted_stat = sorted(stat, key=lambda k: k['count'])

    grouped = groupby(sorted_stat, key=lambda x: x['count'])

    for key, clicks in grouped:
        data = [
            key,

            len(list(clicks))
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def unload_user_comments(user_id):
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



def unload_cc_matrix(file):
    import dropbox, os, csv

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    w.writerow([
        'Рубрика',
        'Регион',
        'Количество поставщиков',
        'Количество платных поставщиков',
        'Коэффициент',
        'Количество заказов'
    ])

    from project.apps.main.matrix import MatrixKey
    from project.apps.cold_callers.models import DeficitCustomersMatrix
    from project.apps.cold_callers.helpers.matrix.matrix_for_suppliers import \
        DeficitCustomersMatrix as HDeficitCustomersMatrix

    matrix = DeficitCustomersMatrix.objects.first()
    helper = HDeficitCustomersMatrix()
    stats = helper.stats_orders_count

    rub_title_map = {}
    reg_title_map = {}

    for json_repr, cell_data in matrix.matrix.items():
        matrix_key = MatrixKey.init_from_json_repr(json_repr)

        rub_title = rub_title_map.get(matrix_key.rubric_id)
        if not rub_title:
            rub_title = Rubric.objects.get(id=matrix_key.rubric_id).title
            rub_title_map.update({matrix_key.rubric_id: rub_title})

        reg_title = reg_title_map.get(matrix_key.region_id)
        if not reg_title:
            reg_title = Region.objects.get(id=matrix_key.region_id).title
            reg_title_map.update({matrix_key.region_id: reg_title})

        data = [
            rub_title,
            reg_title,
            cell_data['count_suppliers'],
            cell_data['count_paid_suppliers'],
            cell_data['customers_deficit_coefficient'],
            stats[matrix_key],
        ]

        w.writerow(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def stats_orders_count(rubric_id, region_id):
    from project.apps.main.matrix import MatrixKey
    from project.apps.cold_callers.helpers.matrix.matrix_for_suppliers import DeficitCustomersMatrix as HDeficitCustomersMatrix

    m = HDeficitCustomersMatrix()
    stats = m.stats_orders_count

    key = MatrixKey(rubric_id=rubric_id, region_id=region_id)

    return stats[key]



def demo_users_sent_offers_in_june():
    import datetime
    from itertools import groupby
    dt_range = (datetime.date(2019, 6, 1), datetime.date(2019, 7, 1))
    offers = Offer.objects.filter(created_at__range=dt_range).order_by('user')
    print(offers.count(), len(set(list(offers.values_list('user', flat=True)))))
    grouped = groupby(offers, key=lambda x: x.user_id)

    counter = 0
    users = []
    for uid, offers in grouped:
        offers_ = list(offers)
        if not User.objects.get(id=uid).on_tariff and len(offers_) >= 20:
            counter += 1
            users.append(uid)

    return counter, users


def cc_events(file):
    import dropbox, os, csv
    from itertools import groupby

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    ll = LeadActionLog.objects.filter(
        created_at__gt=timezone.now() - timezone.timedelta(minutes=100),
        verb=LeadActionLogVerbEnum.ASSIGNED_FIRSTLY, lead__crm_type=0).order_by('-created_at')

    w.writerow([
        'Тип',
        'Коэффициент',
    ])
    for l in ll:
        w.writerow([
            'Кандидат' if l.lead.is_potential_supplier else f'https://admin.supl.biz/profiles/{l.lead.user.id}/',
            l.lead.user.potential_customer_coefficient_for_suppliers,
        ])
    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)


def get_orders_data():
    from tqdm import tqdm
    year_ago = timezone.now() - timezone.timedelta(days=365)
    orders = Order.objects.filter(
        created_at__gt=year_ago,
        clicks__isnull=False,
        clicks__user__subscription__start_date__gt=year_ago,
    ).distinct().order_by('id')

    result = []
    unique_orders = set()

    # /id заказчика/Какой по счету этот заказ у заказчика
    for order in tqdm(orders):

        # Уже добавили
        if order.id in unique_orders:
            continue

        customer = order.user
        views = order.clicks.order_by('created_at')

        for view in views:
            subscriptions = view.user.subscription_set.all()
            for s in subscriptions:

                # Уже добавили
                if order.id in unique_orders:
                    continue

                sub_starts = s.payment_date or s.start_date
                if view.created_at.date() <= sub_starts <= (view.created_at + timezone.timedelta(days=30)).date():
                    # Заказ принес деньги
                    unique_orders.add(order.id)

                    all_customer_orders = list(
                        customer.orders.order_by(
                            'created_at'
                        ).values_list('id', flat=True)
                    )

                    row = [
                        order.description,
                        f'https://admin.supl.biz/orders/{order.id}/',
                        ', '.join(order.rubrics.values_list('title', flat=True)),
                        ', '.join(order.regions.values_list('title', flat=True)),
                        order.get_source_display(),
                        customer.get_type_display(),
                        customer.origin.title if customer.origin else '',
                        customer.id,
                        all_customer_orders.index(order.id) + 1,
                        f'https://admin.supl.biz/profiles/{view.user_id}/',
                    ]

                    result.append(row)

    return result


def unload(file):
    import dropbox, os, csv

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    data = get_orders_data()
    header = [
        'Текст', 'Ссылка', 'Рубрики', 'Регионы', 'Источник',
        'Тип заказчика', 'Город заказчика',
        'id заказчика', 'Какой по счету заказ',
        'Платник, по которому заказ засчитался в стату',
    ]
    w.writerow(header)
    w.writerows(data)

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



def unload(file):
    import dropbox, os, csv
    from django.utils import timezone
    from itertools import groupby

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    year_ago = timezone.now() - timezone.timedelta(days=365)

    subs = Subscription.objects.filter(
        start_date__gt=year_ago,
        user__staff_status__isnull=True
    ).order_by('user')
    subs_list = list(subs.values('user', 'actual_payment'))

    grouped = groupby(subs_list, key=lambda x: x['user'])

    for user, actual_payments in grouped:
        payments = [i['actual_payment'] for i in actual_payments]
        print(user, payments)
        w.writerow([
            f'https://admin.supl.biz/profiles/{user}/',
            sum(filter(None, payments))
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)

def unload(file):
    import dropbox, os, csv
    from django.utils import timezone
    from itertools import groupby
    from tqdm import tqdm
    from project.apps.rubrics.models import Rubric
    from project.apps.cold_callers.models import Lead, LeadActionLogVerbEnum
    from project.apps.users.models import User

    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    rubrics = Rubric.objects.filter(level=1)

    for rubric in tqdm(rubrics):
        user_ct = ContentType.objects.get_for_model(User)
        leads = Lead.objects.filter(
            user_content_type=user_ct
        )
        users = User.objects.filter(
            id__in=leads.values_list('user_id'),
            rubrics__in=rubric.get_descendants(include_self=True)
        )

        leads_wo = Lead.objects.filter(
            user_content_type=user_ct,
            action_logs__verb=LeadActionLogVerbEnum.ORDER_WAS_CREATED
        )
        users_wo = User.objects.filter(
            id__in=leads_wo.values_list('user_id'),
            rubrics__in=rubric.get_descendants(include_self=True)
        )

        w.writerow([
            rubric.title,
            users.count(),
            users_wo.count(),
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'))
    f.close()
    os.remove(file_name)



# reset leads
def check_helper():
    from tqdm import tqdm
    helper = LeadsForResetStatusHelper()

    # leads = helper._get_leads_with_less_duration_call()

    leads = SalesLead.objects.filter(user_id=991155)

    data = []
    for lead in tqdm(list(leads)):
        for filter_info in helper._get_calls_duration_per_period_filters():
            date = helper._get_months_ago(filter_info.month_gt)
            asl = lead.actiononsalesleads.values(
                'caller', 'duration', 'processed_at'
            ).order_by('-processed_at')
            asl = list(asl)
            last_se_processed_at = asl[0]['processed_at']

            for_dur = filter(
                lambda x: x['caller'] == lead.caller_id,
                asl
            )
            duration = sum([i['duration'] for i in for_dur])

            print(lead, last_se_processed_at, duration)

            if last_se_processed_at < date and duration < filter_info.duration_lt:
                data.append([
                    f'https://admin.supl.biz/profiles/{lead.user_id}/',
                    last_se_processed_at,
                    duration,
                    filter_info.month_gt,
                    filter_info.duration_lt,
                ])
                break

    return data


file_name = 'reset_leads'


# scp root@supl.biz:/tmp/reset_leads.csv ~/reset_leads.csv
def unload(file_name):
    import csv

    data = check_helper_leads_with_less_duration_call()
    with open(f'/tmp/{file_name}.csv', 'w+') as f:
        w = csv.writer(f)

        w.writerow([
            'Ссылка',
            'Дата последнего обработанного события',
            'Длительность звонков',
            'Месяцев (Фильтр - последний звонок больше N месяцев)',
            'Продолжительность (Фильтр - продолжительность звонков меньше N)',
        ])

        w.writerows(data)



"Нужны лиды с продаж, у которых есть продавец и которым подключали демо, в формате: Профиль/Продавец/Сколько раз подключали демо/Сколько контактов открыто на всех демо суммарно"
def get_data():
    from tqdm import tqdm
    from project.apps.sales.models import DemoTariffLog, SalesLead
    from project.apps.logs.models import UserClickOrderContactsLog
    leads = SalesLead.objects.filter(
        caller__isnull=False
    )

    data = []
    for lead in tqdm(list(leads)):
        demo_logs = DemoTariffLog.objects.filter(
            user_id=lead.user_id
        )
        contacts_count = 0
        for log in demo_logs:
            date_range = (log.date_start, log.date_finish)
            contacts = UserClickOrderContactsLog.objects.filter(
                user_id=lead.user_id,
                created_at__range=date_range
            )
            contacts_count += contacts.count()

        data.append([
            f'https://admin.supl.biz/profiles/{lead.user_id}/',
            lead.caller.name,
            demo_logs.count(),
            contacts_count
        ])

    return data


file_name = 'sales_leads_opening_contacts_on_demo'


# scp root@supl.biz:/tmp/sales_leads_opening_contacts_on_demo.csv ~/sales_leads_opening_contacts_on_demo.csv
def unload(file_name):
    import csv

    data = get_data()
    with open(f'/tmp/{file_name}.csv', 'w+') as f:
        w = csv.writer(f)

        w.writerow([
            'Ссылка',
            'Продавец',
            'Сколько подключений',
            'Сколько всего открыто на демо',
        ])

        w.writerows(data)
