def unload(file):
    import dropbox, os, csv, datetime
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    f = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(f)

    from django.db.models.functions import TruncDate
    from django.db.models import Count, F
    from itertools import groupby

    asl = ActionOnSalesLead.objects.filter(
        type__in=[
            SalesEventTypesEnum.PAGE,
            SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
            SalesEventTypesEnum.TARIFF_PREMIUM_VIEW
        ],
        created_at__gt=timezone.now() - timezone.timedelta(days=90)
    ).annotate(
        caller_name=F('caller__name')
    ).annotate(
        created_dt=TruncDate('created_at')
    ).annotate(
        orders_count=F('lead__user__sales_orders_count_full')
    ).filter(
        orders_count__gt=10
    ).order_by('created_dt', 'caller_name').annotate(
        ct=Count('created_dt')
    ).values(
        'created_dt', 'ct', 'caller_name'
    )

    gg = groupby(list(asl), key=lambda x: (x['created_dt'], x['caller_name']))

    for g in gg:
        w.writerow([
            g[0][0],
            g[0][1],
            len(list(g[1]))
        ])

    f.close()

    f = open(file_name, 'rb')
    client.files_upload(f.read(), file_name.replace('/tmp', '/Разовые_выгрузки'), autorename=True)
    f.close()
    os.remove(file_name)


def aa():
    from django.db.models.functions import TruncDate
    from django.db.models import Count, F
    from itertools import groupby

    asl = ActionOnSalesLead.objects.filter(
        type__in=[
            SalesEventTypesEnum.PAGE,
            SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
            SalesEventTypesEnum.TARIFF_PREMIUM_VIEW
        ],
        created_at__gt=timezone.now() - timezone.timedelta(days=90)
    ).annotate(
        caller_name=F('caller__name')
    ).annotate(
        created_dt=TruncDate('created_at')
    ).annotate(
        orders_count=F('lead__user__sales_orders_count_full')
    ).filter(
        orders_count__gt=10
    ).order_by('created_dt', 'caller_name').annotate(
        ct=Count('created_dt')
    ).values(
        'created_dt', 'ct', 'caller_name'
    )

    gg = groupby(list(asl), key=lambda x: (x['created_dt'], x['caller_name']))

    for g in gg:
        print(g[0][0], g[0][1], len(list(g[1])))
