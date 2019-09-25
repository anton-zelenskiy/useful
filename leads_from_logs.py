def unload(file):
    import re
    import datetime
    import dropbox, os, csv
    client = dropbox.Dropbox('qrY0Wl2vngAAAAAAAAAABqeAo6N0NpM7pS3kwrr18Q6-B4fH7Msd0mVMQe4yoYo7')
    file_name = '/tmp/{}.csv'.format(file)
    host = 'https://supl.biz'

    ff = open(file_name, 'w', encoding='utf-8')
    w = csv.writer(ff)
    w.writerow([
        'ссылка на пользователя',
        'seller'
    ])

    result = []
    with open('/tmp/prem-log.txt') as f:
        for line in f:
            ip = re.match(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}', line).group(0)
            result.append(ip)

    ips = set(result)

    hst = SalesEventHistory.objects.filter(
        created_at__gt=datetime.date(2018, 11, 20)
    ).values_list('lead__user', flat=True)

    events = SalesEvent.objects.filter(
        created_at__gt=datetime.date(2018, 11, 20)
    ).values_list('lead__user', flat=True)

    ex_ids = []
    ex_ids.extend(list(hst))
    ex_ids.extend(list(events))

    logs = UserIPLog.objects.filter(
        ip__in=ips,
        ip_changed_at__gt=datetime.date(2018, 11, 20)
    ).values_list('user', flat=True)

    logs = list(logs)

    result_ids = set(logs) - set(ex_ids)

    users = User.objects.filter(
        id__in=result_ids
    ).distinct()

    ee = SalesEvent.objects.filter(lead__user__in=result_ids)
    print(ee.count())

    for user in users:
        w.writerow([
            host + user.get_absolute_url(),
            user.sales_lead.caller.name if hasattr(user, 'sales_lead') and user.sales_lead.caller else ''
        ])

    ff.close()

    ff = open(file_name, 'rb')
    client.files_upload(
        ff.read(),
        file_name.replace('/tmp', '/Разовые_выгрузки'),
        autorename=True
    )
    ff.close()
    os.remove(file_name)


def create_se():
    from project.apps.backend_sales.helpers.se_handler import ClickSalesEventHandler
    result = []
    with open('/tmp/stand-log.txt') as f:
        for line in f:
            ip = re.match(r'\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}', line).group(0)
            result.append(ip)

    ips = set(result)

    hst = SalesEventHistory.objects.filter(
        created_at__gt=datetime.date(2018, 11, 20)
    ).values_list('lead__user', flat=True)

    events = SalesEvent.objects.filter(
        created_at__gt=datetime.date(2018, 11, 20)
    ).values_list('lead__user', flat=True)

    ex_ids = []
    ex_ids.extend(list(hst))
    ex_ids.extend(list(events))

    logs = UserIPLog.objects.filter(
        ip__in=ips,
        ip_changed_at__gt=datetime.date(2018, 11, 20)
    ).values_list('user', flat=True)

    logs = list(logs)

    result_ids = set(logs) - set(ex_ids)

    users = User.objects.filter(
        id__in=result_ids
    ).distinct()

    ee = SalesEvent.objects.filter(lead__user__in=result_ids)
    print(ee.count())

    page = Page.objects.get(id=26)
    for user in users:
        click = Click.objects.create(content_object=page, user=user, ip=user.ip)
        ClickSalesEventHandler.process(click)


        def aa():
          leads = SalesLead.objects.filter(
              salesevents__type__in=[
                  SalesEventTypesEnum.TARIFF_STANDARD_VIEW,
                  SalesEventTypesEnum.TARIFF_PREMIUM_VIEW,
                  SalesEventTypesEnum.PLANNED_CALL
              ]
          ).distinct()

          ids = set()
          for lead in leads:
              se_types = list(lead.salesevents.values_list('type', flat=True))
              if SalesEventTypesEnum.PLANNED_CALL in se_types and (SalesEventTypesEnum.TARIFF_STANDARD_VIEW in se_types or SalesEventTypesEnum.TARIFF_PREMIUM_VIEW in se_types):
                  ids.add(lead.user.id)

          ll = SalesLead.objects.filter(user__in=ids)

          for l in ll:
              print(l)
              events = l.salesevents.all()
              for e in
