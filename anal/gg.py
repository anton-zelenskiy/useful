"""
1. Кол-во тех, кто с 19.10.2020 по 09.11.2020 сделал (хотя бы одно из условий):
- опубликовал предложение
- оставил коммент к предложению
- просмотрел более 30 заказов
- купил тариф
- добавил 2 или более товаров
2. Кто из пункта 1 не имеет ни одной рубрики.
3. Кол-во тех, кто с 19.10.2020 по 09.11.2020 сделал событие home page sale form action click (св-во ViewOrdersClick) и ему провели презентацию
4. Кол-во тех, кто с 19.10.2020 по 09.11.2020 сделал событие home page sale form finished (версия А) и ему провели презентацию

"""


def user_with_action():
    import datetime
    from itertools import groupby
    from supl_shared.redis.redis import get_redis, RedisDBEnum

    dt_from = datetime.date(2020, 10, 19)
    dt_to = datetime.date(2020, 11, 10)
    dt_range = (dt_from, dt_to)

    user_published_offers = Offer.objects.filter(
        created_at__date__range=dt_range,
    ).values_list('user', flat=True)

    user_published_comments_to_offer = Comment.objects.filter(
        created_at__date__range=dt_range,
        content_type=ContentType.objects.get_for_model(Offer),
        for_admin=False
    ).values_list('user', flat=True)

    user_clicked_orders = list(Click.objects.filter(
        created_at__date__range=dt_range,
        content_type=ContentType.objects.get_for_model(Order),
    ).values('user', 'created_at').order_by('user'))
    clicked_orders = groupby(user_clicked_orders, key=lambda x: x['user'])
    user_clicked_ids = []
    for user, clicks in clicked_orders:
        if len(list(clicks)) >= 30:
            user_clicked_ids.append(user)

    user_buy_tariff = Subscription.objects.filter(
        start_date__range=dt_range,
    ).values_list('user', flat=True)

    redis = get_redis(db=RedisDBEnum.DEFAULT, decode_responses=True)
    users_created_proposals = redis.lrange('test:users_created_proposals', 0, -1)

    user_ids = list(user_published_offers) + list(user_published_comments_to_offer) + list(user_buy_tariff) + user_clicked_ids + users_created_proposals

    return set(user_ids)


def get_users_created_proposals():
    import datetime
    from itertools import groupby
    from supl_shared.redis.redis import get_redis, RedisDBEnum

    dt_from = datetime.date(2020, 10, 19)
    dt_to = datetime.date(2020, 11, 10)
    dt_range = (dt_from, dt_to)

    redis = get_redis(db=RedisDBEnum.DEFAULT, decode_responses=True)

    users_cp = list(Proposal.objects.filter(
        created_at__range=dt_range, status=ProposalStatusEnum.PUBLISHED,
        source=ProposalSourceEnum.ADDED_MANUALLY,
    ).values('user_id', 'id').order_by('user_id'))

    grouped = groupby(users_cp, key=lambda x: x['user_id'])

    print(len(users_cp))
    user_ids = set()
    for user_id, proposals in grouped:
        print(user_id)
        if len(list(proposals)) >= 2:
            user_ids.add(user_id)

    print(len(set(user_ids)))

    key = 'test:users_created_proposals'
    redis.delete(key)
    redis.lpush(key, *user_ids)
    redis.expire(key, 60 * 60 * 24)


def events_sale_form_action_click():
    import csv
    import datetime

    dt_from = datetime.date(2020, 10, 19)
    dt_to = datetime.date(2020, 11, 10)
    dt_range = (dt_from, dt_to)

    events = HomePageSaleFormActionClick.objects.filter(action='ViewOrdersClick', created_at__date__range=dt_range)

    user_ids = list(filter(None, list(events.values_list('actor_id', flat=True))))
    print(f'form_action_click: {len(user_ids)}')

    with open(f'/tmp/sale_form_action_click.csv', 'w+') as f:
        w = csv.writer(f)
        for i in user_ids:
            w.writerow([i])


def events_sale_form_finished():
    import csv
    import datetime

    dt_from = datetime.date(2020, 10, 19)
    dt_to = datetime.date(2020, 11, 10)
    dt_range = (dt_from, dt_to)

    events = HomePageSaleFormFinished.objects.filter(created_at__date__range=dt_range)

    user_ids = list(filter(None, list(events.values_list('actor_id', flat=True))))
    print(f'form_finished: {len(user_ids)}')

    with open(f'/tmp/sale_form_finished.csv', 'w+') as f:
        w = csv.writer(f)
        for i in user_ids:
            w.writerow([i])


def get_users_showed_presentation(user_ids):
    import datetime

    dt_from = datetime.date(2020, 10, 19)
    dt_to = datetime.date(2020, 11, 10)
    dt_range = (dt_from, dt_to)

    users_showed_presentation = ActionOnSalesLead.objects.filter(
        created_at__date__range=dt_range,
        lead__user__in=user_ids
    ).values_list('lead__user', flat=True)

    return set(list(users_showed_presentation))


def resume():
    user_ids_with_action = user_with_action()
    print(f'1: {len(user_ids_with_action)}')

    users_without_rubrics = User.objects.filter(id__in=user_ids_with_action, rubrics__isnull=True).count()
    print(f'2: {users_without_rubrics}')

    user_ids_3 = get_user_ids_from_file('/tmp/sale_form_action_click.csv')
    third = get_users_showed_presentation(user_ids_3)
    print(f'3: {len(third)}')

    user_ids_4 = get_user_ids_from_file('/tmp/sale_form_finished.csv')
    fourth = get_users_showed_presentation(user_ids_4)
    print(f'4: {len(fourth)}')


def get_user_ids_from_file(name):
    import csv
    with open(name, 'r') as f:
        reader = csv.reader(f)

        ids = []
        for row in reader:
            ids.append(int(row[0]))

        return ids
