import datetime
dt_from = datetime.date(2020, 10, 1)
dt_to = datetime.date(2020, 11, 21)
dt_range = (dt_from, dt_to)


def get_customers_with_offers():
    orders = Order.objects.filter(
        offers__created_at__range=dt_range
    )

    user_ids = orders.order_by('user').distinct('user').values_list('user', flat=True)

    return list(user_ids)


def get_events_chat_with_operator():
    import csv

    events = ChatStartChatWithOperator.objects.filter(created_at__date__range=dt_range)

    user_ids = list(filter(None, list(events.values_list('actor_id', flat=True))))
    print(f'events_chat_with_operator: {len(user_ids)}')

    with open(f'/tmp/events_chat_with_operator.csv', 'w+') as f:
        w = csv.writer(f)
        for i in user_ids:
            w.writerow([i])


def resume():
    """
    1. Кол-во заказчиков, у которых есть хотя бы 1 предложение (с 01.10.2020 по 20.11.2020) и ни разу не было события ChatStartChatWithOperator (из кликстрима).
    2. Кол-во заказчиков, у которых есть хотя бы 1 предложение (с 01.10.2020 по 20.11.2020) и есть хотя бы 1 событие ChatStartChatWithOperator.
    3. Общее кол-во предложений, которое получили заказчики из п.1 (за период с 01.10.2020 по 20.11.2020).
    4. Кол-во предложений из п.3, на которые ответили заказчики (оставили быструю реакцию или написали коммент).
    5. Общее кол-во предложений, которое получили заказчики из п.2 (за период с 01.10.2020 по 20.11.2020).
    6. Кол-во предложений из п.5, на которые ответили заказчики (оставили быструю реакцию или написали коммент).
    7. Среднее кол-во ответов (комментариев или быстрых реакций) на 1 предложение для заказчиков из п.1
    8. Среднее кол-во ответов (комментариев или быстрых реакций) на 1 предложение для заказчиков из п.2 (edited)
    """
    customers_with_offers = get_customers_with_offers()
    users_chat_with_operator = get_user_ids_from_file('/tmp/events_chat_with_operator.csv')

    first_ = set(customers_with_offers) - set(users_chat_with_operator)
    print(f'1: {len(first_)}')

    second_ = set(customers_with_offers) & set(users_chat_with_operator)
    print(f'2: {len(second_)}')

    offers = Offer.objects.filter(created_at__range=dt_range)
    third_ = offers.filter(order__user_id__in=first_)
    print(f'3: {third_.count()}')

    fourth_ = third_.filter(offercomment__isnull=False).distinct()
    print(f'4: {fourth_.count()}')

    fifth_ = offers.filter(order__user_id__in=second_)
    print(f'5: {fifth_.count()}')

    sixth_ = fifth_.filter(offercomment__isnull=False).distinct()
    print(f'6: {sixth_.count()}')

    comments_stats = {}
    for o in tqdm(third_):
        comments_stats.update({o.id: o.offercomment_set.count()})

    from collections import Counter
    import math
    values = comments_stats.values()
    max_comments = max(values)
    counter = Counter(values)
    total_comments = sum(values)
    avg = round(total_comments / third_.count(), 1)
    sorted_comments_count = sorted(values, reverse=True)
    med = sorted_comments_count[(math.ceil(len(sorted_comments_count) / 2))]
    print(f'{max_comments = }, {avg = }, {med = }, {counter = }')

    comments_stats2 = {}
    for o in tqdm(fifth_):
        comments_stats2.update({o.id: o.offercomment_set.count()})

    values = comments_stats2.values()
    max_comments = max(values)
    counter = Counter(values)
    total_comments = sum(values)
    avg = round(total_comments / fifth_.count(), 1)
    sorted_comments_count = sorted(values, reverse=True)
    med = sorted_comments_count[(math.ceil(len(sorted_comments_count) / 2))]
    print(f'{max_comments = }, {avg = }, {med = }, {counter = }')



def get_user_ids_from_file(name):
    import csv
    with open(name, 'r') as f:
        reader = csv.reader(f)

        ids = []
        for row in reader:
            ids.append(int(row[0]))

        return ids
