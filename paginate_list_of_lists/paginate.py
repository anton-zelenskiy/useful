from typing import List, Dict


def extract_user_ids_from_buckets(buckets: List[Dict], page, page_size):
    """Извлекает поставщиков из бакетов в соответствии с количеством у них товаров.
    По сути здесь работает пагинация, как если бы мы обходили список списков,
    где doc_count - `размерность` вложенного списка.
    Пример:
    data = [
        {"key": 210717, "doc_count": 285, "top_score": {"value": 77.96116638183594}},
        {"key": 1741256, "doc_count": 101, "top_score": {"value": 68.48846435546875}},
        {"key": 990155, "doc_count": 47, "top_score": {"value": 68.30876159667969}},
        {"key": 1645063, "doc_count": 1, "top_score": {"value": 59.695430755615234}},
        {"key": 1371975, "doc_count": 3, "top_score": {"value": 59.30809020996094}},
        {"key": 1219817, "doc_count": 33, "top_score": {"value": 58.750572204589844}},
        {"key": 961397, "doc_count": 5, "top_score": {"value": 58.49807357788086}},
        {"key": 1584867, "doc_count": 1, "top_score": {"value": 58.49807357788086}},
        {"key": 1676456, "doc_count": 1, "top_score": {"value": 58.49807357788086}},
        {"key": 1043620, "doc_count": 1, "top_score": {"value": 58.2652702331543}},
    ]

    1 страница: [210717, 1741256, 990155, 1645063, 1371975]
    2 страница: [1219817, 961397, 1584867, 1676456, 1043620]
    3 страница: [210717, 1741256, 990155, 1371975, 1219817]
    и т.д.

    Обходим каждый бакет и набираем user_ids длиной page_size,
    пока не обойдем все бакеты.
    """
    from copy import deepcopy

    if page < 1:
        return []

    copy_buckets = deepcopy(buckets)

    # 'индекс вложенного списка'
    current_doc_index = 0
    # 'индекс внешнего списка' (бакет)
    current_bucket_index = 0

    # Количество элементов, которые нужно пропустить
    docs_to_skip_count = page_size * (page - 1)

    while True:
        buckets_count = len(copy_buckets)
        if buckets_count == 0:
            return []

        # Определяем область (горизонтальный срез, как если бы это был список списков),
        # начиная от длины самого маленького вложенного списка,
        # т.е. бакета с минимальным значением doc_count,
        # по которой будем высчитывать 'индексы внешнего и внутренного списков'.
        # В дальнейшем, если current_doc_index < concrete_bucket['doc_count'],
        # значит мы можем взять поставщика для следующей страницы.
        shortest_doc_count_through_buckets = min(
            copy_buckets, key=lambda x: x['doc_count']
        )['doc_count']
        docs_we_are_considering = buckets_count * (
                shortest_doc_count_through_buckets - current_doc_index
        )

        # Можно сразу вычислять смещение по бакетам
        if docs_we_are_considering >= docs_to_skip_count:
            current_doc_index += docs_to_skip_count // buckets_count
            current_bucket_index = docs_to_skip_count % buckets_count
            break
        # Нужно передвинуть горизонтальный срез на конец текущего, удалить бакеты с
        # минимальной длиной и заново искать смещение
        else:
            current_doc_index = shortest_doc_count_through_buckets
            copy_buckets = list(
                filter(
                    lambda x: x['doc_count'] != shortest_doc_count_through_buckets,
                    copy_buckets,
                )
            )
            docs_to_skip_count -= docs_we_are_considering

    result = []
    largest_doc_count_through_buckets = max(copy_buckets, key=lambda x: x['doc_count'])[
        'doc_count'
    ]

    while (
            len(result) < page_size
            and current_doc_index < largest_doc_count_through_buckets
    ):
        current_bucket = copy_buckets[current_bucket_index]

        if current_doc_index < current_bucket['doc_count']:
            result.append(current_bucket['key'])

        # В текущем бакете пользователя не осталось товаров, переключаемся на следующий
        current_bucket_index += 1
        if current_bucket_index == len(copy_buckets):
            current_bucket_index = 0
            current_doc_index += 1

    return result


def test_extra(page=1, size=5):
    data = [
        {"key": 210717, "doc_count": 285, "top_score": {"value": 77.96116638183594}},
        {"key": 1741256, "doc_count": 101, "top_score": {"value": 68.48846435546875}},
        {"key": 990155, "doc_count": 47, "top_score": {"value": 68.30876159667969}},
        {"key": 1645063, "doc_count": 1, "top_score": {"value": 59.695430755615234}},
        {"key": 1371975, "doc_count": 3, "top_score": {"value": 59.30809020996094}},
        {"key": 1219817, "doc_count": 33, "top_score": {"value": 58.750572204589844}},
        {"key": 961397, "doc_count": 5, "top_score": {"value": 58.49807357788086}},
        {"key": 1584867, "doc_count": 1, "top_score": {"value": 58.49807357788086}},
        {"key": 1676456, "doc_count": 1, "top_score": {"value": 58.49807357788086}},
        {"key": 1043620, "doc_count": 1, "top_score": {"value": 58.2652702331543}},
    ]

    stop = 10

    ids = extract_user_ids_from_buckets(data, page, size)
    print(ids)

    while ids and page < stop:
        extract_user_ids_from_buckets(data, page + 1, size)
