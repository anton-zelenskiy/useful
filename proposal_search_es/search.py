from collections import defaultdict
from typing import Optional

from ..utils import search
from .query_builders import (
    get_es_query_for_global_proposals_search,
    get_es_query_for_searching_proposals_in_category,
)
from .utils import extract_user_ids_from_buckets


class AreaEnum:
    """Enum для названий областей поиска. Используются для именования аггрегаций."""

    FIRST = 'first'
    SECOND = 'second'
    THIRD = 'third'

    @classmethod
    def get_next_area(cls, current_area):
        if current_area == cls.FIRST:
            return cls.SECOND
        if current_area == cls.SECOND:
            return cls.THIRD
        return None

    AREAS = (
        FIRST,
        SECOND,
        THIRD,
    )

    SEARCH_RANGES = {
        FIRST: {'to': 100, 'key': FIRST},
        SECOND: {'from': 100, 'to': 500, 'key': SECOND},
        THIRD: {'from': 500, 'key': THIRD},
    }


def search_proposals_by_category_through_all_suppliers(
        region: dict = None,
        area=AreaEnum.FIRST,
        category=None,
        category_filter_values=None,
        search_string=None,
        availability=None,
        payment_types=None,
        delivery_types=None,
        price_gte=None,
        price_lte=None,
        seo_friendly=None,
        size=20,
        page=1,
        **kwargs,
):
    """Осуществляет поиск товаров."""
    common_filter_kwargs = {
        'region': region,
        'price_gte': price_gte,
        'price_lte': price_lte,
        'category': category,
        'search_string': search_string,
        'category_filter_values': category_filter_values,
        'availability': availability,
        'payment_types': payment_types,
        'delivery_types': delivery_types,
        'seo_friendly': seo_friendly,
    }

    if category:
        query_builder = get_es_query_for_searching_proposals_in_category
        new_category = check_category_has_proposals(query_builder, common_filter_kwargs)
        common_filter_kwargs['category'] = new_category
    else:
        query_builder = get_es_query_for_global_proposals_search
        del common_filter_kwargs['category']
        del common_filter_kwargs['category_filter_values']

    base_query = query_builder(**common_filter_kwargs)
    area_buckets = get_stats(region=region, base_query=base_query)
    del area_buckets['first']
    del area_buckets['second']
    del area_buckets['third']

    default_retval = {
        'hits': [],
        'area': AreaEnum.THIRD,
        'page': -1,
    }

    area = get_area(area_buckets, area)

    print(f'{area = }')

    # Нет областей с товарами
    if not area:
        return default_retval

    bucket = area_buckets.get(area, {})
    user_buckets = bucket.get('by_user', {}).get('buckets', [])
    kk = [{i['key']: i['doc_count']} for i in user_buckets]
    print(f'{kk = }')
    if not user_buckets:
        return default_retval

    user_ids_with_proposal_indices = extract_user_ids_from_buckets(
        user_buckets, page=page, size=size
    )
    print(f'{user_ids_with_proposal_indices = }')

    if not user_ids_with_proposal_indices:
        return default_retval

    common_filter_kwargs.update(
        {'user_ids': [i[0] for i in user_ids_with_proposal_indices]}
    )
    base_query = query_builder(**common_filter_kwargs)
    proposals = search_with_users_top_hits_aggregation(
        user_ids_with_proposal_indices, base_query=base_query, size=size
    )

    if proposals:
        return {
            'hits': proposals,
            'area': area,
            'page': page + 1,
        }

    return default_retval


def get_area(area_stats, area: Optional[str] = AreaEnum.FIRST):
    """Возвращает область, в которой есть уникальные поставщики с товарами."""
    bucket = area_stats.get(area, {})
    if not bucket.get('by_user', {}).get('buckets'):
        next_area = AreaEnum.get_next_area(area)
        if not next_area:
            return None
        return get_area(area_stats, area=next_area)

    return area


def check_category_has_proposals(query_builder_func, common_filter_kwargs):
    """Проверяет, есть ли товары под категорию. Если нет, то пытаемся подняться
    по категории на уровень выше, чтобы выдать хоть какой-то контент.

    Возвращает категорию, под которую есть товары."""
    category = common_filter_kwargs.get('category')
    if not category:
        return None

    # Если категория 2 уровня, не делаем лишний запрос к эластику
    if category.level == 1:
        return category

    base_query = query_builder_func(**common_filter_kwargs)
    if get_search_query_stats(base_query=base_query):
        return category

    category_ancestors = category.get_ancestors().filter(level__gt=0).order_by('-level')
    for cat_ancestor in category_ancestors:
        common_filter_kwargs['category'] = cat_ancestor
        base_query = query_builder_func(**common_filter_kwargs)
        if get_search_query_stats(base_query=base_query):
            return cat_ancestor

    return category


def get_search_query_stats(base_query):
    """Возвращает стату по кол-ву уникальных пользователей для поискового запроса. """
    body = {
        'size': 0,
        'query': base_query,
        'aggs': {'unique_users': {'cardinality': {'field': 'user_id'}}},
    }

    result = search(body)

    return result['hits']['total']['value']


def search_with_users_top_hits_aggregation(
        user_ids_with_proposal_indices, base_query=None, size=20
):
    """Осуществляет запрос на непосредственный поиск товаров."""
    # Определяем горизонтальный срез товаров, чтобы не запрашивать слишком много за раз.
    indices = [i[1] for i in user_ids_with_proposal_indices]
    min_index = min(indices)
    max_index = max(indices)
    size_ = max_index - min_index + 1

    # Т.к. срез может содержать больше size товаров, нам необходимо вытащить только
    # нужные. Зная 'индексы' товаров, полученные из extract_user_ids_from_buckets(),
    # приведем из 'к 0' (23, 24 -> 0, 1). Далее из итогового результата top_hits для
    # каждого пользователя будут взяты нужные товары.
    user_indices = defaultdict(list)
    for user_id, index in user_ids_with_proposal_indices:
        user_indices[user_id].append(index - min_index)

    body = {
        'size': 0,
        'query': base_query,
        'aggs': {
            'by_user': {
                'terms': {
                    'field': 'user_id',
                    'size': size,
                    'order': {'top_score': 'desc'},
                },
                'aggs': {
                    'top_score': {'max': {'script': '_score'}},
                    'top_hits': {
                        'top_hits': {
                            'from': min_index,
                            'size': size_,
                        }
                    },
                },
            },
        },
    }

    search_result = search(body)
    buckets = search_result['aggregations']['by_user']['buckets']

    new_buckets = []
    for b in buckets:
        b['top_hits'] = b['top_hits']['hits']['hits']
        b['indices'] = user_indices[b['key']]
        new_buckets.append(b)

    return extract_proposals_from_buckets(new_buckets)


def extract_proposals_from_buckets(buckets):
    """Извлекает товары юзеров с учетом переданных индексов (bucket['indices'])."""
    result = []
    for i, bucket in enumerate(buckets, start=1):
        indices = bucket['indices']
        for j, index in enumerate(indices):
            doc = bucket['top_hits'][index]
            # Для сортировки, чтобы не было подряд идущих пользователей
            doc['_sort_value_'] = i * j
            result.append(doc)

    return sorted(result, key=lambda x: x['_sort_value_'])


def get_stats(region: dict, base_query=None) -> dict:
    """Возвращает статистику по областям поиска: сколько в области уникальных
    поставщиков и сколько у них товаров."""
    region = region or {}

    ranges = list(AreaEnum.SEARCH_RANGES.values())
    if region.get('is_country'):
        ranges = [AreaEnum.SEARCH_RANGES.get(AreaEnum.FIRST)]

    # Если у города нет координат, то подпихиваем нулевые координаты, чтобы поиск не
    # поломался, т.к. в фильтре мы все равно добавляем фильтр по стране
    coordinates = {
        'lat': region.get('latitude', 0) or 0,
        'lon': region.get('longitude', 0) or 0,
    }

    body = {
        'size': 0,
        'query': base_query,
        'aggs': {
            'areas': {
                'geo_distance': {
                    'field': 'user_location',
                    'origin': coordinates,
                    'unit': 'km',
                    'ranges': ranges,
                    'keyed': True,
                },
                'aggs': {
                    'by_user': {
                        'terms': {
                            'field': 'user_id',
                            'size': 10000,
                            'order': {'top_score': 'desc'},
                        },
                        'aggs': {'top_score': {'max': {'script': '_score'}}},
                    },
                },
            }
        },
    }

    result = search(body)

    return result['aggregations']['areas']['buckets']


def test_searching(page=1, size=20):
    query_data = {
        # 'category': 32824,
        'region': 1,
        'search_string': 'фильтр топливный',
        'page': page,
        'size': size,
        'area': 'first',
    }
    s = ProposalsThroughAllSuppliersFilter(data=query_data)
    s.is_valid(raise_exception=True)
    data = dict(s.validated_data)
    proposals = search_proposals_by_category_through_all_suppliers(**data)
    hits = proposals['hits']
    for h in hits:
        print(h['_source']['user_id'], h['_source']['id'], h['_score'], h['_source']['title'], h['_source']['_meta']['user']['tariff'])

    return [h['_source']['id'] for h in hits]


def test_searching(page=1, size=20):
    query_data = {
        # 'category': 32824,
        'region': 1,
        'search_string': 'фильтр топливный',
        'page': page,
        'size': size,
        'area': 'first',
    }
    s = ProposalsThroughAllSuppliersFilter(data=query_data)
    s.is_valid(raise_exception=True)
    data = dict(s.validated_data)
    proposals = search_proposals_through_all_suppliers(**data)
    hits = proposals['hits']
    for h in hits:
        print(h['_source']['user_id'], h['_source']['id'], h['_score'], h['_source']['title'], h['_source']['_meta']['user']['tariff'])

    return [h['_source']['id'] for h in hits]
