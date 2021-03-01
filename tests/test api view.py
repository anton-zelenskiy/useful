def test():
    from django.contrib.auth.models import AnonymousUser
    from rest_framework.test import APIRequestFactory

    from project.apps.orders.api import ApiOrderSearchView
    data = {
        'size': 15,
        'query': '820702',
        'rubrics': '',
        'regions': '',
        'completed': 'all',
        'exact': 'false',
        'is_order_id': 'true',
        'hide_viewed': 'false',
        'exclude_blocked_users': 'false',
        'filter_by_supply_city': 'true',
    }
    request = APIRequestFactory().get('/orders/elsearch/', data=data)
    request.user = AnonymousUser()

    request = ApiOrderSearchView().initialize_request(request)
    view = ApiOrderSearchView()
    view.request = request
    actual_params = view._get_query_params(request)

    return actual_params

params = test()
In [7]: result = EsOrderSearch(**params).search()
{'sort': [{'actualized_at': {'order': 'desc'}}], 'query': {'bool': {'filter': [{'terms': {'status': ['published', 'completed']}}]}}, '_source': {'includes': ['_meta', 'actualized_at']}, 'from': 0, 'size': 15}

In [8]: result = EsOrderSearch(**params).search()
