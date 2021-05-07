# Прокачка
rtbmedia.public.report_reportgenaccount содержит cred_id - его берем с прода здесь https://report.improvado.io/admin/rtbmedia/adplatformcredentials/

Запускаем update_report_accounts, чтобы сгенерить креды по этим аккаунтам

dce rtbmedia ./manage.py update_report_accounts --provider=



"""
Алгоритм миграции

* Проверяем, что сегодня нужный репорт прокачался сегодня и больше сегодня он качаться не будет
    * Нужно, чтобы у вас было достаточно времени, чтобы провести все необзодимые манипуляции
* Внимание! Проверяем, у провайдера dsas_name равен ли db_name
    * Если нет, то нужно будет на стороне clickhouse поменять имена таблиц этого провайдера. Заменить dsas_name на db_name. Так же нужно переименовать fdw вьюху
        * Пример, eventbrite
* Селектим для каждого репорт тайпа тоталы по суммам метрик, чтобы потом сравнить с итоговым результатом
* Запускаем команду в кластере migrate_report_etl_to_rtbm.py (https://github.com/tekliner/rtbmedia/pull/6962/files#diff-1a7af72fa73f9a5bc497fed3bd6f54df68d28fd3ac0f7c6ece2657b3554bc01c) на каждый репорт тайп
* Селектим новые тоталы для каждого репорт тайпа, сравниваем со старыми. Данные должны совпадать.
    * Если случилась беда и данные не совпадают - бежим к Вове, чтобы вернуть все назад
* Как все перемигрировали и протестировали - проталкиваем пул реквест в деплой, чтобы его срочно задеплоили


getintent:
- время прокачки:
+ dsas_name = db_name
+ check totals:
select sum(unique_imps) as sum_unique_imps, sum(clicks) as sum_clicks, sum(impression) as sum_impression, sum(spent) as spent_sum from creatives_3256_getintent_table




"""


# TODO:
#  Change eventbrite -> eventbrite_sales на стороне clickhouse поменять имена таблиц этого провайдера.
#  Заменить dsas_name на db_name. Так же нужно переименовать fdw вьюху

def get_metrics():
    for provider_iname, provider_id, report in (
        ('getintent', 45, 'creatives'),
        ('adriver', 109, 'banners'),
        ('apple_search', 160, 'adsets'),
        ('apple_search', 160, 'keywords'),
        ('apple_search', 160, 'searchterms'),
        ('apple_search', 160, 'adsets_geo'),
        ('apple_search', 160, 'adsets_device'),
        ('awin', 441, 'creatives'),
        ('awin', 441, 'publisher'),
        ('awin', 441, 'transactions'),
        ('getcake', 445, 'conversions'),
        ('getcake', 445, 'sub_affiliate'),
        ('getcake', 445, 'campaign'),
        ('branch_tune', 450, 'sub_sites_devices'),
        ('branch_tune', 450, 'sub_sites_my_campaign'),
        ('branch_tune', 450, 'keywords'),
        ('branch_tune', 450, 'campaign'),
        ('branch_tune', 450, 'sub_sites_campaign'),
        ('eventbrite', 451, 'eventbrite_sales'),  # ???
        ('act_on', 462, 'message_report'),
        ('act_on', 462, 'optout_list'),
        ('act_on', 462, 'daily_message'),
        ('act_on', 462, 'spam_complaint_list'),
        ('act_on', 462, 'message_drilldown'),
        ('act_on', 462, 'message_list'),
        ('tune_affiliate', 463, 'offers'),
        ('advangelists', 523, 'banners'),
        ('kenshoo_api_v3', 526, 'custom_campaign_by_device'),
        ('kenshoo_api_v3', 526, 'keywords_by_device'),
        ('yahoo_gemini', 71, 'conversions'),
        ('yahoo_gemini', 71, 'geo'),
    ):
        filters = ["provider_id = %s AND '%s' = ANY(report_types)" % (provider_id, report)]
        f_str = ' AND '.join(filters)

        query = '''
            SELECT provider_id, array_agg(field_name) as arr
            FROM report_metric_type
            WHERE %s
            AND field_type = 'metric'
            GROUP BY provider_id
        ''' % f_str

        print(query)


# [(agency_id, provider_db_name, report_type, metrics)]
SAFE_REPORTS = [
    (1, 'getintent', 'creatives', ["clicks", "unique_imps", "spent", "impression"]),
    (3256, 'getintent', 'creatives', ["clicks", "unique_imps", "spent", "impression"]),

    (102, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (3202, 'apple_search', 'searchterms', ["conv", "imps", "taps", "spend"]),
    (5717, 'apple_search', 'adsets_geo', ["imps", "conv", "spend", "taps"]),
    (3937, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),
    (3937, 'apple_search', 'adsets_device', ["imps", "conv", "spend", "taps"]),
    (3937, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (3937, 'apple_search', 'adsets_geo', ["imps", "conv", "spend", "taps"]),
    (3842, 'apple_search', 'searchterms', ["conv", "imps", "taps", "spend"]),
    (3842, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (3842, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),
    (5571, 'apple_search', 'adsets_geo', ["imps", "conv", "spend", "taps"]),
    (5571, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (5571, 'apple_search', 'adsets_device', ["imps", "conv", "spend", "taps"]),
    (5571, 'apple_search', 'searchterms', ["conv", "imps", "taps", "spend"]),
    (5571, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),
    (1, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (4424, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (4424, 'apple_search', 'adsets_device', ["imps", "conv", "spend", "taps"]),
    (4424, 'apple_search', 'adsets_geo', ["imps", "conv", "spend", "taps"]),
    (4424, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),
    (1623, 'apple_search', 'adsets', ["conv", "imps", "taps", "spend"]),
    (1623, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),
    (102, 'apple_search', 'keywords', ["conv", "imps", "taps", "cpt_bid", "spend"]),

    (5571, 'awin', 'creatives',
     ["bonus_comm", "bonus_no", "bonus_value", "clicks", "confirmed_comm", "confirmed_no", "confirmed_value",
      "declined_comm", "declined_no", "declined_value", "impressions", "pending_comm", "pending_no", "pending_value",
      "total_comm", "total_no", "total_value"]),
    (5571, 'awin', 'publisher',
     ["impressions", "clicks", "pending_no", "pending_value", "pending_comm", "confirmed_no", "confirmed_value",
      "confirmed_comm", "bonus_no", "bonus_value", "bonus_comm", "total_no", "total_value", "total_comm", "declined_no",
      "declined_value", "declined_comm"]),
    (5571, 'awin', 'transactions', ["commission_amount", "sale_amount"]),

    (3675, 'getcake', 'conversions', ["price"]),
    (3675, 'getcake', 'sub_affiliate',
     ["clicks", "conversions", "events", "impressions", "lite_clicks", "revenue", "total_lite_clicks"]),
    (3675, 'getcake', 'campaign',
     ["impressions", "clicks", "conversions", "revenue", "events", "price", "lite_clicks", "total_lite_clicks"]),
    (1, 'getcake', 'campaign',
     ["impressions", "clicks", "conversions", "revenue", "events", "price", "lite_clicks", "total_lite_clicks"]),
    (1, 'getcake', 'sub_affiliate',
     ["clicks", "conversions", "events", "impressions", "lite_clicks", "revenue", "total_lite_clicks"]),
    (1, 'getcake', 'conversions', ["price"]),

    (3202, 'branch_tune', 'sub_sites_devices',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (3202, 'branch_tune', 'sub_sites_my_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd", "gross_clicks"]),
    (3202, 'branch_tune', 'keywords', ["installs", "conv", "visits", "events", "enrollments"]),
    (3202, 'branch_tune', 'campaign',
     ["installs", "events", "payouts", "revenues_usd", "ad_clicks", "ad_clicks_unique", "ad_impressions",
      "ad_impressions_unique", "opens", "purchase", "updates"]),
    (3202, 'branch_tune', 'sub_sites_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (1623, 'branch_tune', 'sub_sites_devices',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (1623, 'branch_tune', 'campaign',
     ["installs", "events", "payouts", "revenues_usd", "ad_clicks", "ad_clicks_unique", "ad_impressions",
      "ad_impressions_unique", "opens", "purchase", "updates"]),
    (1623, 'branch_tune', 'sub_sites_my_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd", "gross_clicks"]),
    (1623, 'branch_tune', 'keywords', ["installs", "conv", "visits", "events", "enrollments"]),
    (1623, 'branch_tune', 'sub_sites_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (102, 'branch_tune', 'campaign',
     ["installs", "events", "payouts", "revenues_usd", "ad_clicks", "ad_clicks_unique", "ad_impressions",
      "ad_impressions_unique", "opens", "purchase", "updates"]),
    (102, 'branch_tune', 'keywords', ["installs", "conv", "visits", "events", "enrollments"]),
    (102, 'branch_tune', 'sub_sites_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (102, 'branch_tune', 'sub_sites_devices',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd"]),
    (102, 'branch_tune', 'sub_sites_my_campaign',
     ["installs", "conv", "visits", "events", "enrollments", "payouts", "revenues_usd", "gross_clicks"]),

    (1, 'eventbrite', 'eventbrite_sales', ["total_tickets_amount", "quantity_of_sold_tickets"]),

    (3675, 'tune_affiliate', 'offers', ["stat_clicks", "stat_impressions", "stat_payout", "stat_conversions"]),
    (3256, 'tune_affiliate', 'offers', ["stat_clicks", "stat_impressions", "stat_payout", "stat_conversions"]),

    (3368, 'advangelists', 'banners', ["impressions", "conversions", "completes", "spend", "clicks"]),
    (4439, 'advangelists', 'banners', ["impressions", "conversions", "completes", "spend", "clicks"]),

    (102, 'kenshoo_api_v3', 'custom_campaign_by_device',
     ["cost", "validated_leads_revenue", "m_install_revenue", "appointment_revenue", "purchase_revenue",
      "online_aisle_revenue", "orders_revenue", "server_download_revenue", "mobile_download_revenue",
      "ecommerce_purchase_revenue", "forrester_lead_revenue", "subscription_revenue", "impressions", "clicks",
      "revenue", "conversions", "conversions_revenue", "d_v_start", "d_enroll", "m_install", "appointment",
      "online_aisle", "orders", "purchase", "server_download", "mobile_download", "ecommerce_purchase",
      "forrester_lead", "subscription", "validated_leads", "cloud_dbaas", "cloud_dbaas_revenue", "white_pages",
      "white_pages_revenue"]),
    (102, 'kenshoo_api_v3', 'keywords_by_device',
     ["conversions", "potential_impressions", "lost_is_rank", "cost", "impressions", "revenue", "clicks"]),

    (3641, 'yahoo_gemini', 'conversions',
     ["post_view_conversions", "post_click_conversions", "conversions", "post_click_conversion_value",
      "post_view_conversion_value"]),
    (3651, 'yahoo_gemini', 'geo',
     ["impressions", "clicks", "conv", "total_conversions", "spend", "follows", "engagements", "likes", "video_views",
      "video_starts", "video_closed", "video_skipped"]),
]


DANGER_REPORTS = [
    (3710, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression']),
    (1570, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression']),
    (1, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression']),
    (3256, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression']), (
    3576, 'yahoo_gemini', 'adroll_custom_report',
    ['spend', 'impressions', 'clicks', 'conversions', 'video_views']), (
    1981, 'yahoo_gemini', 'campaign_device',
    ['impressions', 'clicks', 'spend', 'conv', 'total_conversions', 'opens', 'video_views', 'video_starts',
     'video_closed', 'video_skipped']),
    (3641, 'yahoo_gemini', 'conversions',
                                         ['post_view_conversions', 'post_click_conversions', 'conversions',
                                          'post_click_conversion_value', 'post_view_conversion_value']),
    (
    1981, 'yahoo_gemini', 'device_keywords',
    ['conv', 'total_conversions', 'impressions', 'spend', 'url', 'average_position', 'clicks']), (
    3651, 'yahoo_gemini', 'geo',
    ['impressions', 'clicks', 'conv', 'total_conversions', 'spend', 'follows', 'engagements', 'likes',
     'video_views', 'video_starts', 'video_closed', 'video_skipped']), (
    3860, 'yahoo_gemini', 'search_query',
    ['conv', 'impressions', 'spend', 'clicks', 'post_click_conversions', 'post_impression_conversions',
     'impression_share', 'click_share', 'conversion_share']), (3710, 'adriver', 'banners',
                                                               ['impression', 'creative_view', 'start',
                                                                'midpoint', 'first_quartile',
                                                                'third_quartile', 'complete', 'mute',
                                                                'unmute', 'pause', 'click', 'exp']),
    (102, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3202, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3202, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (3202, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (3749, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (3749, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3749, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (5717, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps']),
    (3660, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3937, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (3937, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps']),
    (3937, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3937, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps']),
    (3842, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (3842, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (3842, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (5571, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps']),
    (5571, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (5571, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps']),
    (5571, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (5571, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (1, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (4570, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (4570, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (4570, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (4424, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (4424, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps']),
    (4424, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps']),
    (4546, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps']),
    (4546, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend']),
    (4424, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (4994, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (4994, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (1623, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend']),
    (1623, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']),
    (102, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend']), (
    5571, 'awin', 'creatives',
    ['bonus_comm', 'bonus_no', 'bonus_value', 'clicks', 'confirmed_comm', 'confirmed_no', 'confirmed_value',
     'declined_comm', 'declined_no', 'declined_value', 'impressions', 'pending_comm', 'pending_no',
     'pending_value', 'total_comm', 'total_no', 'total_value']), (5571, 'awin', 'publisher',
                                                                  ['impressions', 'clicks', 'pending_no',
                                                                   'pending_value', 'pending_comm',
                                                                   'confirmed_no', 'confirmed_value',
                                                                   'confirmed_comm', 'bonus_no',
                                                                   'bonus_value', 'bonus_comm', 'total_no',
                                                                   'total_value', 'total_comm',
                                                                   'declined_no', 'declined_value',
                                                                   'declined_comm']),
    (5571, 'awin', 'transactions', ['commission_amount', 'sale_amount']),
    (3884, 'awin', 'transactions', ['commission_amount', 'sale_amount']), (3884, 'awin', 'publisher',
                                                                           ['impressions', 'clicks',
                                                                            'pending_no', 'pending_value',
                                                                            'pending_comm', 'confirmed_no',
                                                                            'confirmed_value',
                                                                            'confirmed_comm', 'bonus_no',
                                                                            'bonus_value', 'bonus_comm',
                                                                            'total_no', 'total_value',
                                                                            'total_comm', 'declined_no',
                                                                            'declined_value',
                                                                            'declined_comm']), (
    3884, 'awin', 'creatives',
    ['bonus_comm', 'bonus_no', 'bonus_value', 'clicks', 'confirmed_comm', 'confirmed_no', 'confirmed_value',
     'declined_comm', 'declined_no', 'declined_value', 'impressions', 'pending_comm', 'pending_no',
     'pending_value', 'total_comm', 'total_no', 'total_value']),
    (3675, 'getcake', 'conversions', ['price']), (3675, 'getcake', 'sub_affiliate',
                                                  ['clicks', 'conversions', 'events', 'impressions',
                                                   'lite_clicks', 'revenue', 'total_lite_clicks']), (
    3675, 'getcake', 'campaign',
    ['impressions', 'clicks', 'conversions', 'revenue', 'events', 'price', 'lite_clicks',
     'total_lite_clicks']), (1, 'getcake', 'campaign',
                             ['impressions', 'clicks', 'conversions', 'revenue', 'events', 'price',
                              'lite_clicks', 'total_lite_clicks']), (1, 'getcake', 'sub_affiliate',
                                                                     ['clicks', 'conversions', 'events',
                                                                      'impressions', 'lite_clicks',
                                                                      'revenue', 'total_lite_clicks']),
    (1, 'getcake', 'conversions', ['price']), (3202, 'branch_tune', 'sub_sites_devices',
                                               ['installs', 'conv', 'visits', 'events', 'enrollments',
                                                'payouts', 'revenues_usd']), (
    3202, 'branch_tune', 'sub_sites_my_campaign',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks']),
    (3202, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments']), (
    3202, 'branch_tune', 'campaign',
    ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions',
     'ad_impressions_unique', 'opens', 'purchase', 'updates']), (3202, 'branch_tune', 'sub_sites_campaign',
                                                                 ['installs', 'conv', 'visits', 'events',
                                                                  'enrollments', 'payouts',
                                                                  'revenues_usd']), (
    1623, 'branch_tune', 'sub_sites_devices',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd']), (
    1623, 'branch_tune', 'campaign',
    ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions',
     'ad_impressions_unique', 'opens', 'purchase', 'updates']), (
    1623, 'branch_tune', 'sub_sites_my_campaign',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks']),
    (1623, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments']), (
    1623, 'branch_tune', 'sub_sites_campaign',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd']), (
    102, 'branch_tune', 'campaign',
    ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions',
     'ad_impressions_unique', 'opens', 'purchase', 'updates']),
    (102, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments']), (
    102, 'branch_tune', 'sub_sites_campaign',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd']), (
    102, 'branch_tune', 'sub_sites_devices',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd']), (
    102, 'branch_tune', 'sub_sites_my_campaign',
    ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks']),
    (5710, 'eventbrite', 'eventbrite_sales', ['total_tickets_amount', 'quantity_of_sold_tickets']),
    (5629, 'eventbrite', 'eventbrite_sales', ['total_tickets_amount', 'quantity_of_sold_tickets']),
    (3927, 'eventbrite', 'eventbrite_sales', ['total_tickets_amount', 'quantity_of_sold_tickets']),
    (1, 'eventbrite', 'eventbrite_sales', ['total_tickets_amount', 'quantity_of_sold_tickets']), (
    3916, 'act_on', 'message_report',
    ['sent', 'delivered', 'bounced', 'hard_bounced', 'soft_bounced', 'notopened', 'optout', 'last_open',
     'last_clicks', 'last_open_ts', 'rebroad_cast', 'rebroad_cast_click', 'clicked', 'unuqie_clicked',
     'opened', 'unique_opened', 'effective_opened', 'unique_effective_opened', 'notsent',
     'unique_notsent']), (3916, 'act_on', 'optout_list', []),
    (3916, 'act_on', 'daily_message', ['opted_out', 'sent', 'bounced', 'clicked', 'opened']),
    (3916, 'act_on', 'spam_complaint_list', []), (3916, 'act_on', 'message_drilldown', []),
    (3916, 'act_on', 'message_list', []), (3675, 'tune_affiliate', 'offers',
                                           ['stat_clicks', 'stat_impressions', 'stat_payout',
                                            'stat_conversions']), (3256, 'tune_affiliate', 'offers',
                                                                   ['stat_clicks', 'stat_impressions',
                                                                    'stat_payout', 'stat_conversions']),
    (3368, 'advangelists', 'banners', ['impressions', 'conversions', 'completes', 'spend', 'clicks']),
    (4439, 'advangelists', 'banners', ['impressions', 'conversions', 'completes', 'spend', 'clicks']), (
    102, 'kenshoo_api_v3', 'custom_campaign_by_device',
    ['cost', 'validated_leads_revenue', 'm_install_revenue', 'appointment_revenue', 'purchase_revenue',
     'online_aisle_revenue', 'orders_revenue', 'server_download_revenue', 'mobile_download_revenue',
     'ecommerce_purchase_revenue', 'forrester_lead_revenue', 'subscription_revenue', 'impressions',
     'clicks', 'revenue', 'conversions', 'conversions_revenue', 'd_v_start', 'd_enroll', 'm_install',
     'appointment', 'online_aisle', 'orders', 'purchase', 'server_download', 'mobile_download',
     'ecommerce_purchase', 'forrester_lead', 'subscription', 'validated_leads', 'cloud_dbaas',
     'cloud_dbaas_revenue', 'white_pages', 'white_pages_revenue']), (
    102, 'kenshoo_api_v3', 'keywords_by_device',
    ['conversions', 'potential_impressions', 'lost_is_rank', 'cost', 'impressions', 'revenue', 'clicks'])
]


def test():
    for agency_id, provider_name, report_type, metrics in SAFE_REPORTS:
        print(agency_id, provider_name, report_type, metrics)
        print(create_totals_sql(report_type, agency_id, provider_name, metrics))
        print('\n\n')


def create_totals_sql(report_type, agency_id, provider, metrics):
    table_name = '{0}_{1}_{2}_table'.format(report_type, agency_id, provider)
    metrics_str = ', '.join(['sum({0}) as sum_{0}'.format(m) for m in metrics])

    return "select %s from %s" % (metrics_str, table_name)


metrics_map = {
    ('apple_search', 'keywords'): ['conv', 'imps', 'taps', 'cpt_bid', 'spend'],
    ('eventbrite', 'eventbrite_sales'): ['total_tickets_amount', 'quantity_of_sold_tickets'],
    ('getcake', 'sub_affiliate'): ['clicks', 'conversions', 'events', 'impressions', 'lite_clicks', 'revenue',
                                   'total_lite_clicks'],
    ('yahoo_gemini', 'geo'): ['impressions', 'clicks', 'conv', 'total_conversions', 'spend', 'follows', 'engagements',
                              'likes', 'video_views', 'video_starts', 'video_closed', 'video_skipped'],
    ('apple_search', 'adsets_device'): ['imps', 'conv', 'spend', 'taps'],
    ('branch_tune', 'sub_sites_campaign'): ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts',
                                            'revenues_usd'],
    ('branch_tune', 'sub_sites_my_campaign'): ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts',
                                               'revenues_usd', 'gross_clicks'],
    ('apple_search', 'adsets_geo'): ['imps', 'conv', 'spend', 'taps'], ('getcake', 'conversions'): ['price'],
    ('kenshoo_api_v3', 'custom_campaign_by_device'): ['cost', 'validated_leads_revenue', 'm_install_revenue',
                                                      'appointment_revenue', 'purchase_revenue', 'online_aisle_revenue',
                                                      'orders_revenue', 'server_download_revenue',
                                                      'mobile_download_revenue', 'ecommerce_purchase_revenue',
                                                      'forrester_lead_revenue', 'subscription_revenue', 'impressions',
                                                      'clicks', 'revenue', 'conversions', 'conversions_revenue',
                                                      'd_v_start', 'd_enroll', 'm_install', 'appointment',
                                                      'online_aisle', 'orders', 'purchase', 'server_download',
                                                      'mobile_download', 'ecommerce_purchase', 'forrester_lead',
                                                      'subscription', 'validated_leads', 'cloud_dbaas',
                                                      'cloud_dbaas_revenue', 'white_pages', 'white_pages_revenue'],
    ('branch_tune', 'keywords'): ['installs', 'conv', 'visits', 'events', 'enrollments'],
    ('apple_search', 'searchterms'): ['conv', 'imps', 'taps', 'spend'],
    ('kenshoo_api_v3', 'keywords_by_device'): ['conversions', 'potential_impressions', 'lost_is_rank', 'cost',
                                               'impressions', 'revenue', 'clicks'],
    ('awin', 'publisher'): ['impressions', 'clicks', 'pending_no', 'pending_value', 'pending_comm', 'confirmed_no',
                            'confirmed_value', 'confirmed_comm', 'bonus_no', 'bonus_value', 'bonus_comm', 'total_no',
                            'total_value', 'total_comm', 'declined_no', 'declined_value', 'declined_comm'],
    ('getcake', 'campaign'): ['impressions', 'clicks', 'conversions', 'revenue', 'events', 'price', 'lite_clicks',
                              'total_lite_clicks'],
    ('branch_tune', 'campaign'): ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique',
                                  'ad_impressions', 'ad_impressions_unique', 'opens', 'purchase', 'updates'],
    ('getintent', 'creatives'): ['clicks', 'unique_imps', 'spent', 'impression'],
    ('tune_affiliate', 'offers'): ['stat_clicks', 'stat_impressions', 'stat_payout', 'stat_conversions'],
    ('branch_tune', 'sub_sites_devices'): ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts',
                                           'revenues_usd'],
    ('awin', 'creatives'): ['bonus_comm', 'bonus_no', 'bonus_value', 'clicks', 'confirmed_comm', 'confirmed_no',
                            'confirmed_value', 'declined_comm', 'declined_no', 'declined_value', 'impressions',
                            'pending_comm', 'pending_no', 'pending_value', 'total_comm', 'total_no', 'total_value'],
    ('apple_search', 'adsets'): ['conv', 'imps', 'taps', 'spend'],
    ('advangelists', 'banners'): ['impressions', 'conversions', 'completes', 'spend', 'clicks'],
    ('awin', 'transactions'): ['commission_amount', 'sale_amount'],
    ('yahoo_gemini', 'conversions'): ['post_view_conversions', 'post_click_conversions', 'conversions',
                                      'post_click_conversion_value', 'post_view_conversion_value'],
    ('yahoo_gemini', 'adroll_custom_report'): ["spend","impressions","clicks","conversions","video_views"],
    ('yahoo_gemini', 'campaign_device'): ["impressions","clicks","spend","conv","total_conversions","opens","video_views","video_starts","video_closed","video_skipped"],
    ('yahoo_gemini', 'device_keywords'): ["conv","total_conversions","impressions","spend","url","average_position","clicks"],
    ('yahoo_gemini', 'search_query'): ["conv","impressions","spend","clicks","post_click_conversions","post_impression_conversions","impression_share","click_share","conversion_share"],
    ('adriver', 'banners'): ["impression","creative_view","start","midpoint","first_quartile","third_quartile","complete","mute","unmute","pause","click","exp"],
    ('act_on', 'message_report'): ["sent","delivered","bounced","hard_bounced","soft_bounced","notopened","optout","last_open","last_clicks","last_open_ts","rebroad_cast","rebroad_cast_click","clicked","unuqie_clicked","opened","unique_opened","effective_opened","unique_effective_opened","notsent","unique_notsent"],
    ('act_on', 'optout_list'): [],
    ('act_on', 'daily_message'): ["opted_out","sent","bounced","clicked","opened"],
    ('act_on', 'spam_complaint_list'): [],
    ('act_on', 'message_drilldown'): [],
    ('act_on', 'message_list'): [],
}


pp = (
    ('getintent', 45, 'creatives'),
    ('adriver', 109, 'banners'),
    ('apple_search', 160, 'adsets'),
    ('apple_search', 160, 'keywords'),
    ('apple_search', 160, 'searchterms'),
    ('apple_search', 160, 'adsets_geo'),
    ('apple_search', 160, 'adsets_device'),
    ('awin', 441, 'creatives'),
    ('awin', 441, 'publisher'),
    ('awin', 441, 'transactions'),
    ('getcake', 445, 'conversions'),
    ('getcake', 445, 'sub_affiliate'),
    ('getcake', 445, 'campaign'),
    ('branch_tune', 450, 'sub_sites_devices'),
    ('branch_tune', 450, 'sub_sites_my_campaign'),
    ('branch_tune', 450, 'keywords'),
    ('branch_tune', 450, 'campaign'),
    ('branch_tune', 450, 'sub_sites_campaign'),
    ('eventbrite', 451, 'eventbrite_sales'),  # ???
    ('act_on', 462, 'message_report'),
    ('act_on', 462, 'optout_list'),
    ('act_on', 462, 'daily_message'),
    ('act_on', 462, 'spam_complaint_list'),
    ('act_on', 462, 'message_drilldown'),
    ('act_on', 462, 'message_list'),
    ('tune_affiliate', 463, 'offers'),
    ('advangelists', 523, 'banners'),
    ('kenshoo_api_v3', 526, 'custom_campaign_by_device'),
    ('kenshoo_api_v3', 526, 'keywords_by_device'),
    ('yahoo_gemini', 71, 'conversions'),
    ('yahoo_gemini', 71, 'geo'),
)

provider_map = {i[0]: i[1] for i in pp}



"""

(1, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression'])
select sum(clicks) as sum_clicks, sum(unique_imps) as sum_unique_imps, sum(spent) as sum_spent, sum(impression) as sum_impression from creatives_1_getintent_table
totals: 834,550.00	309,652,846.00	12,140,249.09	498,567,636.00
command: migrate_report_etl_to_rtbm --provider 45 --report-type creatives --agency 1





(3256, 'getintent', 'creatives', ['clicks', 'unique_imps', 'spent', 'impression'])
select sum(clicks) as sum_clicks, sum(unique_imps) as sum_unique_imps, sum(spent) as sum_spent, sum(impression) as sum_impression from creatives_3256_getintent_table

(102, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_102_apple_search_table

(3202, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from searchterms_3202_apple_search_table

(5717, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_geo_5717_apple_search_table

(3937, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_3937_apple_search_table

(3937, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_device_3937_apple_search_table

(3937, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_3937_apple_search_table

(3937, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_geo_3937_apple_search_table

(3842, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from searchterms_3842_apple_search_table

(3842, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_3842_apple_search_table

(3842, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_3842_apple_search_table

(5571, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_geo_5571_apple_search_table

(5571, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_5571_apple_search_table

(5571, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_device_5571_apple_search_table

(5571, 'apple_search', 'searchterms', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from searchterms_5571_apple_search_table

(5571, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_5571_apple_search_table

(1, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_1_apple_search_table

(4424, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_4424_apple_search_table

(4424, 'apple_search', 'adsets_device', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_device_4424_apple_search_table

(4424, 'apple_search', 'adsets_geo', ['imps', 'conv', 'spend', 'taps'])
select sum(imps) as sum_imps, sum(conv) as sum_conv, sum(spend) as sum_spend, sum(taps) as sum_taps from adsets_geo_4424_apple_search_table

(4424, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_4424_apple_search_table

(1623, 'apple_search', 'adsets', ['conv', 'imps', 'taps', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(spend) as sum_spend from adsets_1623_apple_search_table

(1623, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_1623_apple_search_table

(102, 'apple_search', 'keywords', ['conv', 'imps', 'taps', 'cpt_bid', 'spend'])
select sum(conv) as sum_conv, sum(imps) as sum_imps, sum(taps) as sum_taps, sum(cpt_bid) as sum_cpt_bid, sum(spend) as sum_spend from keywords_102_apple_search_table

(5571, 'awin', 'creatives', ['bonus_comm', 'bonus_no', 'bonus_value', 'clicks', 'confirmed_comm', 'confirmed_no', 'confirmed_value', 'declined_comm', 'declined_no', 'declined_value', 'impressions', 'pending_comm', 'pending_no', 'pending_value', 'total_comm', 'total_no', 'total_value'])
select sum(bonus_comm) as sum_bonus_comm, sum(bonus_no) as sum_bonus_no, sum(bonus_value) as sum_bonus_value, sum(clicks) as sum_clicks, sum(confirmed_comm) as sum_confirmed_comm, sum(confirmed_no) as sum_confirmed_no, sum(confirmed_value) as sum_confirmed_value, sum(declined_comm) as sum_declined_comm, sum(declined_no) as sum_declined_no, sum(declined_value) as sum_declined_value, sum(impressions) as sum_impressions, sum(pending_comm) as sum_pending_comm, sum(pending_no) as sum_pending_no, sum(pending_value) as sum_pending_value, sum(total_comm) as sum_total_comm, sum(total_no) as sum_total_no, sum(total_value) as sum_total_value from creatives_5571_awin_table

(5571, 'awin', 'publisher', ['impressions', 'clicks', 'pending_no', 'pending_value', 'pending_comm', 'confirmed_no', 'confirmed_value', 'confirmed_comm', 'bonus_no', 'bonus_value', 'bonus_comm', 'total_no', 'total_value', 'total_comm', 'declined_no', 'declined_value', 'declined_comm'])
select sum(impressions) as sum_impressions, sum(clicks) as sum_clicks, sum(pending_no) as sum_pending_no, sum(pending_value) as sum_pending_value, sum(pending_comm) as sum_pending_comm, sum(confirmed_no) as sum_confirmed_no, sum(confirmed_value) as sum_confirmed_value, sum(confirmed_comm) as sum_confirmed_comm, sum(bonus_no) as sum_bonus_no, sum(bonus_value) as sum_bonus_value, sum(bonus_comm) as sum_bonus_comm, sum(total_no) as sum_total_no, sum(total_value) as sum_total_value, sum(total_comm) as sum_total_comm, sum(declined_no) as sum_declined_no, sum(declined_value) as sum_declined_value, sum(declined_comm) as sum_declined_comm from publisher_5571_awin_table

(5571, 'awin', 'transactions', ['commission_amount', 'sale_amount'])
select sum(commission_amount) as sum_commission_amount, sum(sale_amount) as sum_sale_amount from transactions_5571_awin_table

(3675, 'getcake', 'conversions', ['price'])
select sum(price) as sum_price from conversions_3675_getcake_table

(3675, 'getcake', 'sub_affiliate', ['clicks', 'conversions', 'events', 'impressions', 'lite_clicks', 'revenue', 'total_lite_clicks'])
select sum(clicks) as sum_clicks, sum(conversions) as sum_conversions, sum(events) as sum_events, sum(impressions) as sum_impressions, sum(lite_clicks) as sum_lite_clicks, sum(revenue) as sum_revenue, sum(total_lite_clicks) as sum_total_lite_clicks from sub_affiliate_3675_getcake_table

(3675, 'getcake', 'campaign', ['impressions', 'clicks', 'conversions', 'revenue', 'events', 'price', 'lite_clicks', 'total_lite_clicks'])
select sum(impressions) as sum_impressions, sum(clicks) as sum_clicks, sum(conversions) as sum_conversions, sum(revenue) as sum_revenue, sum(events) as sum_events, sum(price) as sum_price, sum(lite_clicks) as sum_lite_clicks, sum(total_lite_clicks) as sum_total_lite_clicks from campaign_3675_getcake_table

(1, 'getcake', 'campaign', ['impressions', 'clicks', 'conversions', 'revenue', 'events', 'price', 'lite_clicks', 'total_lite_clicks'])
select sum(impressions) as sum_impressions, sum(clicks) as sum_clicks, sum(conversions) as sum_conversions, sum(revenue) as sum_revenue, sum(events) as sum_events, sum(price) as sum_price, sum(lite_clicks) as sum_lite_clicks, sum(total_lite_clicks) as sum_total_lite_clicks from campaign_1_getcake_table

(1, 'getcake', 'sub_affiliate', ['clicks', 'conversions', 'events', 'impressions', 'lite_clicks', 'revenue', 'total_lite_clicks'])
select sum(clicks) as sum_clicks, sum(conversions) as sum_conversions, sum(events) as sum_events, sum(impressions) as sum_impressions, sum(lite_clicks) as sum_lite_clicks, sum(revenue) as sum_revenue, sum(total_lite_clicks) as sum_total_lite_clicks from sub_affiliate_1_getcake_table

(1, 'getcake', 'conversions', ['price'])
select sum(price) as sum_price from conversions_1_getcake_table

(3202, 'branch_tune', 'sub_sites_devices', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_devices_3202_branch_tune_table

(3202, 'branch_tune', 'sub_sites_my_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(gross_clicks) as sum_gross_clicks from sub_sites_my_campaign_3202_branch_tune_table

(3202, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments from keywords_3202_branch_tune_table

(3202, 'branch_tune', 'campaign', ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions', 'ad_impressions_unique', 'opens', 'purchase', 'updates'])
select sum(installs) as sum_installs, sum(events) as sum_events, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(ad_clicks) as sum_ad_clicks, sum(ad_clicks_unique) as sum_ad_clicks_unique, sum(ad_impressions) as sum_ad_impressions, sum(ad_impressions_unique) as sum_ad_impressions_unique, sum(opens) as sum_opens, sum(purchase) as sum_purchase, sum(updates) as sum_updates from campaign_3202_branch_tune_table

(3202, 'branch_tune', 'sub_sites_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_campaign_3202_branch_tune_table

(1623, 'branch_tune', 'sub_sites_devices', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_devices_1623_branch_tune_table

(1623, 'branch_tune', 'campaign', ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions', 'ad_impressions_unique', 'opens', 'purchase', 'updates'])
select sum(installs) as sum_installs, sum(events) as sum_events, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(ad_clicks) as sum_ad_clicks, sum(ad_clicks_unique) as sum_ad_clicks_unique, sum(ad_impressions) as sum_ad_impressions, sum(ad_impressions_unique) as sum_ad_impressions_unique, sum(opens) as sum_opens, sum(purchase) as sum_purchase, sum(updates) as sum_updates from campaign_1623_branch_tune_table

(1623, 'branch_tune', 'sub_sites_my_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(gross_clicks) as sum_gross_clicks from sub_sites_my_campaign_1623_branch_tune_table

(1623, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments from keywords_1623_branch_tune_table

(1623, 'branch_tune', 'sub_sites_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_campaign_1623_branch_tune_table

(102, 'branch_tune', 'campaign', ['installs', 'events', 'payouts', 'revenues_usd', 'ad_clicks', 'ad_clicks_unique', 'ad_impressions', 'ad_impressions_unique', 'opens', 'purchase', 'updates'])
select sum(installs) as sum_installs, sum(events) as sum_events, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(ad_clicks) as sum_ad_clicks, sum(ad_clicks_unique) as sum_ad_clicks_unique, sum(ad_impressions) as sum_ad_impressions, sum(ad_impressions_unique) as sum_ad_impressions_unique, sum(opens) as sum_opens, sum(purchase) as sum_purchase, sum(updates) as sum_updates from campaign_102_branch_tune_table

(102, 'branch_tune', 'keywords', ['installs', 'conv', 'visits', 'events', 'enrollments'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments from keywords_102_branch_tune_table

(102, 'branch_tune', 'sub_sites_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_campaign_102_branch_tune_table

(102, 'branch_tune', 'sub_sites_devices', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd from sub_sites_devices_102_branch_tune_table

(102, 'branch_tune', 'sub_sites_my_campaign', ['installs', 'conv', 'visits', 'events', 'enrollments', 'payouts', 'revenues_usd', 'gross_clicks'])
select sum(installs) as sum_installs, sum(conv) as sum_conv, sum(visits) as sum_visits, sum(events) as sum_events, sum(enrollments) as sum_enrollments, sum(payouts) as sum_payouts, sum(revenues_usd) as sum_revenues_usd, sum(gross_clicks) as sum_gross_clicks from sub_sites_my_campaign_102_branch_tune_table

(1, 'eventbrite', 'eventbrite_sales', ['total_tickets_amount', 'quantity_of_sold_tickets'])
select sum(total_tickets_amount) as sum_total_tickets_amount, sum(quantity_of_sold_tickets) as sum_quantity_of_sold_tickets from eventbrite_sales_1_eventbrite_table

(3675, 'tune_affiliate', 'offers', ['stat_clicks', 'stat_impressions', 'stat_payout', 'stat_conversions'])
select sum(stat_clicks) as sum_stat_clicks, sum(stat_impressions) as sum_stat_impressions, sum(stat_payout) as sum_stat_payout, sum(stat_conversions) as sum_stat_conversions from offers_3675_tune_affiliate_table

(3256, 'tune_affiliate', 'offers', ['stat_clicks', 'stat_impressions', 'stat_payout', 'stat_conversions'])
select sum(stat_clicks) as sum_stat_clicks, sum(stat_impressions) as sum_stat_impressions, sum(stat_payout) as sum_stat_payout, sum(stat_conversions) as sum_stat_conversions from offers_3256_tune_affiliate_table

(3368, 'advangelists', 'banners', ['impressions', 'conversions', 'completes', 'spend', 'clicks'])
select sum(impressions) as sum_impressions, sum(conversions) as sum_conversions, sum(completes) as sum_completes, sum(spend) as sum_spend, sum(clicks) as sum_clicks from banners_3368_advangelists_table

(4439, 'advangelists', 'banners', ['impressions', 'conversions', 'completes', 'spend', 'clicks'])
select sum(impressions) as sum_impressions, sum(conversions) as sum_conversions, sum(completes) as sum_completes, sum(spend) as sum_spend, sum(clicks) as sum_clicks from banners_4439_advangelists_table

(102, 'kenshoo_api_v3', 'custom_campaign_by_device', ['cost', 'validated_leads_revenue', 'm_install_revenue', 'appointment_revenue', 'purchase_revenue', 'online_aisle_revenue', 'orders_revenue', 'server_download_revenue', 'mobile_download_revenue', 'ecommerce_purchase_revenue', 'forrester_lead_revenue', 'subscription_revenue', 'impressions', 'clicks', 'revenue', 'conversions', 'conversions_revenue', 'd_v_start', 'd_enroll', 'm_install', 'appointment', 'online_aisle', 'orders', 'purchase', 'server_download', 'mobile_download', 'ecommerce_purchase', 'forrester_lead', 'subscription', 'validated_leads', 'cloud_dbaas', 'cloud_dbaas_revenue', 'white_pages', 'white_pages_revenue'])
select sum(cost) as sum_cost, sum(validated_leads_revenue) as sum_validated_leads_revenue, sum(m_install_revenue) as sum_m_install_revenue, sum(appointment_revenue) as sum_appointment_revenue, sum(purchase_revenue) as sum_purchase_revenue, sum(online_aisle_revenue) as sum_online_aisle_revenue, sum(orders_revenue) as sum_orders_revenue, sum(server_download_revenue) as sum_server_download_revenue, sum(mobile_download_revenue) as sum_mobile_download_revenue, sum(ecommerce_purchase_revenue) as sum_ecommerce_purchase_revenue, sum(forrester_lead_revenue) as sum_forrester_lead_revenue, sum(subscription_revenue) as sum_subscription_revenue, sum(impressions) as sum_impressions, sum(clicks) as sum_clicks, sum(revenue) as sum_revenue, sum(conversions) as sum_conversions, sum(conversions_revenue) as sum_conversions_revenue, sum(d_v_start) as sum_d_v_start, sum(d_enroll) as sum_d_enroll, sum(m_install) as sum_m_install, sum(appointment) as sum_appointment, sum(online_aisle) as sum_online_aisle, sum(orders) as sum_orders, sum(purchase) as sum_purchase, sum(server_download) as sum_server_download, sum(mobile_download) as sum_mobile_download, sum(ecommerce_purchase) as sum_ecommerce_purchase, sum(forrester_lead) as sum_forrester_lead, sum(subscription) as sum_subscription, sum(validated_leads) as sum_validated_leads, sum(cloud_dbaas) as sum_cloud_dbaas, sum(cloud_dbaas_revenue) as sum_cloud_dbaas_revenue, sum(white_pages) as sum_white_pages, sum(white_pages_revenue) as sum_white_pages_revenue from custom_campaign_by_device_102_kenshoo_api_v3_table

(102, 'kenshoo_api_v3', 'keywords_by_device', ['conversions', 'potential_impressions', 'lost_is_rank', 'cost', 'impressions', 'revenue', 'clicks'])
select sum(conversions) as sum_conversions, sum(potential_impressions) as sum_potential_impressions, sum(lost_is_rank) as sum_lost_is_rank, sum(cost) as sum_cost, sum(impressions) as sum_impressions, sum(revenue) as sum_revenue, sum(clicks) as sum_clicks from keywords_by_device_102_kenshoo_api_v3_table

(3641, 'yahoo_gemini', 'conversions', ['post_view_conversions', 'post_click_conversions', 'conversions', 'post_click_conversion_value', 'post_view_conversion_value'])
select sum(post_view_conversions) as sum_post_view_conversions, sum(post_click_conversions) as sum_post_click_conversions, sum(conversions) as sum_conversions, sum(post_click_conversion_value) as sum_post_click_conversion_value, sum(post_view_conversion_value) as sum_post_view_conversion_value from conversions_3641_yahoo_gemini_table

(3651, 'yahoo_gemini', 'geo', ['impressions', 'clicks', 'conv', 'total_conversions', 'spend', 'follows', 'engagements', 'likes', 'video_views', 'video_starts', 'video_closed', 'video_skipped'])
select sum(impressions) as sum_impressions, sum(clicks) as sum_clicks, sum(conv) as sum_conv, sum(total_conversions) as sum_total_conversions, sum(spend) as sum_spend, sum(follows) as sum_follows, sum(engagements) as sum_engagements, sum(likes) as sum_likes, sum(video_views) as sum_video_views, sum(video_starts) as sum_video_starts, sum(video_closed) as sum_video_closed, sum(video_skipped) as sum_video_skipped from geo_3651_yahoo_gemini_table
"""
