def lead_duration(user_id, caller_id=None):
    from django.db.models import Sum
    lead = SalesLead.objects.get(user_id=user_id)

    logs = ActionOnSalesLead.objects.filter(
        lead=lead,
        caller=caller_id or lead.caller
    )

    for log in logs:
        print(log.created_at, log.caller.name, log.duration)

    return logs.aggregate(duration=Sum('duration'))['duration']


print(lead_duration(user_id=1307620))


def get_calls_duration_per_period_filters():
    """
    Фильтры для продолжительности звонков за определённый период

    `month_gt` - последний звонок лиду более `month_gt` месяцев назад
    `duration_lt` - суммарная длительность звонков меньше `duration_lt`
    """
    FilterInfo = namedtuple(
        'FilterInfo',
        ['month_gt', 'duration_lt']
    )
    return [
        FilterInfo(month_gt=0, duration_lt=3),
        FilterInfo(month_gt=1, duration_lt=4),
        FilterInfo(month_gt=2, duration_lt=5),
        FilterInfo(month_gt=3, duration_lt=12),
        FilterInfo(month_gt=4, duration_lt=22),
        FilterInfo(month_gt=5, duration_lt=42),
        FilterInfo(month_gt=6, duration_lt=62),
    ]


def get_months_ago(amount: int):
    """
    Возвращает дату, количество 'amount' месяцев назад
    """
    now = timezone.now()
    if amount is None:
        return now

    days = amount * 30

    if days:
        days = days - 1
    return now - timezone.timedelta(days=days)


def check_lead_reset(user_id):
    lead = SalesLead.objects.get(user_id=user_id)

    logs = ActionOnSalesLead.objects.order_by('-processed_at').filter(
        lead=lead,
        caller=lead.caller
    )

    last_processed_call = logs.first()
    last_call_dt = last_processed_call.processed_at
    all_duration = logs.aggregate(duration=Sum('duration'))['duration']

    print(last_call_dt)
    print(all_duration)

    for filter_info in get_calls_duration_per_period_filters():
        # print(filter_info.month_gt, filter_info.duration_lt)
        date = get_months_ago(filter_info.month_gt)

        dt_diff = (date - last_call_dt).days

        print(date, dt_diff, filter_info.duration_lt)


leads = SalesLead.objects.filter(caller__isnull=False).annotate(
    sum_duration=Sum(
        Case(
            When(
                actiononsalesleads__caller=F('caller'),
                then=F('actiononsalesleads__duration')
            ),
            default=Value(0),
            output_field=IntegerField(),
        )
    ),
    last_se_processed_at=Coalesce(
        Max('actiononsalesleads__processed_at'),
        Value(timezone.now() - timezone.timedelta(days=30*7))
    )
).filter(sum_duration__gt=62)


def months_between_two_dates():
    """Месяцев между датами"""
    from datetime import datetime
    from dateutil import relativedelta
    date1 = datetime.strptime(str('2018-04-20 12:00:00'),
                                   '%Y-%m-%d %H:%M:%S')
    date2 = datetime.strptime(str('2019-07-22'), '%Y-%m-%d')
    relative_delta = relativedelta.relativedelta(date2, date1)
    return relative_delta.years * 12 + relative_delta.months



def nltk_experiments():
    import nltk
    from nltk import pos_tag, word_tokenize, tagset_mapping

    # Download
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger_ru')
    nltk.download('universal_tagset')

    # Russian tagset
    tagset_mapping('ru-rnc-new', 'universal')
    """
    :return
        {'A': 'ADJ',
        'A-PRO': 'PRON',
        'ADV': 'ADV',
        'ADV-PRO': 'PRON',
        'ANUM': 'ADJ',
        'CONJ': 'CONJ',
        'INTJ': 'X',
        'NONLEX': '.',
        'NUM': 'NUM',
        'PARENTH': 'PRT',
        'PART': 'PRT',
        'PR': 'ADP',
        'PRAEDIC': 'PRT',
        'PRAEDIC-PRO': 'PRON',
        'S': 'NOUN',
        'S-PRO': 'PRON',
        'V': 'VERB'}
    """
    tagset_mapping('ru-rnc', 'universal')

    pos_tag(word_tokenize('Адаптер латунный 1 2 внешняя резьба PALISAD 66272'), lang='rus')
    """
    :return
        [('Адаптер', 'S'),
         ('латунный', 'A=m'),
         ('1', 'NUM=ciph'),
         ('2', 'NUM=ciph'),
         ('внешняя', 'A=f'),
         ('резьба', 'S'),
         ('PALISAD', 'NONLEX'),
         ('66272', 'NUM=ciph')]
    """