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