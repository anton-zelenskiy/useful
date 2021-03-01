from django.db import transaction, IntegrityError
from django.db.models import Q, Subquery
from django.utils import timezone
from django.utils.functional import cached_property

from operator import __or__ as OR
from functools import reduce

from project.apps.marketing.models import SourceEnum
from project.apps.orders.models.order import (
    Order,
    OrderStatusEnum,
)

__all__ = (
    'ModerationSessionSelector',
    'UnassignedSessionSelector',
    'NewSessionSelector',
)


class UnassignedSessionSelector:
    """Класс для выборки зависших сессий.

    Учитывает, что текущий модератор до этого момента не работал
    с данной сессией (проверка на поле `prev_group_moderators`).

    Такие сесии являются `теплыми` (т.е. с ними совсем недавно
    кто-то работал), поэтому их приоритет высокий.

    Проверяет наличие оставшихся подходящих под модератора
    заказов. Фильтрация по настройкам группы исключается (вручную
    переброшенные в другую группу заказы могут не подходить под
    фильтры текущей группы).

    Модератор со звонком может брать ЛЮБЫЕ заказы, если в его
    группе НЕТ модераторов без звонка.

    Модератор со звонком может брать ТОЛЬКО заказы со звонком,
    если в его группе ЕСТЬ модератор без звонка.

    Модератор без звонка - смотрит только подходящие ему без
    звонка заказы.
    """

    def __init__(self, moderator_group_member):
        self.moderator_group_member = moderator_group_member
        self.group = self.moderator_group_member.group
        self.moderator_id = self.moderator_group_member.user_id

    @property
    def group_has_silent_moderators(self):
        """Проверяет, есть ли в группе модераторы без звонка. """
        return self.group.moderator.filter(
            process_order_type=ProcessOrderTypesEnum.SILENT
        ).exists()

    def get(self):
        """Возвращает зависшую сессию. """

        session_qs = self.get_session_qs_with_settings_conditions()

        return session_qs

        if not session_qs:
            return None

        with transaction.atomic():
            session = session_qs.select_for_update().first()

            if not session:
                return None

            self.bind_session_to_moderator(session)

            ModerationSessionLog.objects.create(
                moderator_id=self.moderator_id,
                customer=session.customer,
                log_type=ModerSessionLogTypeEnum.CONNECTED,
                comment='Модератор взял сессию в работу'
            )

        return session

    def get_session_qs_with_settings_conditions(self):
        """Возвращает queryset с учетом настройки модератора по
        типам обрабатываемых заказов"""

        session_qs = self.session_qs

        moderator_process_order_type = (
            self.moderator_group_member.process_order_type
        )

        if moderator_process_order_type == ProcessOrderTypesEnum.SILENT:
            session_qs = self.get_ua_session_for_silent_moderator()

        elif moderator_process_order_type == ProcessOrderTypesEnum.WITH_CALL:
            session_qs = self.get_ua_session_for_caller()

        return session_qs

    @cached_property
    def session_qs(self):
        """Возвращает qs.

        Если в группе один активный звонарь, то не накладываем фильтр
        по исключению prev_group_moderators.
        """
        session_qs = ModerationSession.objects.filter(
            moderator__isnull=True,
            group=self.group,
            empty_call_counter=0,
            planned_call_dt__isnull=True
        ).order_by('created_at')

        is_more_one_moderator_in_group = (
                self.group.moderator.filter(is_active=True).count() > 1
        )

        if is_more_one_moderator_in_group:
            session_qs = session_qs.exclude(
                prev_group_moderators__contains=[self.moderator_id]
            )

        return session_qs

    @cached_property
    def customer_ids(self):
        return list(self.session_qs.values_list('customer', flat=True))

    def get_ua_session_for_silent_moderator(self):
        """Возвращает зависшую сессию для модератора без звонка.

        Почему не Q(customer__orders__creator__is_staff=True)?
        Django orm не умеет следующее:
        FOR UPDATE cannot be applied to the nullable side of an outer join
        """

        customer_qs = Order.objects.filter(
            user__in=self.customer_ids,
            status=OrderStatusEnum.MODERATION,
            creator__is_staff=True,
        ).values_list('user', flat=True)

        return self.session_qs.filter(customer_id__in=Subquery(customer_qs))

    def get_ua_session_for_caller(self):
        """Возвращает зависшую сессию для модератора со звонком. """

        if not self.group_has_silent_moderators:
            return self.session_qs

        customer_qs = Order.objects.filter(
            user__in=self.customer_ids,
            status=OrderStatusEnum.MODERATION,
        ).filter(
            Q(creator__is_staff=False) |
            Q(creator__isnull=True)
        ).values_list('user', flat=True)

        return self.session_qs.filter(customer_id__in=Subquery(customer_qs))

    def bind_session_to_moderator(self, session):
        """Обновляет сессию.
            - устанавливаем звонящего для сессии;
            - сбрасываем дату запланированного звонка.
        """
        session.moderator_id = self.moderator_id
        session.group = self.group
        session.planned_call_dt = None
        session.save()


class FilterByOrderCreatedAt:
    """Класс, предназначенный для построения фильтра по
    времени создания заказа. """

    MIN_HOUR = 0
    MAX_HOUR = 23

    def __init__(self, group):
        self.group = group

        self.group_utc_offset = self.group.utc_offset
        self.msk_utc_offset = 3
        self.current_utc_hour = timezone.now().hour

    def get(self):
        """Возвращает фильтр для заказов, учитывающий дату их создания
        и настройки группы"""

        order_settings = self.group.order_settings

        group_current_time = self.current_utc_hour + self.group_utc_offset

        result_filter = []
        for item in order_settings.values():
            call_hour_from = item['call_hour_range']['from_hour']
            call_hour_to = item['call_hour_range']['to_hour']
            created_at_from = item['order_created_at_range']['from_hour']
            created_at_to = item['order_created_at_range']['to_hour']

            if call_hour_from <= group_current_time <= call_hour_to:

                if created_at_from > created_at_to:
                    # разбивем на 2 интервала: с 21 до 0 и с 0 до 6
                    # (если время в настройках, например, с 21:00 до 06:00)
                    q_filter = self.get_q_filter(hour_from=created_at_from)
                    result_filter.append(q_filter)

                    q_filter = self.get_q_filter(hour_to=created_at_to)
                    result_filter.append(q_filter)

                else:
                    q_filter = self.get_q_filter(hour_from=created_at_from,
                                                 hour_to=created_at_to)
                    result_filter.append(q_filter)

        return reduce(OR, result_filter) if result_filter else Q()

    def get_q_filter(self, hour_from=None, hour_to=None):
        """Возвращает range-фильтр по времени создания заказа.
        Значения часов приводятся к московскому времени,
        т.к. в настройках проекта стоит московский часовой пояс.
        """

        _from = self._get_hour_from(hour_from)
        _to = self._get_hour_to(hour_to)

        return Q(actualized_at__hour__range=(_from, _to))

    def _get_hour_from(self, hour_from=None):
        """Возвращает левую границу для фильтра по времени. """
        return ((hour_from - self.group_utc_offset + self.msk_utc_offset)
                if hour_from else self.MIN_HOUR)

    def _get_hour_to(self, hour_to=None):
        """Возвращает правую границу для фильтра по времени. """
        return ((hour_to - self.group_utc_offset + self.msk_utc_offset)
                if hour_to else self.MAX_HOUR)


class NewSessionSelector:
    """Создает и возвращает сессию для неотмодерированного заказа.

    На первый план заказы от заказчиков, у которых источник - это реклама.

    Проверяет, что для заказчика не должно быть любых сессиий
    модерации. Если сессия есть - значит кто-то с ним уже работает.
    """

    def __init__(self, moderator_group_member):
        self.moderator_group_member = moderator_group_member
        self.group = self.moderator_group_member.group
        self.moderator_id = self.moderator_group_member.user_id

    def get(self):
        """Возвращает сессию. """
        orders_qs = self.get_order_qs()
        return orders_qs

        # for order in orders_qs:
        #     with transaction.atomic():
        #         if ModerationSession.objects.filter(
        #                 customer=order.user).exists():
        #             continue
        #
        #         new_session = self.create_session(order)
        #
        #         if not new_session:
        #             continue
        #
        #         ModerationSessionLog.objects.create(
        #             moderator_id=self.moderator_id,
        #             customer=new_session.customer,
        #             log_type=ModerSessionLogTypeEnum.STARTED,
        #             comment='Инициализация сессии'
        #         )
        #
        #         return new_session

    def create_session(self, order):
        """Создает сессию. """
        try:
            new_session = ModerationSession.objects.create(
                customer=order.user,
                moderator_id=self.moderator_id,
                group=self.group
            )

            return new_session

        except IntegrityError:
            return None

    def get_order_qs(self):
        """Возвращает заказы на модерации, для которых нет сессии. """

        filter_by_source_regions = self._get_qs_filters_by_source_regions()
        filter_by_process_order_type = (
            self._get_qs_filter_by_process_order_type()
        )
        filter_by_order_created_at = FilterByOrderCreatedAt(self.group).get()

        orders = Order.objects.filter(
            status=OrderStatusEnum.MODERATION,
            user__moderation_session__isnull=True
        ).filter(
            filter_by_source_regions
        ).filter(
            filter_by_process_order_type
        ).filter(
            filter_by_order_created_at
        )

        if self.moderator_group_member.moderate_old_orders_firstly:
            orders = orders.order_by('created_at')

        orders_with_ads_source = orders.filter(
            user__source_relation__source__in=SourceEnum.ADS_SOURCES
        )

        if orders_with_ads_source:
            return orders_with_ads_source

        else:
            return orders

    def _get_qs_filters_by_source_regions(self):
        """Возвращает `Q`-фильтры для QuerySet по настройкам, связанным с
        регионами появления заказа.
        """

        countries = self.group.countries
        country_tree_ids = set(countries.values_list('tree_id', flat=True))

        if country_tree_ids:
            countries_q = Q(source_region__tree_id__in=country_tree_ids)
        else:
            countries_q = Q()

        without_sr_q = Q()
        if self.group.without_source_region:
            without_sr_q = Q(source_region__isnull=True)

        _filter = (countries_q | without_sr_q)

        return _filter

    def _get_qs_filter_by_process_order_type(self):
        """Возвращает Q-фильтр для заказов, в зависимости от
        настройки модератора "Модерирует заказы без звонка".

        Если модератор "Модерирует заказы без звонка", то подбираем ему заказы,
        созданные сотрудниками и заказы с включенной галочкой "Без звонка".
        Либо подбираем ему такие заказы, которые были размещены повторно. Об
        этом может сведетельствовать непустая дата публикации заказа.

        Заказы без создателя (созданные из КП), выдаем зврнящим со звонком
        """

        moderator_process_order_type = (
            self.moderator_group_member.process_order_type
        )

        if moderator_process_order_type == ProcessOrderTypesEnum.SILENT:
            return (
                    Q(creator__is_staff=True) |
                    Q(published_at__isnull=False)
            )

        elif moderator_process_order_type == ProcessOrderTypesEnum.WITH_CALL:
            return (
                    (
                            Q(creator__is_staff=False) |
                            Q(creator__isnull=True)
                    ) &
                    Q(published_at__isnull=True)
            )

        elif moderator_process_order_type == ProcessOrderTypesEnum.ALL:
            return Q()


class ModerationSessionSelector:
    """
    Хелпер, предназначенный для создания сессии модерации для
    указанного модератора.
    """
    ordered_methods = (
        'get_urgent_session',
        'get_active_session',
        'get_session_with_artificial_call_with_sms',
        'get_session_with_natural_planned_calls',
        'get_unassigned_session',
        'select_from_pull',
        'get_session_with_artificial_planned_calls',
    )

    def __init__(self, moderator):
        self.moderator = moderator
        self.moderator_group_member = ModeratorGroupMember.objects.get(
            user_id=moderator.id,
            is_active=True
        )
        self.group = self.moderator_group_member.group
        self.now_plus_five_min = timezone.now() + timezone.timedelta(minutes=5)

    def get_session(self):
        """Подбирает сессию модерации для модератора. """

        for method_name in self.ordered_methods:
            method = getattr(self, method_name)
            session = method()
            print(method_name, session)

            if method_name == 'get_unassigned_session':
                for s in session[:10]:
                    print(s.customer)
                    logs = ModerationSessionLog.objects.filter(customer=s.customer)
                    for l in logs:
                        print(l.comment)

        #     if not session:
        #         continue
        #
        #     return session
        #
        # return ModerationSession.objects.none()

    def get_urgent_session(self):
        """Выбирает срочные сессии, которые были созданы для
        указанного модератора.
        """

        return ModerationSession.objects.filter(
            moderator=self.moderator,
            group=self.group,
            urgent=True
        ).order_by('-updated_at').count()

    def get_active_session(self):
        """Возвращает активную сессию модератора. """

        return ModerationSession.objects.filter(
            moderator=self.moderator,
            group=self.group,
            planned_call_dt__isnull=True
        ).count()

    def get_session_with_artificial_call_with_sms(self):
        """Возвращает сессию с недозвоном, по которой было отправлено смс. """
        if (self.moderator_group_member.process_order_type ==
                ProcessOrderTypesEnum.SILENT):
            return None

        return ModerationSession.objects.filter(
            group=self.group,
            moderator__isnull=True,
            empty_call_counter=1,
            empty_call_with_sms=True,
            planned_call_dt__lte=timezone.now()
        ).count()

        with transaction.atomic():
            session = queryset.select_for_update(skip_locked=True).first()

            if not session:
                return None

            # Обновляем сессию (делаем ее 'активной')
            session.moderator = self.moderator
            session.planned_call_dt = None
            session.save()

            ModerationSessionLog.objects.create(
                moderator_id=self.moderator.id,
                customer=session.customer,
                log_type=ModerSessionLogTypeEnum.CONNECTED,
                comment='Модератор взял сессию в работу'
            )

        return session

    def get_session_with_natural_planned_calls(self):
        """Возвращает сессии с запланированными звонками, которые
        были созданы вручную модераторами.

        Приоритет у таких сессий низкий, поскольку чем старее
        заказ - тем он становится менее актуальным.
        """

        return ModerationSession.objects.filter(
            moderator=self.moderator,
            group=self.group,
            planned_call_dt__lte=self.now_plus_five_min,
            empty_call_counter=0
        ).count()

    def get_unassigned_session(self):
        """Возвращает `Зависшую` сессию. """

        unassigned_session = UnassignedSessionSelector(
            moderator_group_member=self.moderator_group_member
        ).get()

        return unassigned_session

    def select_from_pull(self):
        """Возвращает новую сессию для неотмодерированного заказа. """

        new_session = NewSessionSelector(
            moderator_group_member=self.moderator_group_member
        ).get()

        return new_session

    def get_session_with_artificial_planned_calls(self):
        """Возвращает сессию с запланированным звонком, который
        был создан системой после недозвона.

        Количество подряд идущих недозвонов должно быть меньше 5-ти.

        Недозвоны выдаем только модераторам 'cо звонком'
        Если нет недозвонов по времени (т.е. кончились заказы для модерации,
        то выдаем недозвоны, игнорируя дату недозвона.
        Дополнительно сортируем такие сессии по дате обновления,
        чтобы не выпадал один и тот же недозвон).
        """

        if (self.moderator_group_member.process_order_type ==
                ProcessOrderTypesEnum.SILENT):
            return None

        planned_call_dt_filter = Q(planned_call_dt__lte=self.now_plus_five_min)

        base_session_qs = ModerationSession.objects.filter(
            group=self.group,
            moderator__isnull=True,
        ).filter(
            Q(
                empty_call_counter__gt=1,
                empty_call_with_sms=True
            ) |
            Q(
                empty_call_counter__gt=0,
                empty_call_with_sms=False,
            )
        )

        session_qs = base_session_qs.filter(planned_call_dt_filter)

        return session_qs.count()

        if not session_qs:
            session_qs = base_session_qs.filter(
                planned_call_dt__lte=timezone.now()
            )

        with transaction.atomic():
            session = session_qs.select_for_update().first()

            if not session:
                return None

            # Обновляем сессию (делаем ее 'активной')
            session.moderator = self.moderator
            session.planned_call_dt = None
            session.save()

            ModerationSessionLog.objects.create(
                moderator_id=self.moderator.id,
                customer=session.customer,
                log_type=ModerSessionLogTypeEnum.CONNECTED,
                comment='Модератор взял сессию в работу'
            )

        return session
