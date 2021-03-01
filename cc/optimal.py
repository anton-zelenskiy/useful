WITH

-- реальные юзеры со звонками с панели пост-в за указанный период
with_supplier_panel_calls AS (
    SELECT DISTINCT supplier_leads.user_id AS id FROM supplier_leads

INNER JOIN (
    SELECT logs.lead_id FROM supplier_logs AS logs
WHERE logs.created_at >= '2019-10-25 10:47:23.318646+00:00'
) AS logs ON (logs.lead_id = supplier_leads.id)

WHERE supplier_leads.user_id IS NOT NULL
)
,

-- невалидные юзеры с запланированными звонками от продажников
with_sales_planned_calls AS (
    SELECT sales_planned_call.user_id AS id
FROM sales_planned_call
WHERE (
    planned_datetime > '2020-04-22 10:47:23.318646+00:00'
AND sales_planned_call.user_id IS NOT NULL
)
)
,

-- невалидные юзеры с запланированными звонками от повторных
with_ro_planned_calls AS (
    SELECT callerspanel_lead.user_id AS id
FROM callerspanel_lead
WHERE (
    callerspanel_lead.planned_call_dt > '2020-04-22 10:47:23.318646+00:00'
AND callerspanel_lead.crm_type = 1
)
)
,

-- невалидные юзеры, с кем продажники наговорили > N минут
-- за последние N месяцев
with_sales_call_duration as (
    SELECT sales_lead.user_id AS id
FROM action_on_sales_lead as asl
INNER JOIN sales_lead ON (asl.lead_id = sales_lead.id)
WHERE asl.processed_at > '2019-10-25 10:47:23.318646+00:00'
GROUP BY asl.id, sales_lead.user_id
HAVING SUM (asl.duration) > 5
)
,

-- невалидные юзеры со звонками с панели повторных
with_calls_from_ro_panel AS (
    SELECT lead.user_id AS id
FROM callerspanel_lead AS lead
INNER JOIN callerspanel_leadactionlog AS lead_logs ON (
    lead_logs.lead_id = lead.id
)
WHERE (
    lead.user_content_type_id = 12 AND
lead.crm_type = 1
) AND
lead_logs.created_at >= current_timestamp - interval '0' DAY
)
,

-- невалидные юзеры, которые создавали заказы за последнее время
with_orders AS (
    SELECT orders_order.user_id AS id FROM orders_order
WHERE
orders_order.actualized_at > '2019-10-25 10:47:23.318646+00:00'
AND orders_order.user_id IS NOT NULL
),


with_users AS (
    SELECT users.id, users.cold_lead_efficiency_coefficient FROM users_user AS users

                                                                               -- JOIN-ы на доп выборки

LEFT JOIN with_supplier_panel_calls ON
(users.id = with_supplier_panel_calls.id)

LEFT JOIN with_sales_planned_calls ON
(users.id = with_sales_planned_calls.id)

LEFT JOIN with_ro_planned_calls ON
(users.id = with_ro_planned_calls.id)

LEFT JOIN with_sales_call_duration ON
(users.id = with_sales_call_duration.id)

LEFT JOIN with_calls_from_ro_panel ON
(users.id = with_calls_from_ro_panel.id)

LEFT JOIN with_orders ON
(users.id = with_orders.id)

-- JOIN на origin
LEFT JOIN regions_region AS origin ON
(users.origin_id = origin.id)

LEFT JOIN marketing_sourceinfo AS source_info ON
(source_info.object_type_id = 12 AND users.id = source_info.object_id)

-- JOIN на company_profile
INNER JOIN users_companyprofile AS companyprofile ON
(users.company_profile_id = companyprofile.id)

WHERE

users.is_staff = FALSE
AND users.is_superuser = FALSE
AND	with_supplier_panel_calls.id IS NULL
AND with_sales_planned_calls.id IS NULL
AND with_ro_planned_calls.id IS NULL
AND with_sales_call_duration.id IS NULL
AND with_calls_from_ro_panel.id IS NULL
AND with_orders.id IS NULL

                      -- невалидные юзеры по дате логина или изменения источника
AND NOT (
    (source_info.source = 'cold_customer' AND
    source_info.activity_date < '2019-10-25 10:47:23.318646+00:00')
OR
(users.date_joined < '2019-10-25 10:47:23.318646+00:00' AND
source_info.source = 'cold_customer')
)

-- фильтр для того, чтобы не выдавать древние спарсенные неактуальные компании
AND users.last_login > '2016-01-01'

    -- исключаем в зависимости от настройки группы, у кого нет сайта,
                                                               AND ( true
OR (companyprofile.site IS NOT NULL AND companyprofile.site <> '')
)
AND (('true' AND origin.id IS NULL )
OR origin.tree_id = 17 )
AND ((
    -- Фильтр для пользователей с регионом
users.origin_id IS NOT NULL AND
origin.utc_offset=ANY(VALUES (1),(2),(3),(4),(5),(6),(-1))
) OR (
    -- Фильтр для пользователей без региона
'true' = 'true' AND users.origin_id IS NULL
)
) AND users.company_name ~* '(фабрика|завод|комбинат|производств|ооо|оао|зао)' AND NOT users.company_name ~* '(дверей|торгов|магазин|окна|потолки|потолков|двери|гастроном|супермаркет|гипермаркет|агенств|агентст|строящиеся|склад|теплиц|научно|филиал|коммерч|аренд|муп|гуп|монтаж|логистическая|сервис|муниципальное|фгку|тоо|управление|автостоянка|инженер|представитель|автомойка|управляющая|кадастр|касс|авторазбор|-тур|тураген|пто|осмотр|консалт|брокер|реклам|страхов|фсин|такси|красоты|авто|транспорт|ремонт|перевозк|приема|утилизирующая|утилизации|утилизация|обслуживан|служба|сельсовет|пекарня|пекарен|булочная|булочных|грузоперевоз|шоу-рум|студия мебели|дизайн-студия|лесничество|блинная|чебуречная|металлобаза|двутавр|переезд|okna|киоск|бутик|сток-центр|центр|меховой|меховая|РусАлка|Marazzi|Amway|Орифлейм|Тиккурила|газпром|роснефть|трансгаз|исправительное|колония|свадебный|салон|продаж|импортер|дилер|оценочно-экспертная)'
AND (users.phone IS NOT NULL AND users.phone <> '')
ORDER BY users.cold_lead_efficiency_coefficient DESC
)

SELECT leads.id, with_users.cold_lead_efficiency_coefficient FROM callerspanel_lead AS leads
INNER JOIN with_users ON (leads.id = with_users.id)

WHERE leads.crm_type = 0
AND leads.status = 999
AND leads.user_content_type_id = 12
LIMIT 20;



class PatchedLeadCCSelector(LeadCCSelector):
    def actualize_existing_leads_cache(self, status):
        """Наполняет кэш лидами из выборки при необходимости. """
        cache_key = self.existing_leads_redis_key.format(
            group_id=self.settings.id, status=status
        )
        if not self.redis.llen(cache_key):
            lead_ids = self._get_existing_lead_ids(status)
            print(status, len(lead_ids))
            if len(lead_ids) > 0:
                self.redis.rpush(cache_key, *lead_ids)
                self.redis.expire(cache_key, 60 * 15)


def test():
    for status in [
        LeadStatusEnum.PROCESSED,
        LeadStatusEnum.DENIED_BY_CALLER,
        LeadStatusEnum.DENIED_BY_EMPTY_CALLS_LIMIT,
        LeadStatusEnum.DENIED_BY_SAME_PHONE,
    ]:
        c = Caller.objects.get(user_id=1031096)
        h = PatchedLeadCCSelector(c)
        h.actualize_existing_leads_cache(status)
