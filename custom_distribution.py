from django.contrib.contenttypes.models import ContentType

from project.apps.notifications.channels import (
    EmailChannel,
)
from project.apps.notifications.models import (
    Activity,
    EmailStatusEnum,
    Notification,
)
from project.apps.regions.models import Region
from project.apps.users.models import User


def prepare_users():
    """Возвращает пользователей с казахским origin"""

    # Копипаст
    queryset = User.objects.all()
    not_send_to_paid_users = False
    not_send_customers = False

    if not_send_to_paid_users:
        queryset = User.objects.self_and_parent_not_on_main_tariff()

    queryset = queryset.annotate(
        offers_count=models.Count('offers')
    ).filter(
        models.Q(source='enticed') |
        models.Q(offers_count__gt=0) |
        ~models.Q(logo__exact='') |
        (~models.Q(company_profile__inn='') &
         ~models.Q(company_profile__inn=None)) |
        (~models.Q(company_profile__description='') &
         ~models.Q(company_profile__description=None)) |
        models.Q(login_count__gte=3)
    ).filter(is_active=True)

    user_ids = set(queryset.distinct().values_list('id', flat=True))

    exclude_users_id = None

    if not_send_customers:
        exclude_query = """
                SELECT orders.user_id
                FROM orders_order AS orders
                LEFT JOIN offers_offer AS offers ON
                  (offers.user_id = orders.user_id)
                LEFT JOIN proposals_proposal AS proposals ON
                  (proposals.user_id = orders.user_id)

                WHERE
                  offers.user_id IS NULL AND
                  proposals.user_id IS NULL
            """

        exclude_users_id = RawSQLManager(
            sql_query=exclude_query
        ).execute(flat=True)
        exclude_users_id = set(exclude_users_id)

    if exclude_users_id is not None:
        user_ids = user_ids.difference(exclude_users_id)

    # END Копипаст

    # Фильтр по регионам
    kz = Region.objects.get(title='Казахстан')
    users = User.objects.filter(
        id__in=user_ids,
        origin__tree_id=kz.tree_id
    )
    # END Фильтр по регионам

    return users

TEST
def prepare_users():
    return User.objects.filter(email__in=['oakrasikov@gmail.com'])


def send_distribution():
    """Запускает рассылка по указанным пользвоателям."""
    activity_type = 'specific_distribution'
    users_ct = ContentType.objects.get_by_natural_key('users', 'user')
    blame = User.objects.get(email='testsitesupl@mail.ru')

    activity = Activity.objects.create(
        verb=activity_type,
        actor_id=blame.id,
        object_type=users_ct,
        object_id=blame.id,
    )

    users = User.objects.filter(email__in=['oakrasikov@gmail.com'])

    for user in users:
        notification, created = Notification.objects.get_or_create(
            activity=activity,
            user=user,
            email_status=EmailStatusEnum.NOT_SENT
        )

        if not created:
            continue

    from project.apps.background_tasks import task_publisher
    task_publisher.send_task(
        'project.apps.notifications.tasks.send_distribution_activity',
        [activity.id]
    )
