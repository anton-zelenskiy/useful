def time_exec(func):
    """ Декоратор, позволяющий определить время выплнения функции """
    import time

    def inner(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print('Time execution: {} seconds.'.format(end_time - start_time))

        return result

    return inner


def extract_digit_from_string(str, start_substr, end_substr):
    """Извлекает число из подстрок. """
    import re

    digit = re.search(
        r'{0}(\d+){1}'.format(start_substr, end_substr), str
    ).group(1)

    return digit


def test():
    """Тест id_sequence"""
    from django.db import transaction
    from project.apps.cold_callers.factories.leads import UserLeadFactory

    u = User.objects.last()

    Lead.objects.last().id  # id = 19
    l = UserLeadFactory(user=u)
    l = UserLeadFactory(user=u)  # id = 20

    # Пытаемся создать еще лида с таким user
    with transaction.atomic():
        l = UserLeadFactory(user=u)

    # и еще
    with transaction.atomic():
        l = UserLeadFactory(user=u)

    # Создаем без IntegrityError
    l = UserLeadFactory()  # id = 23



# Запуск скрипта в окнтейнере
docker run -d --name page_view_set_owner_id \
--env-file /etc/suplbiz-gunicorn.conf \
-v /tmp:/tmp \
--entrypoint=django-admin.py \
docker.supl.biz/suplbiz-gunicorn:7a20af12 \
runscript page_view_set_owner_id




# SQl RANK
from project.core.db.raw_sql import RawSQLManager
sql = """

WITH user_scores AS (
    SELECT U.id, SUM(US.score) as total_score
    FROM users_user as U
    LEFT JOIN scores_score as US
    ON (US.user_id = U.id)
    GROUP BY U.id
)

SELECT * from (
    SELECT users_user.id, US.total_score, rank() over(
        ORDER BY US.total_score DESC NULLS LAST
    ) as position
    FROM users_user
    INNER JOIN user_scores AS US
    ON US.id = users_user.id
) result
WHERE id = 1031096

"""
res = RawSQLManager(sql_query=sql).execute()



from django.db.models.expressions import Window
from django.db.models.functions import RowNumber, DenseRank
from django.db.models import F

results = User.objects.annotate(row_number=Window(
    expression=RowNumber(),
    order_by=F('date_joined').desc())
).order_by('row_number', 'name')


django_shell_detached() {
    export TAG="$(docker images docker.supl.biz/backend-monolith --format "{{.Tag}}" | head -1)"
docker run --rm -it --env-file /etc/backend_monolith.env -v /tmp:/tmp --entrypoint=django-admin.py docker.supl.biz/backend-monolith:$TAG shell_plus --quiet-load
}


class CharacterSeparatedField(serializers.ListField):
    """Поле для сериализации строки с разделитетем в список."""

    def __init__(self, *args, **kwargs):
        self.separator = kwargs.pop('separator', ',')
        super().__init__(*args, **kwargs)

    def get_value(self, dictionary):
        if html.is_html_input(dictionary):
            if self.field_name not in dictionary:
                if getattr(self.root, 'partial', False):
                    return empty
                return dictionary.get(self.field_name)

        return dictionary.get(self.field_name, empty)

    def to_internal_value(self, data):
        if not data:
            return []

        data = data.split(self.separator)
        return super().to_internal_value(data)

    def to_representation(self, data):
        data = super().to_representation(data)
        return self.separator.join(data)


def get_all_nested_objects(instance):
    """Возвращает все связанные с model instance сущности.
    Связь не обязательно прямая через FK, но и через связанные сущности
    например, если instance is Region, то в связанных сущностях окажутся User, !Notification!
    """
    from django.contrib.admin.utils import NestedObjects
    from django.conf import settings
    collector = NestedObjects(using=settings.DEFAULT_DB_ALIAS)
    collector.collect([instance])

    return collector.nested()


def flatten(items):
    from collections import Iterable
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x


def get_nested(region):
    nested = get_all_nested_objects(region)
    return list(flatten(nested))


def get_duplicate_regions():
    dupes = Region.objects.values('title').annotate(Count('id')).order_by().filter(id__count__gt=1)

    regions = Region.objects.filter(title__in=[item['title'] for item in dupes]).order_by('title')

    return regions
