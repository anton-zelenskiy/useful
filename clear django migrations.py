# Временная функция
from project.utils.raw_sql import RawSQLManager
from project.settings.base.installed_apps import LOCAL_APPS
from copy import deepcopy


def clear_django_migrations_table():
    """Удаляет из django_migrations записи о всех локальных приложениях и
    о некоторых third-party и django приложениях.

    Откатить через python manage.py migrate --fake <app> zero
    не получается из-за сильной связности между приложениями.

    Новые миграции применяем командой python manage.py migrate --fake-initial
    """

    # Получаем список всех локальных приложений в строку
    local_apps = deepcopy(LOCAL_APPS)
    local_apps.remove('project')
    local_apps = [
        app.replace('project.apps.orders.', '').replace('project.apps.', '')
        for app in local_apps
    ]
    local_apps = ', '.join("'{0}'".format(app) for app in local_apps)

    # local apps
    sql_local = """DELETE FROM django_migrations WHERE app IN ({local_apps})"""
    sql_local = sql_local.format(local_apps=local_apps)
    print(sql_local)
    RawSQLManager(sql_query=sql_local).execute(without_result=True)

    # third-party и django миграциях (django вроде только на эти ругалась)
    sql_django_apps = """
    DELETE FROM django_migrations 
    WHERE app IN ('reversion', 'admin', 'authtoken')
    """
    print(sql_django_apps)
    RawSQLManager(sql_query=sql_django_apps).execute(without_result=True)
