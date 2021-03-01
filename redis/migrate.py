from supl_shared.redis import RedisDBEnum

sandbox_redis_host = '10.100.0.4'
production_redis_host = '10.100.0.104'


def grouper(n, iterable):
    import itertools
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def migrate_redis_keys(source_db, dest_db, key_match):
    """Мигрирует ключи редиса в отдельную БД. """
    from supl_shared.redis import get_redis
    from redis.exceptions import ResponseError
    from tqdm import tqdm

    redis = get_redis(db=source_db, write=False)
    redis_write = get_redis(db=source_db)

    keys_iter = redis.scan_iter(match=key_match, count=10000)
    for keys in tqdm(grouper(100, keys_iter)):
        try:
            redis_write.migrate(
                host=sandbox_redis_host,
                port=6379,
                keys=keys,
                timeout=10,
                destination_db=dest_db,
                auth='7CO9kINfMjCIfDPEH7MMO7PCb9oLAKFX7D7aZEcfX6ftpF5iZJ',
                copy=True,
                replace=True,
            )
        except ResponseError:
            pass


def delete_old_redis_keys(source_db, key_match):
    """Удаляет ссылки на картинки со старой БД. """
    from supl_shared.redis import get_redis
    from redis.exceptions import ResponseError
    from tqdm import tqdm

    redis = get_redis(db=source_db)

    keys_iter = redis.scan_iter(match=key_match, count=100000)
    for keys in tqdm(grouper(100, keys_iter)):
        try:
            redis.delete(*keys)
        except ResponseError:
            pass


def migrate():
    # MONOLITH THUMBNAILS
    migrate_redis_keys(source_db=RedisDBEnum.MONOLITH, dest_db=RedisDBEnum.MONOLITH_THUMBNAILS, key_match='sorl-thumbnail*')
    delete_old_redis_keys(source_db=RedisDBEnum.MONOLITH, key_match='sorl-thumbnail*')


    # PROPOSAL THUMBNAILS
    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_THUMBNAILS, key_match='sorl-thumbnail*')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='sorl-thumbnail*')


    # PROPOSAL VIEWS
    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_VIEWS, key_match='proposals:*:views')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='proposals:*:views')

    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_VIEWS, key_match='proposals:*:views:authorized')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='proposals:*:views:authorized')

    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_VIEWS, key_match='suppliers:*:proposals:views')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='suppliers:*:proposals:views')


    # PROPOSAL AUTO_PLACEMENTS
    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_AUTO_PLACEMENTS, key_match='proposals:*:auto_placement')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='proposals:*:auto_placement')


    # PROPOSAL OPENING_CONTACTS
    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_OPENING_CONTACTS, key_match='proposals:*:opened_contacts')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='proposals:*:opened_contacts')

    migrate_redis_keys(source_db=RedisDBEnum.PROPOSALS, dest_db=RedisDBEnum.PROPOSAL_OPENING_CONTACTS, key_match='proposals:*:opened_contacts:authorized')
    delete_old_redis_keys(source_db=RedisDBEnum.PROPOSALS, key_match='proposals:*:opened_contacts:authorized')
