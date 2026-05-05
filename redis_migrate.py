import redis
from config import RedisCache

LOCAL_HOST = "127.0.0.1"
LOCAL_PORT = 6379
LOCAL_DB = 0
LOCAL_PASSWORD = None


def connect_local_redis():
    return redis.Redis(
        host=LOCAL_HOST,
        port=LOCAL_PORT,
        db=LOCAL_DB,
        password=LOCAL_PASSWORD,
        decode_responses=False,
    )


def copy_by_type(src, dst, key):
    t = src.type(key)

    if t == b"string":
        value = src.get(key)
        if value is None:
            return False
        ttl = src.ttl(key)
        if ttl and ttl > 0:
            dst.setex(key, ttl, value)
        else:
            dst.set(key, value)
        return True

    if t == b"hash":
        value = src.hgetall(key)
        if not value:
            return False
        dst.hset(key, mapping=value)
        ttl = src.ttl(key)
        if ttl and ttl > 0:
            dst.expire(key, ttl)
        return True

    if t == b"list":
        value = src.lrange(key, 0, -1)
        if not value:
            return False
        dst.delete(key)
        dst.rpush(key, *value)
        ttl = src.ttl(key)
        if ttl and ttl > 0:
            dst.expire(key, ttl)
        return True

    if t == b"set":
        value = src.smembers(key)
        if not value:
            return False
        dst.sadd(key, *value)
        ttl = src.ttl(key)
        if ttl and ttl > 0:
            dst.expire(key, ttl)
        return True

    if t == b"zset":
        value = src.zrange(key, 0, -1, withscores=True)
        if not value:
            return False
        mapping = {member: score for member, score in value}
        dst.zadd(key, mapping)
        ttl = src.ttl(key)
        if ttl and ttl > 0:
            dst.expire(key, ttl)
        return True

    print(f"Unhandled type for key {key!r}: {t!r}")
    return False


def sync_cloud_to_local():
    cloud = RedisCache()
    local = connect_local_redis()

    print("FLUSHING local Redis...")
    local.flushdb()
    print("Local Redis flushed.")

    cursor = 0
    total = 0
    dump_ok = 0
    fallback_ok = 0
    failed = 0

    while True:
        cursor, keys = cloud.scan(cursor=cursor, count=1000)

        for key in keys:
            total += 1
            try:
                payload = cloud.dump(key)
                if payload is not None:
                    ttl = cloud.pttl(key)
                    if ttl < 0:
                        ttl = 0
                    try:
                        local.restore(key, ttl, payload, replace=True)
                        dump_ok += 1
                        continue
                    except redis.ResponseError as e:
                        print(f"DUMP/RESTORE failed for {key!r}: {e}")

                if copy_by_type(cloud, local, key):
                    fallback_ok += 1
                else:
                    failed += 1

            except Exception as e:
                print(f"FAILED {key!r}: {e}")
                failed += 1

            if total % 1000 == 0:
                print(f"Processed {total} keys...")

        if cursor == 0:
            break

    print("Sync complete.")
    print(f"Total scanned: {total}")
    print(f"Restored via DUMP/RESTORE: {dump_ok}")
    print(f"Copied via fallback: {fallback_ok}")
    print(f"Failed: {failed}")
    print("Final local DBSIZE:", local.dbsize())
    print("Sample local keys:", local.keys("*")[:10])


if __name__ == "__main__":
    sync_cloud_to_local()