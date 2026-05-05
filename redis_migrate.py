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


def sync_cloud_to_local():
    cloud = RedisCache()
    local = connect_local_redis()

    print("FLUSHING local Redis...")
    local.flushdb()
    print("Local Redis flushed.")

    print("Scanning cloud Redis keys...")
    cursor = 0
    count = 0

    while True:
        cursor, keys = cloud.scan(cursor=cursor, count=1000)
        if keys:
            pipe = local.pipeline(transaction=False)

            for key in keys:
                t = cloud.type(key)

                if t == b"string":
                    value = cloud.get(key)
                    ttl = cloud.ttl(key)
                    if ttl and ttl > 0:
                        pipe.setex(key, ttl, value)
                    else:
                        pipe.set(key, value)

                elif t == b"list":
                    value = cloud.lrange(key, 0, -1)
                    pipe.delete(key)
                    if value:
                        pipe.rpush(key, *value)
                    ttl = cloud.ttl(key)
                    if ttl and ttl > 0:
                        pipe.expire(key, ttl)

                elif t == b"set":
                    value = cloud.smembers(key)
                    pipe.delete(key)
                    if value:
                        pipe.sadd(key, *value)
                    ttl = cloud.ttl(key)
                    if ttl and ttl > 0:
                        pipe.expire(key, ttl)

                elif t == b"zset":
                    value = cloud.zrange(key, 0, -1, withscores=True)
                    pipe.delete(key)
                    if value:
                        mapping = {member: score for member, score in value}
                        pipe.zadd(key, mapping)
                    ttl = cloud.ttl(key)
                    if ttl and ttl > 0:
                        pipe.expire(key, ttl)

                elif t == b"hash":
                    value = cloud.hgetall(key)
                    pipe.delete(key)
                    if value:
                        pipe.hset(key, mapping=value)
                    ttl = cloud.ttl(key)
                    if ttl and ttl > 0:
                        pipe.expire(key, ttl)

                else:
                    print("Unhandled type for key", key, "type:", t)

                count += 1
                if count % 1000 == 0:
                    print(f"Synced {count} keys...")

            pipe.execute()

        if cursor == 0:
            break

    print(f"Sync complete. Copied {count} keys from cloud Redis to local Redis.")
    print("Local DBSIZE:", local.dbsize())


if __name__ == "__main__":
    sync_cloud_to_local()