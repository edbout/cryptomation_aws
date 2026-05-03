import time
from config import RedisCache
import redis


def connect_local_redis():
    # Adjust host/port/password if yours is different
    return redis.Redis(
        host="127.0.0.1",
        port=6379,
        db=0,
        password=None,
        decode_responses=False,  # Keep bytes for exact copy
    )


def sync_cloud_to_local():
    cloud = RedisCache()  # your cloud Redis
    local = connect_local_redis()

    print("FLUSHING local Redis...")
    local.flushdb()  # clear local before copy
    print("Local Redis flushed.")

    print("Scanning cloud Redis keys...")
    count = 0
    cursor = 0

    while True:
        cursor, keys = cloud.scan(cursor=cursor, count=1000)
        if not keys:
            break

        pipe = local.pipeline(transaction=False)

        for key in keys:
            # Get type and value
            t = cloud.type(key)
            if t == b"string":
                value = cloud.get(key)
                ttl = cloud.ttl(key)
                if ttl > 0:
                    pipe.setex(key, ttl, value)
                else:
                    pipe.set(key, value)
            elif t == b"list":
                value = cloud.lrange(key, 0, -1)
                pipe.delete(key)
                if value:
                    pipe.rpush(key, *value)
                ttl = cloud.ttl(key)
                if ttl > 0:
                    pipe.expire(key, ttl)

            elif t == b"set":
                value = cloud.smembers(key)
                pipe.delete(key)
                if value:
                    pipe.sadd(key, *value)
                ttl = cloud.ttl(key)
                if ttl > 0:
                    pipe.expire(key, ttl)

            elif t == b"zset":
                # zset: list of (member, score)
                value = cloud.zrange(key, 0, -1, withscores=True)
                pipe.delete(key)
                if value:
                    for member, score in value:
                        pipe.zadd(key, {member: score})
                ttl = cloud.ttl(key)
                if ttl > 0:
                    pipe.expire(key, ttl)

            elif t == b"hash":
                value = cloud.hgetall(key)
                pipe.delete(key)
                if value:
                    pipe.hset(key, mapping=value)
                ttl = cloud.ttl(key)
                if ttl > 0:
                    pipe.expire(key, ttl)

            elif t in (b"none", b"stream"):
                # ignore deleted or stream types for now
                pass
            else:
                print("Unhandled type for key", key, "type:", t)

            count += 1
            if count % 1000 == 0:
                print(f"Synced {count} keys...")

        pipe.execute()

        if cursor == 0:
            break

    print(f"Sync complete. Copied {count} keys from cloud Redis to local Redis.")


if __name__ == "__main__":
    sync_cloud_to_local()