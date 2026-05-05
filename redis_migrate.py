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

    cursor = 0
    count = 0

    while True:
        cursor, keys = cloud.scan(cursor=cursor, count=1000)

        if keys:
            pipe = local.pipeline(transaction=False)

            for key in keys:
                try:
                    dump = cloud.dump(key)
                    if dump is None:
                        print(f"SKIP {key!r}: dump returned None")
                        continue

                    ttl = cloud.pttl(key)
                    if ttl is None or ttl < 0:
                        ttl = 0

                    pipe.restore(key, ttl, dump, replace=True)
                    count += 1

                    if count % 1000 == 0:
                        print(f"Synced {count} keys...")

                except Exception as e:
                    print(f"FAILED {key!r}: {e}")

            pipe.execute()

        if cursor == 0:
            break

    print(f"Sync complete. Copied {count} keys from cloud Redis to local Redis.")
    print("Final local DBSIZE:", local.dbsize())
    print("Sample local keys:", local.keys("*")[:10])


if __name__ == "__main__":
    sync_cloud_to_local()