"""
Redis migration script: copies all keys from SRC to DST, preserving types and TTLs.
Run locally before switching REDISCLOUD_URL to the new instance.
"""

import redis
import sys
import time

SRC_URL = "redis://default:hFRMneFKxJRoQC9TH7ghLiiEjq0rYsui@redis-18175.c2.eu-west-1-3.ec2.cloud.redislabs.com:18175"
DST_URL = "redis://default:t80D3eeREn1GIaZJQZTqaxVvF69rw6OG@redis-17340.c6.eu-west-1-1.ec2.cloud.redislabs.com:17340"

SCAN_COUNT = 200   # keys per SCAN batch
DRY_RUN    = "--dry-run" in sys.argv


def connect(url: str, label: str) -> redis.Redis:
    r = redis.from_url(url, decode_responses=False, socket_connect_timeout=10, socket_timeout=15)
    r.ping()
    print(f"✓ Connected to {label}")
    return r


def copy_key(src: redis.Redis, dst: redis.Redis, key: bytes) -> bool:
    try:
        ttl     = src.pttl(key)   # milliseconds; -1 = no expiry, -2 = gone
        ktype   = src.type(key).decode()

        if ktype == "string":
            val = src.get(key)
            if not DRY_RUN:
                dst.set(key, val)

        elif ktype == "hash":
            val = src.hgetall(key)
            if not DRY_RUN and val:
                dst.hset(key, mapping=val)

        elif ktype == "list":
            val = src.lrange(key, 0, -1)
            if not DRY_RUN and val:
                dst.delete(key)
                dst.rpush(key, *val)

        elif ktype == "set":
            val = src.smembers(key)
            if not DRY_RUN and val:
                dst.delete(key)
                dst.sadd(key, *val)

        elif ktype == "zset":
            val = src.zrange(key, 0, -1, withscores=True)
            if not DRY_RUN and val:
                dst.delete(key)
                dst.zadd(key, {m: s for m, s in val})

        else:
            print(f"  ⚠  Unknown type '{ktype}' for key {key!r} — skipped")
            return False

        # Restore TTL (skip if no expiry or key already expired)
        if not DRY_RUN and ttl > 0:
            dst.pexpire(key, ttl)

        return True

    except Exception as e:
        print(f"  ✗ Error on key {key!r}: {e}")
        return False


def main():
    print("=" * 60)
    print(f"Redis migration {'(DRY RUN — no writes)' if DRY_RUN else ''}")
    print("=" * 60)

    src = connect(SRC_URL, "SOURCE (old)")
    dst = connect(DST_URL, "DEST   (new)")

    src_total = src.dbsize()
    print(f"\nSource key count: {src_total}")
    print(f"Dest   key count: {dst.dbsize()} (before migration)\n")

    if src_total == 0:
        print("Nothing to migrate.")
        return

    copied = skipped = errors = 0
    cursor = 0
    start  = time.time()

    while True:
        cursor, keys = src.scan(cursor=cursor, count=SCAN_COUNT)
        for key in keys:
            ok = copy_key(src, dst, key)
            if ok:
                copied += 1
            else:
                errors += 1

            total_done = copied + errors
            if total_done % 100 == 0:
                pct = total_done / src_total * 100
                elapsed = time.time() - start
                rate = total_done / elapsed if elapsed > 0 else 0
                print(f"  {total_done}/{src_total} ({pct:.0f}%)  "
                      f"{rate:.0f} keys/s  errors={errors}", end="\r")

        if cursor == 0:
            break

    elapsed = time.time() - start
    print(f"\n\n{'=' * 60}")
    print(f"Done in {elapsed:.1f}s")
    print(f"  Copied : {copied}")
    print(f"  Errors : {errors}")
    print(f"{'=' * 60}")

    # ── Verification ────────────────────────────────────────────────
    print("\nVerification:")
    src_count = src.dbsize()
    dst_count = dst.dbsize()
    print(f"  Source keys : {src_count}")
    print(f"  Dest   keys : {dst_count}")

    if not DRY_RUN:
        if dst_count >= src_count:
            print("\n✅ Migration successful — dest has all keys.")
            print("   You can now update REDISCLOUD_URL to the new instance.")
        else:
            missing = src_count - dst_count
            print(f"\n⚠  {missing} keys missing in dest — check errors above before switching.")
    else:
        print("\n(Dry-run complete — re-run without --dry-run to write data)")


if __name__ == "__main__":
    main()
