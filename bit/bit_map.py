
# import numpy as np
# from bit_map import BitMap

from datetime import datetime, timedelta
import redis


def mark_request(key_preifix, redis_client):
    now = datetime.now()
    current_key = f"{key_preifix}:{now.strftime('%Y%m%d%H%M')}"
    offset = now.second
    redis_client.setbit(current_key, offset, 1)


def is_over_limit(redis_cli, key_prefix, limit) -> bool:
    now = datetime.now()
    current_key = f"{key_prefix}:{now.strftime("%Y%m%d%H%M")}"
    cur_count = redis_cli.bitcount(current_key)
    last_window = now - timedelta(minutes=1)
    last_key = f"{key_prefix}:{last_window.strftime("%Y%m%d%H%M")}"
    # last_count = 0
    if now.second < 59:
        last_count = redis_cli.bitcount(last_key,now.second+1,59)
    else:
        last_count = 0
    print(f"Current count: {cur_count}, Last count: {last_count}",now.second)
    return last_count + cur_count >= limit



def get_redis_conenction():
    return redis.Redis(host='localhost', port=6379,db=0)


if __name__ == "__main__":
    redis_client = get_redis_conenction()
    print(redis_client.ping())
    key_prefix = "request_limit"

    # Mark a request
    mark_request(key_prefix, redis_client)

    # Check if over limit
    limit = 10  # Example limit
    if is_over_limit(redis_client, key_prefix, limit):
        print("Request limit exceeded")
    else:
        print("Within request limit")