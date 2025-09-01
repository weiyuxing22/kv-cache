# tests/test_cache.py
import bisect

from kv.cache import Cache


def test_bisect_behavior():
    arr = [1, 2, 2, 2, 5, 7]
    assert bisect.bisect_right(arr, 2) == 4  # after last 2
    assert bisect.bisect_left(arr, 2) == 1  # before first 2


def test_cache_insert_and_get():
    c = Cache()
    c.insert(1, "a", "apple")
    c.insert(2, "a", "apricot")
    c.insert(5, "a", "avocado")
    assert c.get(1, "a") == "apple"
    assert c.get(4, "a") == "apricot"
    assert c.get(5, "a") == "avocado"
    assert c.get(3, "b") is None
