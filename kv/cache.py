# kv/cache.py
from __future__ import annotations

import bisect
from collections import defaultdict
from collections.abc import Iterable
from typing import Any


class Cache:
    """
    Time-versioned key-value store using parallel arrays per key.

    Invariant per key: timestamps are kept in non-decreasing order.

    API:
      - insert(ts, key, value): add or overwrite a version (supports out-of-order)
      - insert_many(entries): batch insert of (ts, key, value)
      - get(ts, key): value at the largest timestamp <= ts, else None
      - latest(key): most recent value for a key, else None
      - get_range(key, start_ts, end_ts): list[(ts, value)] within [start_ts, end_ts]
      - delete_upto(key, ts): delete all versions with timestamp <= ts
      - compact(key, keep_last_n=None, dedup_same_value=True): simple compaction
    """

    def __init__(self) -> None:
        self._ts: dict[str, list[int]] = defaultdict(list)
        self._vals: dict[str, list[Any]] = defaultdict(list)

    # ----------------------
    # Core write paths
    # ----------------------
    def insert(self, timestamp: int, key: str, value: Any) -> None:
        """Insert a version; overwrite if the same timestamp already exists for key.
        Supports out-of-order inserts efficiently via bisect.
        """
        ts_arr = self._ts[key]
        val_arr = self._vals[key]

        # Fast append path when timestamps are monotonic per key
        if not ts_arr or timestamp >= ts_arr[-1]:
            if ts_arr and timestamp == ts_arr[-1]:
                # overwrite same timestamp
                val_arr[-1] = value
            else:
                ts_arr.append(timestamp)
                val_arr.append(value)
            return

        # Out-of-order: position of first element > timestamp
        i = bisect.bisect_right(ts_arr, timestamp)
        # If exact ts exists at i-1, overwrite; else insert aligned
        if i > 0 and ts_arr[i - 1] == timestamp:
            val_arr[i - 1] = value
        else:
            ts_arr.insert(i, timestamp)
            val_arr.insert(i, value)

    def insert_many(self, entries: Iterable[tuple[int, str, Any]]) -> None:
        """Batch insert entries of the form (timestamp, key, value)."""
        buckets: dict[str, list[tuple[int, Any]]] = defaultdict(list)
        for ts, k, v in entries:
            buckets[k].append((ts, v))
        for k, items in buckets.items():
            items.sort(key=lambda x: x[0])  # by timestamp
            ts_arr = self._ts[k]
            val_arr = self._vals[k]
            for ts, v in items:
                if not ts_arr or ts >= ts_arr[-1]:
                    if ts_arr and ts == ts_arr[-1]:
                        val_arr[-1] = v
                    else:
                        ts_arr.append(ts)
                        val_arr.append(v)
                else:
                    i = bisect.bisect_right(ts_arr, ts)
                    if i > 0 and ts_arr[i - 1] == ts:
                        val_arr[i - 1] = v
                    else:
                        ts_arr.insert(i, ts)
                        val_arr.insert(i, v)

    # ----------------------
    # Reads
    # ----------------------
    def get(self, timestamp: int, key: str) -> Any | None:
        ts_arr = self._ts.get(key)
        if not ts_arr:
            return None
        i = bisect.bisect_right(ts_arr, timestamp) - 1
        if i < 0:
            return None
        return self._vals[key][i]

    def latest(self, key: str) -> Any | None:
        ts_arr = self._ts.get(key)
        if not ts_arr:
            return None
        return self._vals[key][-1]

    def get_range(self, key: str, start_ts: int, end_ts: int) -> list[tuple[int, Any]]:
        if start_ts > end_ts:
            return []
        ts_arr = self._ts.get(key)
        if not ts_arr:
            return []
        left = bisect.bisect_left(ts_arr, start_ts)
        right = bisect.bisect_right(ts_arr, end_ts)
        vals = self._vals[key]
        return [(ts_arr[i], vals[i]) for i in range(left, right)]

    # ----------------------
    # Maintenance helpers
    # ----------------------
    def delete_upto(self, key: str, ts: int) -> int:
        ts_arr = self._ts.get(key)
        if not ts_arr:
            return 0
        cut = bisect.bisect_right(ts_arr, ts)
        if cut <= 0:
            return 0
        del ts_arr[:cut]
        del self._vals[key][:cut]
        return cut

    def compact(
        self, key: str, keep_last_n: int | None = None, dedup_same_value: bool = True
    ) -> None:
        ts_arr = self._ts.get(key)
        val_arr = self._vals.get(key)
        if not ts_arr:
            return
        # Keep only last N versions
        if keep_last_n is not None and keep_last_n >= 0 and len(ts_arr) > keep_last_n:
            start = len(ts_arr) - keep_last_n
            del ts_arr[:start]
            del val_arr[:start]
        # Dedup adjacent identical values
        if dedup_same_value and ts_arr:
            new_ts: list[int] = [ts_arr[0]]
            new_vals: list[Any] = [val_arr[0]]
            for i in range(1, len(ts_arr)):
                if val_arr[i] != new_vals[-1]:
                    new_ts.append(ts_arr[i])
                    new_vals.append(val_arr[i])
            self._ts[key] = new_ts
            self._vals[key] = new_vals


if __name__ == "__main__":
    c = Cache()
    c.insert(1, "a", "apple")
    c.insert(2, "a", "apricot")
    c.insert(5, "a", "avocado")
    c.insert(5, "a", "avocado2")  # overwrite same ts
    c.insert(4, "a", "anchovy")  # out-of-order in the middle

    assert c._ts["a"] == [1, 2, 4, 5]
    assert c._vals["a"] == ["apple", "apricot", "anchovy", "avocado2"]
    assert c.get(0, "a") is None
    assert c.get(1, "a") == "apple"
    assert c.get(3, "a") == "apricot"
    assert c.get(4, "a") == "anchovy"
    assert c.get(999, "a") == "avocado2"
    assert c.latest("a") == "avocado2"
    assert c.get_range("a", 2, 4) == [(2, "apricot"), (4, "anchovy")]
    assert c.delete_upto("a", 2) == 2
    assert c._ts["a"] == [4, 5]
    assert c._vals["a"] == ["anchovy", "avocado2"]
    c.compact("a", keep_last_n=1)
    assert c._ts["a"] == [5]
    assert c._vals["a"] == ["avocado2"]
    print("Sanity OK")
