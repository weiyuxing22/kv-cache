# WHY: Ensure pytest adds project root to import path so `from kv.cache import Cache` works,
# regardless of how pytest is invoked or what the CWD is.
import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
