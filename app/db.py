class ValueType:
    STRING = 0
    LIST = 1
    SET = 2
    ZSET = 3
    HASH = 4

class Value:
    def __init__(self, val, type_):
        self.val = val
        self.type_ = type_

class DB:
    def __init__(self):
        self._store = {}
        self._expiries = []
        self._expiry_index = {}

    def add_to_list(self, key, items, prepend=False):
        val = self._store.get(key)
        if val is None:
            from .utils import QuickList
            val = Value(QuickList(), ValueType.LIST)
            self.set(key, val)
        if val.type_ != ValueType.LIST:
            return None
        if prepend:
            for item in items:
                val.val.prepend(item)
        else:
            for item in items:
                val.val.append(item)
        return val.val.length

    def set(self, key, val, ttl=None):
        self._store[key] = val
        if ttl:
            idx = len(self._expiries)
            import time
            self._expiries.append((key, time.time() + ttl))
            self._expiry_index[key] = idx

    def get(self, key):
        idx = self._expiry_index.get(key, None)
        import time
        if idx is not None:
            _, expiry = self._expiries[idx]
            if expiry <= time.time():
                self.delete(key)
                return None
        return self._store.get(key, None)

    def active_expire(self, sample_size=100):
        if not self._expiries:
            return
        import random
        import time
        random_idx = random.randrange(0, len(self._expiries))
        now = time.time()
        to_delete = []
        for i in range(random_idx, min(random_idx + sample_size, len(self._expiries))):
            key, expiry = self._expiries[i]
            if  now >= expiry:
                to_delete.append(key)
        for key in to_delete:
            self.delete(key)

    def delete(self, key):
        self._store.pop(key, None)
        idx = self._expiry_index.pop(key, None)
        if idx is None:
            return
        last_key, _ = self._expiries[-1]
        if idx != len(self._expiries) -1:
            self._expiries[idx], self._expiries[-1] = self._expiries[-1], self._expiries[idx]
            self._expiry_index[last_key] = idx
        self._expiries.pop()
