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
    _store = {}
    _expiries = []
    _expiry_index = {}

    def __init__(self):
        raise NotImplementedError("Attempted to instantiate static class")

    @classmethod
    def add_to_list(cls, key, items, prepend=False):
        val = cls._store.get(key)
        if val is None:
            from .utils import QuickList
            val = Value(QuickList(), ValueType.LIST)
            cls.set(key, val)
        if val.type_ != ValueType.LIST:
            return None
        if prepend:
            for item in items:
                val.val.prepend(item)
        else:
            for item in items:
                val.val.append(item)
        return val.val.length

    @classmethod
    def set(cls, key, val, ttl=None):
        cls._store[key] = val
        if ttl:
            idx = len(cls._expiries)
            import time
            cls._expiries.append((key, time.time() + ttl))
            cls._expiry_index[key] = idx

    @classmethod
    def get(cls, key):
        idx = cls._expiry_index.get(key, None)
        import time
        if idx is not None:
            _, expiry = cls._expiries[idx]
            if expiry <= time.time():
                cls.delete(key)
                return None
        return cls._store.get(key, None)

    @classmethod
    def active_expire(cls, sample_size=100):
        if not cls._expiries:
            return
        import random
        import time
        random_idx = random.randrange(0, len(cls._expiries))
        now = time.time()
        to_delete = []
        for i in range(random_idx, min(random_idx + sample_size, len(cls._expiries))):
            key, expiry = cls._expiries[i]
            if  now >= expiry:
                to_delete.append(key)
        for key in to_delete:
            cls.delete(key)

    @classmethod
    def delete(cls, key):
        cls._store.pop(key, None)
        idx = cls._expiry_index.pop(key, None)
        if idx is None:
            return
        last_key, _ = cls._expiries[-1]
        if idx != len(cls._expiries) -1:
            cls._expiries[idx], cls._expiries[-1] = cls._expiries[-1], cls._expiries[idx]
            cls._expiry_index[last_key] = idx
        cls._expiries.pop()
