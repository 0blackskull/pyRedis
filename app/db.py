db = {}

def set(key: str, val: str):
    db[key] = val

def get(key: str):
    return db.get(key, None)