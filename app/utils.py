class QuickListNode:
    def __init__(self):
        self.values = []
        self.prev = None
        self.next = None

class QuickList:
    def __init__(self, max_node_size = 5):
        self.max_node_size = max_node_size
        self.head: QuickListNode = None
        self.tail: QuickListNode = None
        self.length = 0

    def _append_new_node(self):
        node = QuickListNode()
        # First node case
        if self.head is None:
            self.head = self.tail = node
        # Add to end
        else:
            # Double link
            self.tail.next = node
            node.prev = self.tail
            # Move tail
            self.tail = node
        return node

    def _prepend_new_node(self):
        node = QuickListNode()
        # First node case
        if self.head is None:
            self.head = self.tail = node
        # Add to end
        else:
            # Double link
            self.head.prev = node
            node.next = self.head
            # Move tail
            self.head = node
        return node

    def lrange(self, st: int, end: int):
        if st < 0 or end >= self.length:
            return None
        idx = 0
        cur = self.head
        res = []
        while cur and idx <= end:
            for v in cur.values:
                if st <= idx <= end:
                    res.append(v)
                idx += 1
                if idx > end:
                    break
            cur = cur.next
        return res

    def pop(self):
        items = []
        return items
    
    def popleft(self, count=1):
        if self.head is None or len(self.head.values) == 0:
            return []
        items = []
        node = self.head
        while node and count > 0:
            n = min(len(node.values), count)
            items.extend(node.values[0:n])
            node.values = node.values[n:len(node.values)]
            count -= n
            if len(node.values) == 0:
                nxt = node.next
                node.next = None
                if nxt:
                    nxt.prev = None
                self.head = nxt
                node = nxt
        return items

    def append(self, val: str):
        if self.tail is None or len(self.tail.values) >= self.max_node_size:
            self._append_new_node()
        self.tail.values.append(val)
        self.length += 1
        return self.length

    def prepend(self, val: str):
        if self.head is None or len(self.head.values) >= self.max_node_size:
            self._prepend_new_node()
        self.head.values.append(val)
        self.length += 1
        return self.length
