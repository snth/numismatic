from heapq import heappush, heappop
from itertools import count

class PriorityQueue:
    _REMOVED = object()              # placeholder for a removed item

    def __init__(self):
        self._pq = []                       # heap of entries
        self._entry_finder = {}             # mapping of items to entries
        self._counter = count()   # unique sequence count

    def add(self, item, priority=0):
        'Add a new item or update the priority of an existing item'
        if item in self._entry_finder:
            self._remove_item(item)
        count = next(self._counter)
        entry = [priority, count, item]
        self._entry_finder[item] = entry
        heappush(self._pq, entry)

    def remove(self, item):
        'Mark an existing item as REMOVED.  Raise KeyError if not found.'
        entry = self._entry_finder.pop(item)
        entry[-1] = self._REMOVED

    def pop(self):
        'Remove and return the lowest priority item. Raise KeyError if empty.'
        while self._pq:
            priority, count, item = heappop(self._pq)
            if item is not self._REMOVED:
                del self._entry_finder[item]
                return item
        raise KeyError('pop from an empty priority queue')



if __name__=='__main__':
    pq = PriorityQueue()
    pq.add('b', 2)
    pq.add('a', 1)
    pq.add('c', 3)
    print(pq.pop())
    print(pq.pop())
    print(pq.pop())
