from heapq import heappush, heappop
from itertools import count

class PriorityQueue:
    # Adapted from:
    # https://stackoverflow.com/questions/407734/a-generic-priority-queue-for-python
    _REMOVED = object()              # placeholder for a removed item

    def __init__(self):
        self._heap = []                       # heap of entries
        self._entry_finder = {}             # mapping of items to entries
        self._counter = count()   # unique sequence count

    def add(self, item, priority=0):
        'Add a new item or update the priority of an existing item'
        if item in self._entry_finder:
            self._remove_item(item)
        count = next(self._counter)
        entry = [priority, count, item]
        self._entry_finder[item] = entry
        heappush(self._heap, entry)

    def remove(self, item):
        'Mark an existing item as REMOVED.  Raise KeyError if not found.'
        entry = self._entry_finder.pop(item)
        entry[-1] = self._REMOVED

    def pop(self):
        'Remove and return the lowest priority item. Raise KeyError if empty.'
        if not self.empty():
            priority, count, item = heappop(self._heap)
            return item
        else:
            raise KeyError('pop from an empty priority queue')

    def peek(self):
        return self._heap[0][2] if not self.empty() else \
            KeyError('priority queue is empty')

    def empty(self):
        while self._heap and self._heap[0][2] is self._REMOVED:
            heappop(self._heap)
        return not self._heap


if __name__=='__main__':
    pq = PriorityQueue()
    pq.add('b', 2)
    pq.add('a', 1)
    pq.add('c', 3)
    pq.add('d', 4)
    pq.add('e', 5)
    pq.remove('b')
    pq.remove('d')
    print(pq.pop())
    print(pq.pop())
    print(pq.pop())
