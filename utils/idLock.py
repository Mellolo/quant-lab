import threading
from typing import Dict

# 使用对象 ID 作为锁的标识
class IDLockManager:
    def __init__(self):
        self._locks: Dict[int, threading.Lock] = {}
        self._lock = threading.Lock()
    
    def _get_lock(self, obj_id) -> threading.Lock:
        if obj_id not in self._locks:
            self._lock.acquire()
            if obj_id not in self._locks:
                self._locks[obj_id] = threading.Lock()
        return self._locks[obj_id]

    def acquire_lock(self, obj_id):
        return self._get_lock(obj_id).acquire()

    def release_lock(self, obj_id):
        return self._get_lock(obj_id).release()
