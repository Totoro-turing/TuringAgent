"""
Socket队列注册表

解决SocketIOMessageSender无法序列化的问题
通过session_id查找对应的socket队列实例
"""

import logging
from typing import Optional, Dict
import threading

logger = logging.getLogger(__name__)


class SocketQueueRegistry:
    """Socket队列注册表，支持通过session_id查找socket_queue"""
    
    def __init__(self):
        self._socket_queues: Dict[str, any] = {}  # session_id -> SocketIOMessageSender
        self._lock = threading.Lock()
    
    def register_socket_queue(self, session_id: str, socket_queue) -> None:
        """注册session_id对应的socket队列"""
        with self._lock:
            self._socket_queues[session_id] = socket_queue
            logger.debug(f"🔗 注册Socket队列: {session_id[:8]}")
    
    def get_socket_queue(self, session_id: str):
        """通过session_id获取socket队列"""
        with self._lock:
            return self._socket_queues.get(session_id)
    
    def unregister_socket_queue(self, session_id: str) -> None:
        """注销session_id对应的socket队列"""
        with self._lock:
            if session_id in self._socket_queues:
                del self._socket_queues[session_id]
                logger.debug(f"🔌 注销Socket队列: {session_id[:8]}")
    
    def clear_all(self) -> None:
        """清除所有socket队列"""
        with self._lock:
            self._socket_queues.clear()
            logger.info("🧹 清除所有Socket队列")
    
    def get_active_sessions(self) -> list:
        """获取所有活跃的session_id"""
        with self._lock:
            return list(self._socket_queues.keys())


# 全局实例
_socket_queue_registry = SocketQueueRegistry()


def get_socket_queue_registry() -> SocketQueueRegistry:
    """获取Socket队列注册表实例"""
    return _socket_queue_registry


def register_session_socket(session_id: str, socket_queue) -> None:
    """便捷函数：注册session的socket队列"""
    _socket_queue_registry.register_socket_queue(session_id, socket_queue)


def get_session_socket(session_id: str):
    """便捷函数：获取session的socket队列"""
    return _socket_queue_registry.get_socket_queue(session_id)


def unregister_session_socket(session_id: str) -> None:
    """便捷函数：注销session的socket队列"""
    _socket_queue_registry.unregister_socket_queue(session_id)