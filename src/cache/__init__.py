"""
缓存管理模块

提供表结构信息的智能缓存功能，包括：
- TTL（时间生存）机制
- 自动过期清理
- 缓存命中率统计
- 多线程安全
"""

from .table_cache import TableCacheManager, get_cache_manager, init_cache_manager

__all__ = ['TableCacheManager', 'get_cache_manager', 'init_cache_manager']