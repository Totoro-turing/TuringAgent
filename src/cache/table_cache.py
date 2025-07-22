import asyncio
import time
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import threading

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """缓存条目"""
    data: Dict[str, Any]
    timestamp: float
    hits: int = 0
    last_accessed: float = field(default_factory=time.time)

@dataclass
class CacheStats:
    """缓存统计信息"""
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    
    @property
    def hit_rate(self) -> float:
        """缓存命中率"""
        if self.total_requests == 0:
            return 0.0
        return self.cache_hits / self.total_requests

class TableCacheManager:
    """表结构信息缓存管理器"""
    
    def __init__(self, 
                 ttl_seconds: int = 3600,  # 默认1小时TTL
                 max_entries: int = 1000,   # 最大缓存条目数
                 cleanup_interval: int = 300):  # 清理间隔5分钟
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.cleanup_interval = cleanup_interval
        
        self._cache: Dict[str, CacheEntry] = {}
        self._stats = CacheStats()
        self._lock = threading.RLock()  # 线程安全锁
        
        # 启动后台清理任务
        self._cleanup_task = None
        self._start_cleanup_task()
        
        logger.info(f"表缓存管理器初始化完成 - TTL: {ttl_seconds}s, 最大条目: {max_entries}")
    
    def _start_cleanup_task(self):
        """启动后台清理任务"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(self.cleanup_interval)
                    self._cleanup_expired_entries()
                except Exception as e:
                    logger.error(f"缓存清理任务异常: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("后台缓存清理任务已启动")
    
    def _generate_cache_key(self, table_name: str, operation: str = "fields") -> str:
        """生成缓存键"""
        return f"{operation}:{table_name.lower()}"
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """检查缓存条目是否过期"""
        return time.time() - entry.timestamp > self.ttl_seconds
    
    def _cleanup_expired_entries(self):
        """清理过期的缓存条目"""
        with self._lock:
            current_time = time.time()
            expired_keys = []
            
            for key, entry in self._cache.items():
                if current_time - entry.timestamp > self.ttl_seconds:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
            
            if expired_keys:
                logger.info(f"清理了 {len(expired_keys)} 个过期缓存条目")
            
            # 如果超出最大条目数，清理最少使用的条目
            if len(self._cache) > self.max_entries:
                # 按最后访问时间排序，删除最旧的条目
                sorted_items = sorted(
                    self._cache.items(), 
                    key=lambda x: x[1].last_accessed
                )
                
                excess_count = len(self._cache) - self.max_entries
                for i in range(excess_count):
                    key_to_remove = sorted_items[i][0]
                    del self._cache[key_to_remove]
                
                logger.info(f"清理了 {excess_count} 个最少使用的缓存条目")
    
    async def get_table_fields(self, table_name: str, fetch_func) -> Dict[str, Any]:
        """
        获取表字段信息，优先从缓存获取
        
        Args:
            table_name: 表名
            fetch_func: 获取数据的异步函数
        
        Returns:
            表字段信息字典
        """
        cache_key = self._generate_cache_key(table_name, "fields")
        
        with self._lock:
            self._stats.total_requests += 1
            
            # 检查缓存
            if cache_key in self._cache:
                entry = self._cache[cache_key]
                
                if not self._is_expired(entry):
                    # 缓存命中
                    entry.hits += 1
                    entry.last_accessed = time.time()
                    self._stats.cache_hits += 1
                    
                    logger.debug(f"缓存命中: {table_name} (命中次数: {entry.hits})")
                    return entry.data
                else:
                    # 缓存过期，删除
                    del self._cache[cache_key]
                    logger.debug(f"缓存过期: {table_name}")
        
        # 缓存未命中，获取数据
        logger.debug(f"缓存未命中: {table_name}，正在查询数据库...")
        try:
            data = await fetch_func(table_name)
            
            # 存入缓存
            with self._lock:
                self._cache[cache_key] = CacheEntry(
                    data=data,
                    timestamp=time.time(),
                    hits=0
                )
                self._stats.cache_misses += 1
                
            logger.debug(f"数据已缓存: {table_name}")
            return data
            
        except Exception as e:
            with self._lock:
                self._stats.cache_misses += 1
            logger.error(f"获取表字段信息失败: {table_name}, 错误: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        with self._lock:
            return {
                "cache_entries": len(self._cache),
                "total_requests": self._stats.total_requests,
                "cache_hits": self._stats.cache_hits,
                "cache_misses": self._stats.cache_misses,
                "hit_rate": f"{self._stats.hit_rate:.2%}",
                "ttl_seconds": self.ttl_seconds,
                "max_entries": self.max_entries,
                "memory_usage_estimate": len(self._cache) * 1024  # 粗略估算
            }
    
    def clear_cache(self, table_pattern: Optional[str] = None):
        """
        清理缓存
        
        Args:
            table_pattern: 表名模式，如果提供则只清理匹配的表，否则清理所有
        """
        with self._lock:
            if table_pattern is None:
                # 清理所有缓存
                cleared_count = len(self._cache)
                self._cache.clear()
                logger.info(f"清理了所有缓存，共 {cleared_count} 个条目")
            else:
                # 清理匹配模式的缓存
                keys_to_remove = []
                for key in self._cache.keys():
                    if table_pattern.lower() in key.lower():
                        keys_to_remove.append(key)
                
                for key in keys_to_remove:
                    del self._cache[key]
                
                logger.info(f"清理了匹配 '{table_pattern}' 的缓存，共 {len(keys_to_remove)} 个条目")
    
    def refresh_table(self, table_name: str, fetch_func):
        """
        强制刷新指定表的缓存
        
        Args:
            table_name: 表名
            fetch_func: 获取数据的异步函数
        """
        cache_key = self._generate_cache_key(table_name, "fields")
        
        with self._lock:
            # 删除现有缓存
            if cache_key in self._cache:
                del self._cache[cache_key]
                logger.info(f"删除表 {table_name} 的缓存")
        
        # 重新获取数据会自动缓存
        return self.get_table_fields(table_name, fetch_func)
    
    def get_cached_tables(self) -> List[str]:
        """获取所有已缓存的表名"""
        with self._lock:
            tables = []
            for key in self._cache.keys():
                if key.startswith("fields:"):
                    table_name = key.replace("fields:", "")
                    tables.append(table_name)
            return sorted(tables)
    
    def preload_tables(self, table_names: List[str], fetch_func):
        """
        预加载表信息到缓存
        
        Args:
            table_names: 要预加载的表名列表
            fetch_func: 获取数据的异步函数
        """
        async def _preload():
            tasks = []
            for table_name in table_names:
                task = self.get_table_fields(table_name, fetch_func)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            success_count = sum(1 for result in results if not isinstance(result, Exception))
            error_count = len(results) - success_count
            
            logger.info(f"预加载完成 - 成功: {success_count}, 失败: {error_count}")
            
        return asyncio.create_task(_preload())
    
    def __del__(self):
        """清理资源"""
        with self._lock:
            self._cache.clear()


# 全局缓存管理器实例
_global_cache_manager = None

def get_cache_manager() -> TableCacheManager:
    """获取全局缓存管理器实例"""
    global _global_cache_manager
    if _global_cache_manager is None:
        _global_cache_manager = TableCacheManager()
    return _global_cache_manager

def init_cache_manager(ttl_seconds: int = 3600, max_entries: int = 1000) -> TableCacheManager:
    """初始化缓存管理器"""
    global _global_cache_manager
    _global_cache_manager = TableCacheManager(ttl_seconds=ttl_seconds, max_entries=max_entries)
    return _global_cache_manager