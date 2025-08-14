"""
基础异步工具类

提供异步工具的基类和通用功能
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type
from langchain.tools import BaseTool
from langchain_core.callbacks import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun
)
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AsyncBaseTool(BaseTool, ABC):
    """
    异步工具基类
    
    继承此类创建异步工具，实现_arun方法即可
    """
    
    @abstractmethod
    async def _arun(
        self,
        *args,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
        **kwargs
    ) -> Any:
        """
        异步执行工具
        
        子类必须实现此方法
        """
        pass
    
    def _run(
        self,
        *args,
        run_manager: Optional[CallbackManagerForToolRun] = None,
        **kwargs
    ) -> Any:
        """
        同步执行（自动调用异步版本）
        
        如果需要同步调用，框架会自动使用此方法
        """
        try:
            # 检查是否已经在事件循环中
            try:
                loop = asyncio.get_running_loop()
                # 如果已经在运行的循环中，使用线程池执行异步代码
                logger.debug(f"在已有事件循环中同步执行异步工具: {self.name}")
                
                import threading
                import concurrent.futures
                
                # 创建一个新线程来运行异步代码
                def run_async():
                    return asyncio.run(self._arun(*args, run_manager=None, **kwargs))
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async)
                    return future.result(timeout=60)  # 60秒超时
                    
            except RuntimeError:
                # 没有运行中的事件循环，直接运行
                logger.debug(f"在新事件循环中执行异步工具: {self.name}")
                return asyncio.run(self._arun(*args, run_manager=None, **kwargs))
                
        except Exception as e:
            error_msg = f"工具 {self.name} 同步调用失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "result": None
            }


class ToolResult(BaseModel):
    """
    工具执行结果的标准格式
    """
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = {}
    
    class Config:
        arbitrary_types_allowed = True


def create_tool_result(
    success: bool,
    result: Any = None,
    error: str = None,
    **metadata
) -> Dict[str, Any]:
    """
    创建标准化的工具结果
    
    Args:
        success: 是否成功
        result: 执行结果
        error: 错误信息
        **metadata: 额外的元数据
    
    Returns:
        标准化的结果字典
    """
    return {
        "success": success,
        "result": result,
        "error": error,
        "metadata": metadata
    }


async def run_with_timeout(
    coroutine,
    timeout: float = 30.0,
    timeout_message: str = "操作超时"
) -> Any:
    """
    带超时的异步执行
    
    Args:
        coroutine: 要执行的协程
        timeout: 超时时间（秒）
        timeout_message: 超时错误信息
    
    Returns:
        协程执行结果
    
    Raises:
        TimeoutError: 执行超时
    """
    try:
        return await asyncio.wait_for(coroutine, timeout=timeout)
    except asyncio.TimeoutError:
        logger.error(f"{timeout_message} (超时: {timeout}秒)")
        raise TimeoutError(timeout_message)


class RetryableAsyncTool(AsyncBaseTool):
    """
    支持重试的异步工具基类
    """
    
    max_retries: int = 3
    retry_delay: float = 1.0
    
    async def _arun_with_retry(
        self,
        *args,
        **kwargs
    ) -> Any:
        """
        带重试机制的异步执行
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                return await self._arun_internal(*args, **kwargs)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"{self.name} 执行失败 (尝试 {attempt + 1}/{self.max_retries}): {e}"
                    )
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"{self.name} 执行失败，已达最大重试次数: {e}")
        
        raise last_error
    
    async def _arun(
        self,
        *args,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
        **kwargs
    ) -> Any:
        """
        覆盖_arun以添加重试逻辑
        """
        return await self._arun_with_retry(*args, **kwargs)
    
    @abstractmethod
    async def _arun_internal(self, *args, **kwargs) -> Any:
        """
        实际的异步执行逻辑（子类实现）
        """
        pass