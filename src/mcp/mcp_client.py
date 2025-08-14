"""
正确的MCP客户端实现
使用MultiServerMCPClient连接到SSE MCP服务
"""

import logging
from typing import List, Dict, Any, AsyncGenerator
from contextlib import asynccontextmanager
from langchain_mcp_adapters.client import MultiServerMCPClient
from src.config import get_config_manager

logger = logging.getLogger(__name__)

class MCPClientManager:
    """MCP客户端管理器"""
    
    def __init__(self):
        self.config_manager = get_config_manager()
        
    def get_mcp_servers_config(self) -> Dict[str, Dict[str, str]]:
        """获取MCP服务器配置"""
        servers_config = {}
        
        # 从配置中获取所有MCP服务器
        edw_config = self.config_manager.load_config()
        for name, server_config in edw_config.mcp_servers.items():
            servers_config[name] = {
                "url": server_config.url,
                "transport": server_config.transport,
            }
            
        return servers_config
    
    @asynccontextmanager
    async def get_mcp_client(self):
        """获取MCP客户端上下文管理器"""
        mcp_servers_config = self.get_mcp_servers_config()
        
        if not mcp_servers_config:
            logger.warning("未配置MCP服务器")
            yield None
            return
            
        client = None
        try:
            # langchain-mcp-adapters 0.1.0+ 不支持上下文管理器
            import asyncio
            client = MultiServerMCPClient(mcp_servers_config)
            
            # 使用wait_for来设置超时
            async def connect_with_timeout():
                logger.info(f"正在连接MCP服务器: {list(mcp_servers_config.keys())}")
                return client
                
            client = await asyncio.wait_for(connect_with_timeout(), timeout=10.0)
            yield client
        except Exception as e:
            logger.error(f"MCP客户端连接失败: {e}")
            yield None
        finally:
            # 清理客户端资源（如果有的话）
            if client and hasattr(client, 'close'):
                try:
                    await client.close()
                except:
                    pass
    
    @asynccontextmanager 
    async def get_mcp_tools(self) -> AsyncGenerator[List, None]:
        """获取MCP工具列表"""
        async with self.get_mcp_client() as client:
            if client:
                try:
                    tools = await client.get_tools()
                    logger.info(f"获取到 {len(tools)} 个MCP工具")
                    yield tools
                except Exception as e:
                    logger.error(f"获取MCP工具失败: {e}")
                    yield []
            else:
                yield []

# 全局MCP客户端管理器
_mcp_client_manager = None

def get_mcp_client_manager() -> MCPClientManager:
    """获取全局MCP客户端管理器"""
    global _mcp_client_manager
    if _mcp_client_manager is None:
        _mcp_client_manager = MCPClientManager()
    return _mcp_client_manager

@asynccontextmanager
async def get_mcp_client():
    """便捷函数：获取MCP客户端"""
    manager = get_mcp_client_manager()
    async with manager.get_mcp_client() as client:
        yield client

@asynccontextmanager        
async def get_mcp_tools():
    """便捷函数：获取MCP工具"""
    manager = get_mcp_client_manager()
    async with manager.get_mcp_tools() as tools:
        yield tools

# 为了向后兼容保留的函数
async def execute_sql_via_mcp(query: str, mode: str = "batch") -> str:
    """通过MCP执行SQL查询"""
    async with get_mcp_client() as client:
        if client:
            try:
                tools = await client.get_tools()
                # 查找SQL执行工具
                sql_tool = None
                for tool in tools:
                    if hasattr(tool, 'name') and 'execute_sql' in tool.name:
                        sql_tool = tool
                        break
                
                if sql_tool:
                    result = await sql_tool.ainvoke({"query": query, "mode": mode})
                    return str(result)
                else:
                    logger.error("未找到SQL执行工具")
                    return "错误: 未找到SQL执行工具"
            except Exception as e:
                logger.error(f"MCP SQL执行失败: {e}")
                return f"错误: {str(e)}"
        else:
            return "错误: MCP客户端未连接"