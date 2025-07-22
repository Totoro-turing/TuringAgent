"""
MCP (Model Context Protocol) 客户端模块
使用 MultiServerMCPClient 连接到SSE MCP服务
"""

import logging

logger = logging.getLogger(__name__)

# 导出主要的客户端类和函数
from .mcp_client import (
    MCPClientManager,
    get_mcp_client_manager,
    get_mcp_client,
    get_mcp_tools,
    execute_sql_via_mcp
)

__all__ = [
    'MCPClientManager',
    'get_mcp_client_manager',
    'get_mcp_client',
    'get_mcp_tools',
    'execute_sql_via_mcp'
]