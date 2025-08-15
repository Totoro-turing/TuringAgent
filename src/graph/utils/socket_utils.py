"""
统一的Socket消息发送工具

提供统一接口，避免重复代码
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
from src.server.socket_manager import get_session_socket

logger = logging.getLogger(__name__)


def send_socket_message(
    session_id: str, 
    message_type: str, 
    data: Dict[str, Any]
) -> bool:
    """
    统一的socket消息发送函数
    
    Args:
        session_id: 会话ID
        message_type: 消息类型
        data: 消息数据
        
    Returns:
        bool: 是否发送成功
    """
    try:
        # 通过全局管理器获取socket队列
        socket_queue = get_session_socket(session_id)
        
        if socket_queue:
            # 添加时间戳（如果没有）
            if 'timestamp' not in data:
                data['timestamp'] = datetime.now().isoformat()
            
            socket_queue.send_message(session_id, message_type, data)
            logger.debug(f"✅ Socket消息发送成功: {message_type} -> {session_id[:8]}")
            return True
        else:
            logger.debug(f"⚠️ Socket队列不存在: {session_id[:8]}")
            return False
            
    except Exception as e:
        logger.warning(f"❌ Socket消息发送失败: {e}")
        return False


def send_node_progress_message(
    session_id: str,
    node: str,
    status: str,
    message: str,
    progress: float = 0.0,
    extra_data: Optional[Dict] = None
) -> bool:
    """
    发送节点进度消息
    
    Args:
        session_id: 会话ID
        node: 节点名称
        status: 状态（processing, completed, failed等）
        message: 消息内容
        progress: 进度值（0.0-1.0）
        extra_data: 额外数据
        
    Returns:
        bool: 是否发送成功
    """
    data = {
        "node": node,
        "status": status,
        "message": message,
        "progress": progress
    }
    
    if extra_data:
        data.update(extra_data)
    
    return send_socket_message(session_id, "node_progress", data)


def send_validation_progress_message(
    session_id: str,
    node: str,
    status: str,
    message: str,
    progress: float
) -> bool:
    """
    发送验证进度消息
    
    Args:
        session_id: 会话ID
        node: 节点名称
        status: 状态
        message: 消息内容
        progress: 进度值
        
    Returns:
        bool: 是否发送成功
    """
    data = {
        "node": node,
        "status": status,
        "message": message,
        "progress": progress
    }
    
    return send_socket_message(session_id, "validation_progress", data)


def send_workflow_event(
    session_id: str,
    event_type: str,
    message: str,
    extra_data: Optional[Dict] = None
) -> bool:
    """
    发送工作流事件
    
    Args:
        session_id: 会话ID
        event_type: 事件类型（workflow_start, workflow_complete, workflow_resume等）
        message: 消息内容
        extra_data: 额外数据
        
    Returns:
        bool: 是否发送成功
    """
    data = {
        "message": message
    }
    
    if extra_data:
        data.update(extra_data)
    
    return send_socket_message(session_id, event_type, data)


def send_code_display_message(
    session_id: str,
    table_name: str,
    source_code: str,
    **kwargs
) -> bool:
    """
    发送代码展示消息
    
    Args:
        session_id: 会话ID
        table_name: 表名
        source_code: 源代码
        **kwargs: 其他参数（branch_name, file_path等）
        
    Returns:
        bool: 是否发送成功
    """
    data = {
        "table_name": table_name,
        "source_code": source_code
    }
    
    data.update(kwargs)
    
    return send_socket_message(session_id, "original_code", data)