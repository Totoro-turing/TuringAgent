"""
通用进度发送工具
为所有节点提供统一的socket进度发送功能
"""

import logging
from typing import Optional, Any
from src.models.states import EDWState

logger = logging.getLogger(__name__)


def send_progress(
    state: EDWState, 
    node: str, 
    status: str, 
    message: str, 
    progress: float = 0.0,
    extra_data: Optional[dict] = None
):
    """
    通用的进度发送函数 - 通过socket发送实时进度到前端
    
    Args:
        state: EDW状态对象，包含socket_queue和session_id
        node: 节点名称，如 'model_enhance', 'github_push' 等
        status: 状态，如 'processing', 'completed', 'failed' 等
        message: 用户友好的进度描述信息
        progress: 进度百分比（0.0-1.0）
        extra_data: 可选的额外数据
    """
    socket_queue = state.get("socket_queue")
    session_id = state.get("session_id", "unknown")
    
    # 🎯 Socket直接发送（主要方案）
    if socket_queue:
        try:
            progress_data = {
                "node": node,
                "status": status,
                "message": message,
                "progress": progress,
                "timestamp": __import__('datetime').datetime.now().isoformat()
            }
            
            # 添加额外数据
            if extra_data:
                progress_data.update(extra_data)
            
            socket_queue.send_message(
                session_id,
                "node_progress",  # 统一的进度消息类型
                progress_data
            )
            logger.debug(f"✅ Socket进度发送成功: {node} - {status} - {message}")
        except Exception as e:
            logger.warning(f"Socket进度发送失败: {e}")
    else:
        logger.debug(f"Socket队列不存在，无法发送进度: {node} - {message}")


def send_node_start(state: EDWState, node: str, message: str = ""):
    """发送节点开始进度"""
    send_progress(state, node, "started", message or f"{node}节点开始处理")


def send_node_processing(state: EDWState, node: str, message: str, progress: float = 0.5):
    """发送节点处理中进度"""
    send_progress(state, node, "processing", message, progress)


def send_node_completed(state: EDWState, node: str, message: str = "", extra_data: Optional[dict] = None):
    """发送节点完成进度"""
    send_progress(state, node, "completed", message or f"{node}节点处理完成", 1.0, extra_data)


def send_node_failed(state: EDWState, node: str, error_message: str, extra_data: Optional[dict] = None):
    """发送节点失败进度"""
    send_progress(state, node, "failed", f"错误: {error_message}", 0.0, extra_data)


def send_node_skipped(state: EDWState, node: str, reason: str):
    """发送节点跳过进度"""
    send_progress(state, node, "skipped", f"跳过: {reason}", 1.0)


def send_progress_message(
    state: EDWState, 
    message_type: str, 
    data: dict
):
    """
    发送带特定类型的进度消息（工具监控专用）
    
    Args:
        state: EDW状态对象
        message_type: 消息类型（tool_progress, agent_decision, agent_complete等）
        data: 消息数据
    """
    socket_queue = state.get("socket_queue")
    session_id = state.get("session_id", "unknown")
    
    if socket_queue:
        try:
            # 添加时间戳
            data["timestamp"] = __import__('datetime').datetime.now().isoformat()
            
            socket_queue.send_message(
                session_id,
                message_type,
                data
            )
            logger.debug(f"✅ 工具监控消息发送成功: {message_type}")
        except Exception as e:
            logger.warning(f"工具监控消息发送失败: {e}")
    else:
        logger.debug(f"Socket队列不存在，无法发送工具监控消息: {message_type}")