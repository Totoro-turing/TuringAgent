"""
会话管理工具
统一管理用户会话和线程ID，支持工具调用监控
"""

import uuid
import hashlib
from typing import List, Optional
from src.config import get_config_manager

# 获取系统配置
config_manager = get_config_manager()
system_config = config_manager.get_system_config()


class SessionManager:
    """统一管理用户会话，特别是线程ID管理"""
    
    @staticmethod
    def generate_thread_id(user_id: str, agent_type: str = "default") -> str:
        """基于user_id和agent_type生成唯一的thread_id"""
        if not user_id or user_id.strip() == "":
            # 如果没有user_id，生成一个随机ID
            return str(uuid.uuid4())
        
        # 使用user_id和agent_type的组合生成thread_id，确保不同智能体的会话隔离
        combined_id = f"{user_id}_{agent_type}"
        thread_id_length = system_config.thread_id_length
        return hashlib.md5(combined_id.encode()).hexdigest()[:thread_id_length]
    
    @staticmethod
    def get_config(user_id: str = "", agent_type: str = "default") -> dict:
        """获取标准配置，不同agent_type的智能体会有独立的memory"""
        thread_id = SessionManager.generate_thread_id(user_id, agent_type)
        return {
            "configurable": {
                "thread_id": thread_id
            }
        }
    
    @staticmethod
    def get_config_with_monitor(
        user_id: str = "", 
        agent_type: str = "default",
        state=None,
        node_name: str = "unknown",
        enhanced_monitoring: bool = True
    ) -> dict:
        """
        获取带有工具监控回调的配置
        
        Args:
            user_id: 用户ID
            agent_type: Agent类型
            state: EDWState状态对象
            node_name: 节点名称，用于标识监控来源
            enhanced_monitoring: 是否使用增强监控
        
        Returns:
            包含监控回调的配置字典
        """
        # 获取基础配置
        base_config = SessionManager.get_config(user_id, agent_type)
        
        # 如果没有提供state，返回基础配置
        if state is None:
            return base_config
        
        # 导入工具监控器
        from src.graph.utils.tool_monitor import create_tool_monitor
        
        # 创建工具监控器
        tool_monitor = create_tool_monitor(
            state=state,
            node_name=node_name,
            agent_type=agent_type,
            enhanced=enhanced_monitoring
        )
        
        # 添加回调到配置
        config_with_monitor = base_config.copy()
        config_with_monitor["callbacks"] = [tool_monitor]
        
        return config_with_monitor