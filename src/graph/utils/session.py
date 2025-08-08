"""
会话管理工具
统一管理用户会话和线程ID
"""

import uuid
import hashlib
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