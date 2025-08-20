"""
导航和分类节点
负责任务分类和基础对话处理
"""

import logging
from langchain.prompts import PromptTemplate
from langchain.schema.messages import HumanMessage, AIMessage

from src.models.states import EDWState
from src.agent.edw_agents import (
    get_navigation_agent,
    get_chat_agent,
    get_shared_llm
)
from src.config import get_config_manager
from src.graph.utils.session import SessionManager
from src.server.socket_manager import get_session_socket

logger = logging.getLogger(__name__)

# 获取配置管理器
config_manager = get_config_manager()

# 获取共享的agents
llm_agent = get_navigation_agent()
chat_agent = get_chat_agent()


async def navigate_node(state: EDWState):
    """导航节点：负责用户输入的初始分类"""
    
    # 如果已经有type且不为空，且不是None，且不是'other'，直接返回
    task_type = state.get('type')
    if task_type and task_type != 'other':
        return {"type": task_type, "user_id": state.get("user_id", "")}
    
    prompt_template = config_manager.get_prompt("navigation_prompt")
    prompt = PromptTemplate.from_template(prompt_template)



    try:
        # 使用带监控的配置管理器 - 导航智能体独立memory
        config = SessionManager.get_config_with_monitor(
            user_id=state.get("user_id", ""),
            agent_type="navigation",
            state=state,
            node_name="navigation",
            enhanced_monitoring=True
        )
        session_id = state.get("session_id", "unknown")

    # 🎯 通过全局socket管理器获取socket队列
        socket_queue = get_session_socket(session_id)

    # 🎯 Socket直接发送（主要方案）
        socket_queue.send_message(
            session_id,
            "validation_progress",
            {
                "node": "navigate_node",
                "status": "processing",
                "message": "正在分析您的需求",
                "progress": 0.1
            }
        )
        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)
        
        response = await llm_agent.ainvoke(
            {"messages": [{"role": "user", "content": prompt.format(input=content)}]},
            config
        )
        
        classification = response["messages"][-1].content.strip().lower()
        logger.info(f"Navigation classification: {classification}")
        
        if "function" in classification:
            return {"type": "function", "user_id": state.get("user_id", "")}
        elif "other" in classification:
            return {"type": "other", "user_id": state.get("user_id", "")}
        else:
            return {"type": "model_dev", "user_id": state.get("user_id", "")}
    except Exception as e:
        error_msg = f"导航节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}


def chat_node(state: EDWState):
    """聊天节点：处理普通对话"""
    try:
        # 使用带监控的配置管理器 - 聊天智能体独立memory
        config = SessionManager.get_config_with_monitor(
            user_id=state.get("user_id", ""),
            agent_type="chat",
            state=state,
            node_name="chat",
            enhanced_monitoring=True
        )
        
        # 获取最后一条消息的内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)
        
        response = chat_agent.invoke(
            {"messages": [{"role": "user", "content": content}]},
            config
        )
        
        # 获取响应内容
        response_content = response["messages"][-1].content
        logger.info(f"Chat response: {response_content[:100]}...")
        
        return {"messages": response["messages"]}
    except Exception as e:
        error_msg = f"聊天节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {"messages": [AIMessage("抱歉，我遇到了一些问题，请稍后再试。")], "error_message": error_msg}


def edw_model_node(state: EDWState):
    """模型节点：进一步分类模型相关任务"""
    
    # 如果已经识别到具体的意图类型，直接返回
    if state.get("type") in ["model_enhance", "model_add", "switch_model"]:
        logger.info(f"已识别意图类型: {state['type']}，跳过重复检测")
        return {"type": state["type"], "user_id": state.get("user_id", "")}
    
    prompt_template = config_manager.get_prompt("model_classification_prompt")
    prompt = PromptTemplate.from_template(prompt_template)
    
    try:
        # 使用带监控的配置管理器 - 模型智能体独立memory
        config = SessionManager.get_config_with_monitor(
            user_id=state.get("user_id", ""),
            agent_type="model",
            state=state,
            node_name="model_classification",
            enhanced_monitoring=True
        )
        
        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)
        
        response = llm_agent.invoke(
            {"messages": [{"role": "user", "content": prompt.format(input=content)}]},
            config
        )
        
        classification = response["messages"][-1].content.strip().lower()
        logger.info(f"Model classification result: {classification}")
        
        if "model_enhance" in classification:
            return {"type": "model_enhance", "user_id": state.get("user_id", "")}
        elif "model_add" in classification:
            return {"type": "model_add", "user_id": state.get("user_id", "")}
        else:
            return {"type": "switch_model", "user_id": state.get("user_id", "")}
    except Exception as e:
        error_msg = f"模型节点分类失败: {str(e)}"
        logger.error(error_msg)
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}