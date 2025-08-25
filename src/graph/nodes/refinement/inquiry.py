"""
微调询问节点 - 拆分为上下文准备和中断执行两个节点
展示代码并询问用户想法
"""

import logging
from langgraph.types import interrupt

from src.graph.utils.message_sender import send_node_message
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt

logger = logging.getLogger(__name__)


def refinement_context_node(state: EDWState):
    """微调上下文节点：准备微调询问的上下文感知提示"""
    
    enhanced_code = state.get("enhance_code", "")
    table_name = state.get("table_name", "")
    user_id = state.get("user_id", "")
    
    logger.info(f"生成微调询问的上下文提示，表: {table_name}")
    send_node_message(state, "AI", "processing", "我需要总结一下本次增强结果...", 0.1)
    # 生成上下文感知的提示
    contextual_prompt = generate_contextual_prompt(state, "code_refinement")
    
    # 存储到 ai_response 供中断节点使用
    return {
        "ai_response": contextual_prompt,
        "original_enhanced_code": enhanced_code,  # 备份原始代码
        "current_refinement_round": state.get("current_refinement_round", 0) + 1,
        "user_id": user_id
    }


def refinement_interrupt_node(state: EDWState):
    """微调中断节点：执行中断并处理用户输入"""
    
    user_id = state.get("user_id", "")
    
    # 从 ai_response 获取准备好的提示
    contextual_prompt = state.get("ai_response", "请提供您的微调意见")
    
    logger.info("触发微调询问中断")
    
    # 调用 interrupt 获取用户输入
    user_response = interrupt({
        "prompt": contextual_prompt,
        "action_type": "refinement_conversation"
    })
    
    # 返回用户输入
    return {
        "user_refinement_input": user_response,
        "refinement_conversation_started": True,
        "user_id": user_id,
        "ai_response": None  # 清理 ai_response
    }