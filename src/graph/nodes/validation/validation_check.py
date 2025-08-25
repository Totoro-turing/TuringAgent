"""
验证检查节点 - 拆分为上下文准备和中断执行两个节点
处理验证状态并实施中断机制
"""

import logging
from langgraph.types import interrupt
from langchain.schema.messages import HumanMessage, AIMessage

from src.graph.utils.message_sender import send_node_message
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt

logger = logging.getLogger(__name__)


def validation_context_node(state: EDWState):
    """验证上下文节点：准备验证失败的上下文感知提示"""
    
    validation_status = state.get("validation_status")
    user_id = state.get("user_id", "")
    
    # 如果验证信息不完整，生成上下文感知的提示
    if validation_status == "incomplete_info":
        failed_node = state.get("failed_validation_node", "unknown")
        logger.info(f"验证失败于节点: {failed_node}, 生成上下文感知提示")
        send_node_message(state, "ai", "processing", "AI整理中...", 0.9)
        # 生成上下文感知的提示
        contextual_prompt = generate_contextual_prompt(state, "validation_error")
        
        # 存储到 ai_response 供中断节点使用
        return {
            "messages": [AIMessage(content=contextual_prompt)],
            "ai_response": contextual_prompt,
            "user_id": user_id
        }
    
    # 验证通过，直接继续（不需要中断）
    elif validation_status == "completed":
        return {
            "validation_status": "proceed",  # 标记可以继续
            "user_id": user_id
        }
    
    # 其他情况
    return {"user_id": user_id}


def validation_interrupt_node(state: EDWState):
    """验证中断节点：执行中断并处理用户输入"""
    
    validation_status = state.get("validation_status")
    user_id = state.get("user_id", "")
    
    # 如果验证信息不完整，触发中断
    if validation_status == "incomplete_info":
        # 从 ai_response 获取准备好的提示
        contextual_prompt = state.get("ai_response", "请提供缺失的信息")
        failed_node = state.get("failed_validation_node", "unknown")
        
        logger.info(f"触发验证中断，节点: {failed_node}")
        
        # 调用 interrupt 获取用户输入
        user_input = interrupt({
            "prompt": contextual_prompt,
            "failed_node": failed_node,
            "validation_status": "waiting_for_input"
        })
        
        # 用户输入作为新消息添加到状态中
        return {
            "messages": [HumanMessage(content=user_input)],
            "validation_status": "retry",  # 标记需要重试
            "user_id": user_id,
            "ai_response": None  # 清理 ai_response
        }
    
    # 验证已通过或其他情况，无需中断
    return {
        "user_id": user_id
    }