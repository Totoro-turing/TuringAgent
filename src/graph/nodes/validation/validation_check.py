"""
验证检查节点
处理验证状态并实施中断机制
"""

import logging
from langgraph.types import interrupt
from langchain.schema.messages import HumanMessage
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt

logger = logging.getLogger(__name__)


def validation_check_node(state: EDWState):
    """验证检查节点：处理验证状态并实施中断"""
    
    validation_status = state.get("validation_status")
    user_id = state.get("user_id", "")
    
    # 如果验证信息不完整，触发中断
    if validation_status == "incomplete_info":
        error_message = state.get("error_message", "需要补充信息")
        failed_node = state.get("failed_validation_node", "unknown")
        
        logger.info(f"验证失败于节点: {failed_node}, 准备生成上下文感知提示")
        
        # 生成上下文感知的提示
        contextual_prompt = generate_contextual_prompt(state, "validation_error")
        
        # 使用智能生成的提示进行中断
        user_input = interrupt({
            "prompt": contextual_prompt,
            "failed_node": failed_node,
            "validation_status": "waiting_for_input"
        })
        
        # 用户输入作为新消息添加到状态中
        return {
            "messages": [HumanMessage(content=user_input)],
            "validation_status": "retry",  # 标记需要重试
            "user_id": user_id
        }
    
    # 验证通过，可以继续
    elif validation_status == "completed":
        
        return {
            "validation_status": "proceed",  # 标记可以继续
            "user_id": user_id
        }
    
    # 其他情况
    return {"user_id": user_id}