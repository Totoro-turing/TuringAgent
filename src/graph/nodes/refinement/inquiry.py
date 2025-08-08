"""
微调询问节点
展示代码并询问用户想法
"""

import logging
from langgraph.types import interrupt
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt

logger = logging.getLogger(__name__)


def refinement_inquiry_node(state: EDWState):
    """微调询问节点 - 展示代码并询问用户想法"""
    
    enhanced_code = state.get("enhance_code", "")
    table_name = state.get("table_name", "")
    user_id = state.get("user_id", "")
    
    # 使用上下文感知生成询问
    contextual_prompt = generate_contextual_prompt(state, "code_refinement")
    
    # 使用智能生成的提示进行中断
    user_response = interrupt({
        "prompt": contextual_prompt,
        "action_type": "refinement_conversation"
    })
    
    return {
        "user_refinement_input": user_response,
        "refinement_conversation_started": True,
        "original_enhanced_code": enhanced_code,  # 备份原始代码
        "current_refinement_round": 1,
        "user_id": user_id
    }