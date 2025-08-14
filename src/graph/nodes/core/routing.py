"""
路由函数模块
包含所有条件路由逻辑
"""

import logging
from langgraph.graph import END
from src.models.states import EDWState

logger = logging.getLogger(__name__)


def routing_fun(state: EDWState):
    """主路由函数：决定进入聊天、模型处理还是功能节点"""
    task_type = state.get("type", "")
    # 处理None或空值的情况
    if not task_type:
        return "chat_node"
    
    if 'function' in task_type:
        return "function_node"
    elif 'model' in task_type:
        return "model_node"
    return "chat_node"


def model_routing_fun(state: EDWState):
    """模型开发路由函数"""
    task_type = state.get("type", "")
    # 处理None或空值的情况
    if not task_type:
        return END
    
    if "model_enhance" in task_type:
        return "model_enhance_data_validation_node"
    elif "model_add" in task_type:
        return "model_add_data_validation_node"
    else:
        return END


def route_after_validation_check(state: EDWState):
    """验证检查后的路由函数"""
    validation_status = state.get("validation_status")
    
    if validation_status == "proceed":
        # 验证通过，先进入属性review节点
        return "attribute_review_subgraph"
    elif validation_status == "retry":
        # 需要重试，回到验证子图
        return "model_enhance_data_validation_node"
    else:
        # 默认结束
        return END


def enhancement_routing_fun(state: EDWState):
    """增强完成后的路由函数：决定是否需要走后续流程"""
    enhancement_type = state.get("enhancement_type", "")
    
    # 如果是仅修改逻辑，直接结束
    if enhancement_type == "modify_logic":
        logger.info("检测到仅修改逻辑，跳过ADB更新等后续流程")
        return END
    
    # 其他类型先进入代码review流程
    logger.info(f"增强类型 {enhancement_type}，进入代码review流程")
    return "code_review_subgraph"


def refinement_loop_routing(state: EDWState):
    """基于LLM分析结果的智能循环路由"""
    
    user_intent = state.get("user_intent", "SATISFIED_CONTINUE")
    intent_confidence = state.get("intent_confidence", 0.5)
    
    logger.info(f"微调路由决策 - 意图: {user_intent}, 置信度: {intent_confidence}")
    
    # 高置信度的意图识别
    if intent_confidence >= 0.8:
        if user_intent == "REFINEMENT_NEEDED":
            return "code_refinement_node"
        elif user_intent in ["SATISFIED_CONTINUE", "UNRELATED_TOPIC"]:
            return "github_push_node"
    
    # 低置信度情况下的保守策略
    elif intent_confidence >= 0.6:
        if user_intent == "REFINEMENT_NEEDED":
            return "code_refinement_node"  # 倾向于响应用户需求
        else:
            return "github_push_node"
    
    # 极低置信度，默认继续流程
    else:
        logger.warning(f"意图识别置信度过低 ({intent_confidence})，默认继续流程")
        return "github_push_node"