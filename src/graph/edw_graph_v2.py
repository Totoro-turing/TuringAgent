"""
EDW主图定义（重构版）
使用新的节点组织结构
"""

import logging
import uuid
from langgraph.graph import StateGraph, START, END
from src.models.states import EDWState
from src.agent.edw_agents import get_shared_checkpointer
from src.graph.validation_nodes import create_validation_subgraph

# 导入所有节点（从新的包结构）
from src.graph.nodes import (
    # 核心节点
    navigate_node,
    chat_node,
    edw_model_node,
    # 路由函数
    routing_fun,
    model_routing_fun,
    enhancement_routing_fun,
    refinement_loop_routing,
    route_after_validation_check,
    # 验证节点
    validation_check_node,
    edw_model_add_data_validation_node,
    # 增强节点
    edw_model_enhance_node,
    edw_model_addition_node,
    # 微调节点
    refinement_inquiry_node,
    refinement_intent_node,
    code_refinement_node,
    # 外部集成节点
    github_push_node,
    edw_email_node,
    edw_confluence_node,
    edw_adb_update_node,
    # Review子图
    create_review_subgraph,
    create_attribute_review_subgraph,
)

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_model_dev_graph():
    """创建模型开发子图"""
    
    # 创建验证子图实例
    validation_subgraph = create_validation_subgraph()
    
    # 创建代码review子图实例
    review_subgraph = create_review_subgraph()
    
    # 创建属性名称review子图实例
    attribute_review_subgraph = create_attribute_review_subgraph()
    
    model_dev_graph = (
        StateGraph(EDWState)
        # 验证节点
        .add_node("model_enhance_data_validation_node", validation_subgraph)
        .add_node("validation_check_node", validation_check_node)
        .add_node("model_add_data_validation_node", edw_model_add_data_validation_node)
        # Review节点
        .add_node("attribute_review_subgraph", attribute_review_subgraph)
        .add_node("code_review_subgraph", review_subgraph)
        # 增强节点
        .add_node("model_enhance_node", edw_model_enhance_node)
        .add_node("model_addition_node", edw_model_addition_node)
        # 微调节点
        .add_node("refinement_inquiry_node", refinement_inquiry_node)
        .add_node("refinement_intent_node", refinement_intent_node)
        .add_node("code_refinement_node", code_refinement_node)
        # 外部集成节点
        .add_node("github_push_node", github_push_node)
        .add_node("adb_update_node", edw_adb_update_node)
        .add_node("email_node", edw_email_node)
        .add_node("confluence_node", edw_confluence_node)
        
        # 路由配置
        .add_conditional_edges(START, model_routing_fun, [
            "model_enhance_data_validation_node", 
            "model_add_data_validation_node"
        ])
        # 验证流程
        .add_edge("model_enhance_data_validation_node", "validation_check_node")
        .add_conditional_edges("validation_check_node", route_after_validation_check, [
            "attribute_review_subgraph",
            "model_enhance_data_validation_node",
            END
        ])
        # 属性review -> 增强
        .add_edge("attribute_review_subgraph", "model_enhance_node")
        .add_edge("model_add_data_validation_node", "model_addition_node")
        
        # 增强完成后的路由
        .add_conditional_edges("model_enhance_node", enhancement_routing_fun, [
            "code_review_subgraph",
            END
        ])
        
        # 代码review -> 微调询问
        .add_edge("code_review_subgraph", "refinement_inquiry_node")
        
        # 微调循环流程
        .add_edge("refinement_inquiry_node", "refinement_intent_node")
        .add_conditional_edges("refinement_intent_node", refinement_loop_routing, [
            "code_refinement_node",
            "github_push_node"
        ])
        .add_edge("code_refinement_node", "refinement_inquiry_node")
        
        # 后续流程
        .add_edge("model_addition_node", "github_push_node")
        .add_edge("github_push_node", "adb_update_node")
        .add_edge("adb_update_node", "confluence_node")
        .add_edge("confluence_node", "email_node")
        .add_edge("email_node", END)
    )
    
    return model_dev_graph.compile(
        checkpointer=get_shared_checkpointer()
    )


def create_main_graph():
    """创建主图"""
    
    # 创建模型开发子图
    model_dev = create_model_dev_graph()
    
    # 创建主导航图
    guid_graph = (
        StateGraph(EDWState)
        .add_node("navigate_node", navigate_node)
        .add_node("chat_node", chat_node)
        .add_node("model_node", edw_model_node)
        .add_node("model_dev_node", model_dev)
        .add_edge(START, "navigate_node")
        .add_conditional_edges("navigate_node", routing_fun, ["chat_node", "model_node"])
        .add_edge("model_node", "model_dev_node")
        .add_edge("model_dev_node", END)
        .add_edge("chat_node", END)
    )
    
    return guid_graph.compile(
        checkpointer=get_shared_checkpointer()
    )


# 导出主图
guid = create_main_graph()


if __name__ == "__main__":
    logger.info("EDW图重构版本已加载")
    
    # 测试图的编译
    try:
        test_state = {
            "messages": [],
            "user_id": str(uuid.uuid4())[:8],
            "type": "test"
        }
        logger.info("图编译成功")
    except Exception as e:
        logger.error(f"图编译失败: {e}")