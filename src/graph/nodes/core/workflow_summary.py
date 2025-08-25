"""
工作流总结节点
使用LLM从消息历史生成执行总结
"""

import logging
from typing import Dict, Any
from langchain.schema.messages import AIMessage, HumanMessage
from src.models.states import EDWState
from src.agent.edw_agents import get_shared_llm
from src.graph.utils.session import SessionManager

logger = logging.getLogger(__name__)


def workflow_summary_node(state: EDWState) -> Dict[str, Any]:
    """
    使用LLM生成工作流执行总结
    
    从messages历史中提取信息，生成Markdown格式的总结
    """
    try:
        # 获取LLM
        llm = get_shared_llm()
        
        # 获取消息历史
        messages = state.get("messages", [])
        
        # 构建总结提示词
        summary_prompt = _build_summary_prompt(messages)
        
        # 获取会话配置
        user_id = state.get("user_id", "")
        config = SessionManager.get_config_with_monitor(
            user_id=user_id,
            agent_type="summary",
            state=state,
            node_name="workflow_summary",
            enhanced_monitoring=False  # 总结不需要详细监控
        )
        
        # 调用LLM生成总结
        response = llm.invoke(summary_prompt, config)
        
        # 提取响应内容
        summary_content = response.content if hasattr(response, 'content') else str(response)
        
        logger.info("工作流总结生成成功")
        
        return {
            "messages": [AIMessage(content=summary_content)],
            "user_id": user_id,
            "workflow_completed": True,
            "summary_generated": True
        }
        
    except Exception as e:
        logger.error(f"生成工作流总结失败: {e}")
        # 降级处理：返回简单总结
        fallback_summary = "## 任务完成\n\n所有请求的操作已执行完毕。"
        return {
            "messages": [AIMessage(content=fallback_summary)],
            "user_id": state.get("user_id", ""),
            "error_message": str(e)
        }


def _build_summary_prompt(messages: list) -> str:
    """
    构建总结提示词
    
    Args:
        messages: 消息历史列表
    
    Returns:
        格式化的提示词
    """
    # 将消息历史转换为文本
    conversation_history = []
    for msg in messages:
        if isinstance(msg, HumanMessage):
            conversation_history.append(f"用户: {msg.content}")
        elif isinstance(msg, AIMessage):
            conversation_history.append(f"系统: {msg.content}")
        else:
            # 处理其他类型的消息
            content = msg.content if hasattr(msg, 'content') else str(msg)
            conversation_history.append(f"消息: {content}")
    
    # 只保留最近的消息（避免过长）
    if len(conversation_history) > 20:
        # 保留开头的用户请求和最近的执行结果
        conversation_history = (
            conversation_history[:2] +  # 保留最初的用户请求
            ["... (中间消息省略) ..."] +
            conversation_history[-15:]  # 保留最近的15条消息
        )
    
    conversation_text = "\n".join(conversation_history)
    
    prompt = f"""基于以下对话历史，生成一个工作流执行总结。

对话历史：
{conversation_text}

要求：
1. 使用Markdown格式输出
2. 结构清晰，使用标题、列表等元素
3. 总结主要完成的任务
4. 列出各系统的更新状态（如GitHub、ADB、Confluence等）
5. 如果有重要的链接或信息，请包含在总结中
6. 语言简洁专业，不要冗长
7. 不要询问或等待用户反馈，这是一个总结
8. 使用中文

请生成Markdown格式的执行总结："""
    
    return prompt


# 导出
__all__ = ['workflow_summary_node']