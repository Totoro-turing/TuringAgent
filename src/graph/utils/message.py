"""
消息处理工具函数
"""

import logging
from typing import List
from langchain.schema.messages import HumanMessage, AIMessage
from langchain.docstore.document import Document
from langchain.chains.summarize import load_summarize_chain
from src.models.states import EDWState
from src.agent.edw_agents import get_shared_llm

logger = logging.getLogger(__name__)

# 通用总结回复提示词常量
SUMMARY_REPLY_PROMPT = """你是一个专业的对话总结助手，负责分析用户与EDW系统的交互历史，生成简洁明了的总结。

**任务要求**：
1. 仔细分析提供的对话历史和上下文信息
2. 提取关键信息：用户需求、系统回应、当前状态、遇到的问题
3. 生成结构化的markdown格式总结
4. 语言风格要友好、专业、易懂

**上下文信息**：
{context_info}

**对话历史**：
{conversation_history}

**输出要求**：
- 必须使用markdown格式
- 包含关键信息的结构化展示
- 突出当前状态和下一步行动
- 如果有错误或问题，要明确指出
- 总结长度控制在200-400字
- 使用中文回复

请生成对话总结："""


def extract_message_content(message) -> str:
    """统一提取消息内容"""
    if isinstance(message, str):
        return message
    elif hasattr(message, 'content'):
        return message.content
    else:
        return str(message)


def build_context_info(state: EDWState) -> str:
    """构建上下文信息字符串"""
    context_parts = []
    
    # 基础信息
    if state.get("table_name"):
        context_parts.append(f"**目标表**: {state['table_name']}")
    
    if state.get("type"):
        context_parts.append(f"**任务类型**: {state['type']}")
    
    # 状态信息
    if state.get("status"):
        context_parts.append(f"**当前状态**: {state['status']}")
    
    if state.get("status_message"):
        context_parts.append(f"**状态消息**: {state['status_message']}")
    
    if state.get("error_message"):
        context_parts.append(f"**遇到问题**: {state['error_message']}")
    
    # 业务信息
    if state.get("logic_detail"):
        context_parts.append(f"**需求描述**: {state['logic_detail']}")
    
    if state.get("fields"):
        field_count = len(state['fields'])
        context_parts.append(f"**新增字段数量**: {field_count}个")
    
    if state.get("enhancement_type"):
        context_parts.append(f"**增强类型**: {state['enhancement_type']}")
    
    # 进展信息
    if state.get("validation_status"):
        context_parts.append(f"**验证状态**: {state['validation_status']}")
    
    if state.get("current_refinement_round"):
        context_parts.append(f"**微调轮次**: 第{state['current_refinement_round']}轮")
    
    return "\n".join(context_parts) if context_parts else "无特殊上下文信息"


def format_conversation_history(messages: List) -> str:
    """格式化对话历史为易读格式"""
    if not messages:
        return "无对话历史"
    
    formatted_messages = []
    for i, message in enumerate(messages, 1):
        content = extract_message_content(message)
        
        # 确定消息来源
        if isinstance(message, HumanMessage):
            source = "用户"
        elif isinstance(message, AIMessage):
            source = "系统"
        else:
            source = "系统"
        
        # 限制单条消息长度
        if len(content) > 200:
            content = content[:200] + "..."
        
        formatted_messages.append(f"{i}. **{source}**: {content}")
    
    return "\n".join(formatted_messages)


def _generate_summary_with_llm(context_info: str, conversation_history: str) -> str:
    """使用LLM生成总结"""
    try:
        # 获取共享的LLM实例
        llm = get_shared_llm()
        
        # 格式化提示词
        prompt = SUMMARY_REPLY_PROMPT.format(
            context_info=context_info,
            conversation_history=conversation_history
        )
        
        # 使用LLM生成总结
        response = llm.invoke(prompt)
        return response.content if hasattr(response, 'content') else str(response)
        
    except Exception as e:
        logger.error(f"LLM总结生成失败: {e}")
        return f"## 📋 对话总结\n\n生成总结时出现错误: {str(e)}\n\n### 基本信息\n{context_info}"


def _summarize_long_conversation(messages: List) -> str:
    """使用LangChain处理长对话历史"""
    try:
        # 获取共享的LLM实例
        llm = get_shared_llm()
        
        # 将消息转换为文档
        docs = []
        for i, message in enumerate(messages):
            content = extract_message_content(message)
            source = "用户" if isinstance(message, HumanMessage) else "系统"
            doc_content = f"{source}: {content}"
            docs.append(Document(page_content=doc_content))
        
        # 使用LangChain的summarize chain
        summarize_chain = load_summarize_chain(llm, chain_type="stuff")
        summary = summarize_chain.run(docs)
        
        return f"**对话历史总结** (共{len(messages)}条消息):\n{summary}"
        
    except Exception as e:
        logger.error(f"长对话总结失败: {e}")
        # 回退到直接格式化最近的消息
        recent_messages = messages[-5:] if len(messages) > 5 else messages
        return format_conversation_history(recent_messages)


def create_summary_reply(state: EDWState) -> str:
    """
    创建总结回复的独立方法
    
    Args:
        state: EDW状态对象，包含messages等信息
    
    Returns:
        markdown格式的总结回复
    """
    try:
        # 提取消息历史
        messages = state.get("messages", [])
        
        # 构建上下文信息
        context_info = build_context_info(state)
        
        # 处理对话历史
        if len(messages) > 8:
            # 消息较多时，使用LangChain summarize处理长对话
            conversation_history = _summarize_long_conversation(messages)
        else:
            # 消息较少时，直接格式化
            conversation_history = format_conversation_history(messages)
        
        # 使用LLM生成总结
        summary = _generate_summary_with_llm(context_info, conversation_history)
        
        logger.info(f"成功生成对话总结，消息数量: {len(messages)}")
        return summary
        
    except Exception as e:
        logger.error(f"创建总结回复失败: {e}")
        return f"## 📋 对话总结\n\n生成总结时出现错误: {str(e)}"