"""
上下文感知的提示生成模块

提供智能的、基于完整对话历史的提示生成功能，
用于改善用户在验证失败、代码微调等场景的交互体验。
"""

import logging
import json
from typing import List, Optional
from langchain.schema.messages import HumanMessage, AIMessage

logger = logging.getLogger(__name__)


def generate_contextual_prompt(state: dict, scene_hint: str = None) -> str:
    """
    生成上下文感知的提示信息
    
    Args:
        state: 当前完整状态，包含messages、错误信息等
        scene_hint: 场景提示（可选）如 "validation_error" 或 "code_refinement"
    
    Returns:
        生成的上下文感知提示
    """
    try:
        # 获取必要组件
        from src.agent.edw_agents import get_shared_llm
        from src.graph.message_summarizer import get_message_summarizer
        
        llm = get_shared_llm()
        summarizer = get_message_summarizer()
        
        # 总结消息历史
        messages = state.get("messages", [])
        summarized_messages = summarizer.summarize_if_needed(messages)
        
        # 构建上下文
        context = {
            "对话历史": _format_messages(summarized_messages),
            "当前信息": _extract_key_state(state),
            "场景": scene_hint or "general"
        }
        
        # 根据场景构建不同的prompt
        if scene_hint == "validation_error":
            prompt = _build_validation_prompt(context, state)
        elif scene_hint == "code_refinement":
            prompt = _build_refinement_prompt(context, state)
        else:
            prompt = _build_general_prompt(context)
        
        # 让 LLM 生成合适的提示
        response = llm.invoke(prompt)
        return response.content if hasattr(response, 'content') else str(response)
        
    except Exception as e:
        logger.error(f"生成上下文提示失败: {e}")
        # 降级到默认提示
        if scene_hint == "validation_error":
            return state.get("error_message", "请提供更多信息以继续")
        elif scene_hint == "code_refinement":
            return "代码已生成完成，请问您对结果有什么想法？"
        else:
            return "请告诉我您的需求"


def _format_messages(messages: List) -> List[dict]:
    """格式化消息历史为简洁的字典列表"""
    formatted = []
    for msg in messages:
        role = "用户" if isinstance(msg, HumanMessage) else "AI"
        content = msg.content if hasattr(msg, 'content') else str(msg)
        
        # 限制单条消息长度
        if len(content) > 200:
            content = content[:200] + "..."
            
        formatted.append({
            "角色": role,
            "内容": content
        })
    
    return formatted


def _extract_key_state(state: dict) -> dict:
    """提取关键状态信息"""
    key_fields = [
        "table_name", "fields", "error_message", "missing_info",
        "enhancement_type", "logic_detail", "retry_count",
        "validation_status", "current_refinement_round"
    ]
    
    extracted = {}
    for field in key_fields:
        if field in state and state[field] is not None:
            value = state[field]
            # 对于复杂对象，简化显示
            if field == "fields" and isinstance(value, list):
                extracted[field] = f"{len(value)}个字段"
            elif field == "missing_info" and isinstance(value, list):
                extracted[field] = ", ".join(value)
            else:
                extracted[field] = value
    
    return extracted


def _build_validation_prompt(context: dict, state: dict) -> str:
    """构建验证错误场景的prompt"""
    
    retry_count = state.get("retry_count", 0)
    patience_level = "特别耐心" if retry_count > 1 else "友好"
    
    return f"""你是一个数据仓库助手。用户在提供信息时遇到了验证问题，需要你的帮助。

上下文信息：
{json.dumps(context, ensure_ascii=False, indent=2)}

额外信息：
- 用户重试次数：{retry_count}
- 当前验证状态：{state.get("validation_status", "unknown")}

请生成一个{patience_level}的回复，要求：
1. 先确认用户已经提供的有效信息（如果有）
2. 明确说明还需要什么信息
3. 提供具体的格式示例
4. 避免生硬的错误提示，用引导性的语言
5. 如果用户多次失败，要更加详细和体贴

请直接生成回复内容，不要有额外的解释："""


def _build_refinement_prompt(context: dict, state: dict) -> str:
    """构建代码微调场景的prompt"""
    
    current_round = state.get("current_refinement_round", 1)
    table_name = state.get("table_name", "")
    fields_count = len(state.get("fields", []))
    
    return f"""你是一个数据仓库助手。你刚为用户生成了数据模型增强代码。

上下文信息：
{json.dumps(context, ensure_ascii=False, indent=2)}

生成结果概要：
- 目标表：{table_name}
- 新增字段数：{fields_count}
- 当前是第{current_round}轮交互

请生成一个自然的询问，要求：
1. 简要总结你完成了什么（不要太技术性）
2. 询问用户对结果的看法
3. 暗示可以继续调整或确认满意
4. 语气轻松自然，像是完成了一个任务在征求反馈
5. 不要使用太多emoji或符号

请直接生成询问内容："""


def _build_general_prompt(context: dict) -> str:
    """构建通用场景的prompt"""
    
    return f"""你是一个数据仓库助手，需要基于上下文生成合适的提示。

上下文信息：
{json.dumps(context, ensure_ascii=False, indent=2)}

请根据对话历史和当前状态，生成一个合适的提示或询问。
要求友好、具体、有帮助。

请直接生成内容："""


# 可选：提供一个简化的类接口
class ContextualPromptGenerator:
    """上下文提示生成器的类封装"""
    
    def __init__(self):
        from src.agent.edw_agents import get_shared_llm
        from src.graph.message_summarizer import get_message_summarizer
        
        self.llm = get_shared_llm()
        self.summarizer = get_message_summarizer()
    
    def generate(self, state: dict, scene: str = None) -> str:
        """生成上下文感知的提示"""
        return generate_contextual_prompt(state, scene)


# 导出主要函数
__all__ = ['generate_contextual_prompt', 'ContextualPromptGenerator']