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
        from src.agent.edw_agents import get_chat_agent
        from src.graph.message_summarizer import get_message_summarizer
        from src.graph.utils.session import SessionManager
        
        chat_agent = get_chat_agent()
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
            user_prompt = _build_validation_prompt(context, state)
        elif scene_hint == "code_refinement":
            user_prompt = _build_refinement_prompt(context, state)
        else:
            user_prompt = _build_general_prompt(context)
        
        # 构建完整的消息，包含系统提示词
        system_prompt = """你是一个专业的数据仓库助手，专门帮助用户处理EDW(企业数据仓库)相关任务。

**重要要求：**
- 必须使用Markdown格式回复
- 回复要结构清晰，使用适当的标题、列表、代码块等Markdown元素
- 语言要专业但友好，易于理解
- 针对不同场景提供具体、可操作的建议

**Markdown格式示例：**
- 使用 `#` 创建标题
- 使用 `-` 或 `*` 创建列表
- 使用 `**粗体**` 强调重点
- 使用 `代码` 标记技术术语
- 使用代码块 ```展示示例```

请始终保持专业、准确、有帮助的回复风格。"""

        messages_for_agent = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        # 使用带监控的配置管理器获取会话配置
        user_id = state.get("user_id", "contextual_prompt_user")
        config = SessionManager.get_config_with_monitor(
            user_id=user_id,
            agent_type="contextual_prompt",
            state=state,
            node_name="contextual_prompt",
            enhanced_monitoring=True
        )
        
        # 使用chat agent生成回复
        response = chat_agent.invoke(
            {"messages": messages_for_agent},
            config
        )
        
        return response["messages"][-1].content
        
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
    
    return f"""用户在提供信息时遇到了验证问题，需要你的帮助。请基于上下文中最后AI的提示，重新整理出一个清晰的提示。

**上下文信息：**
```json
{json.dumps(context, ensure_ascii=False, indent=2)}
```

**额外信息：**
- 用户重试次数：{retry_count}
- 当前验证状态：{state.get("validation_status", "unknown")}

请生成一个{patience_level}的**Markdown格式**回复，要求：

## 📝 回复要求
1. **确认信息**：先确认用户已经提供的有效信息（如果有）
2. **明确需求**：针对上下文特别指出需要用户补充什么信息，不要自己推断还需要什么信息
3. **格式示例**：提供具体的格式示例（使用代码块）
4. **友好引导**：避免生硬的错误提示，用引导性的语言
5. **耐心说明**：如果用户多次失败，要更加详细和体贴

## 🎯 格式要求
- 使用适当的标题结构（## ###）
- 重点信息用**粗体**标注
- 示例格式用```代码块```展示
- 使用列表组织信息
- 必要时添加适当的emoji增强友好度

请直接生成Markdown格式的回复内容："""


def _build_refinement_prompt(context: dict, state: dict) -> str:
    """构建代码微调场景的prompt"""
    
    current_round = state.get("current_refinement_round", 1)
    table_name = state.get("table_name", "")
    fields_count = len(state.get("fields", []))
    
    return f"""你刚为用户生成了数据模型增强代码，现在需要征求用户的反馈。

**上下文信息：**
```json
{json.dumps(context, ensure_ascii=False, indent=2)}
```

**生成结果概要：**
- **目标表**：`{table_name}`
- **新增字段数**：{fields_count}个
- **当前轮次**：第{current_round}轮交互

请生成一个**Markdown格式**的自然询问，要求：

## 📋 内容要求
1. **总结完成**：简要总结你完成了什么（避免过于技术性）
2. **征求反馈**：询问用户对结果的看法
3. **后续选项**：暗示可以继续调整或确认满意
4. **自然语气**：轻松自然，像是完成了一个任务在征求反馈
5. **适度修饰**：使用适当的emoji和格式，但不要过度

## 🎯 格式要求
- 使用标题和列表组织内容
- 重要信息用**粗体**标注
- 表名、字段名用`代码格式`
- 包含明确的询问或选择项
- 保持专业但友好的语调

请直接生成Markdown格式的询问内容："""


def _build_general_prompt(context: dict) -> str:
    """构建通用场景的prompt"""
    
    return f"""需要基于当前上下文生成合适的提示或询问。

**上下文信息：**
```json
{json.dumps(context, ensure_ascii=False, indent=2)}
```

请根据对话历史和当前状态，生成一个**Markdown格式**的合适提示或询问。

## 📋 内容要求
- **分析上下文**：理解用户当前的状态和需求
- **提供帮助**：给出有用的建议或询问
- **引导对话**：帮助用户明确下一步操作
- **友好专业**：保持专业但易接近的语调

## 🎯 格式要求
- 使用适当的Markdown结构（标题、列表、粗体等）
- 重要信息用**粗体**强调
- 技术术语用`代码格式`
- 根据上下文确定合适的详细程度
- 必要时提供具体的操作建议

请直接生成Markdown格式的内容："""


# 可选：提供一个简化的类接口
class ContextualPromptGenerator:
    """上下文提示生成器的类封装"""
    
    def __init__(self):
        from src.agent.edw_agents import get_chat_agent
        from src.graph.message_summarizer import get_message_summarizer
        
        self.chat_agent = get_chat_agent()
        self.summarizer = get_message_summarizer()
    
    def generate(self, state: dict, scene: str = None) -> str:
        """生成上下文感知的提示"""
        return generate_contextual_prompt(state, scene)


# 导出主要函数
__all__ = ['generate_contextual_prompt', 'ContextualPromptGenerator']