"""
消息历史管理和总结模块

负责管理对话历史，当消息过多时自动进行总结，
避免上下文过长导致的性能问题。
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from langchain.schema.messages import AnyMessage, HumanMessage, AIMessage, SystemMessage
from langchain.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document

logger = logging.getLogger(__name__)


class MessageSummarizer:
    """消息历史管理器，负责自动总结过长的对话历史"""
    
    def __init__(self, threshold: int = 20, keep_recent: int = 5):
        """
        初始化消息总结器
        
        Args:
            threshold: 触发总结的消息数量阈值
            keep_recent: 总结后保留的最近消息数
        """
        self.threshold = threshold
        self.keep_recent = keep_recent
        self._init_summarizer()
        
    def _init_summarizer(self):
        """初始化总结器配置"""
        # 总结提示词模板
        self.summary_prompt_template = """请总结以下对话历史的关键信息：

{text}

总结要求：
1. 提取用户的核心需求和意图
2. 记录已完成的主要操作
3. 保留重要的技术细节（表名、字段名等）
4. 标注当前的处理状态
5. 使用简洁清晰的语言，控制在300字以内

总结："""
        
        self.summary_prompt = PromptTemplate(
            template=self.summary_prompt_template,
            input_variables=["text"]
        )
    
    def summarize_if_needed(self, messages: List[AnyMessage],
                                  force: bool = False) -> List[AnyMessage]:
        """
        检查并执行消息总结
        
        Args:
            messages: 当前的消息列表
            force: 是否强制执行总结
            
        Returns:
            处理后的消息列表（可能包含总结）
        """
        # 检查是否需要总结
        if not force and len(messages) <= self.threshold:
            return messages
            
        logger.info(f"触发消息总结: 当前{len(messages)}条消息，阈值{self.threshold}")
        
        try:
            # 分离需要总结的消息和保留的消息
            if len(messages) > self.keep_recent:
                to_summarize = messages[:-self.keep_recent]
                to_keep = messages[-self.keep_recent:]
            else:
                to_summarize = messages
                to_keep = []
            
            # 生成总结
            summary = self._generate_summary(to_summarize)
            
            # 构建总结消息
            summary_message = AIMessage(content=f"""📋 【历史对话总结】
{summary}

---
*以上为前 {len(to_summarize)} 条消息的总结，保留最近 {len(to_keep)} 条消息*""")
            
            # 返回：总结消息 + 最近的消息
            result = [summary_message] + to_keep
            logger.info(f"消息总结完成: {len(messages)} -> {len(result)} 条")
            
            return result
            
        except Exception as e:
            logger.error(f"消息总结失败: {e}")
            # 失败时返回原始消息
            return messages
    
    def _generate_summary(self, messages: List[AnyMessage]) -> str:
        """
        生成消息总结
        
        Args:
            messages: 需要总结的消息列表
            
        Returns:
            总结文本
        """
        try:
            # 获取共享的 LLM 实例
            from src.agent.edw_agents import get_shared_llm
            llm = get_shared_llm()
            
            # 将消息转换为文本
            conversation_text = self._format_messages_for_summary(messages)
            
            # 直接使用 LLM 生成总结
            prompt = self.summary_prompt.format(text=conversation_text)
            response = llm.invoke(prompt)
            
            return response.content if hasattr(response, 'content') else str(response)
            
        except Exception as e:
            logger.error(f"生成总结时出错: {e}")
            # 回退到简单的格式化
            return self._simple_summary(messages)
    
    def _format_messages_for_summary(self, messages: List[AnyMessage]) -> str:
        """将消息列表格式化为文本"""
        formatted_parts = []
        
        for i, msg in enumerate(messages, 1):
            # 确定角色
            if isinstance(msg, HumanMessage):
                role = "用户"
            elif isinstance(msg, AIMessage):
                role = "AI"
            elif isinstance(msg, SystemMessage):
                role = "系统"
            else:
                role = "未知"
            
            # 获取内容
            content = msg.content if hasattr(msg, 'content') else str(msg)
            
            # 限制单条消息长度
            if len(content) > 500:
                content = content[:500] + "..."
            
            formatted_parts.append(f"{i}. {role}: {content}")
        
        return "\n\n".join(formatted_parts)
    
    def _simple_summary(self, messages: List[AnyMessage]) -> str:
        """简单的回退总结方法"""
        summary_parts = [f"共处理了 {len(messages)} 条消息"]
        
        # 统计消息类型
        user_count = sum(1 for m in messages if isinstance(m, HumanMessage))
        ai_count = sum(1 for m in messages if isinstance(m, AIMessage))
        
        summary_parts.append(f"用户消息: {user_count} 条")
        summary_parts.append(f"AI响应: {ai_count} 条")
        
        # 提取最近的用户需求
        for msg in reversed(messages):
            if isinstance(msg, HumanMessage):
                content = msg.content if hasattr(msg, 'content') else str(msg)
                summary_parts.append(f"最近需求: {content[:100]}...")
                break
        
        return "\n".join(summary_parts)
    
    def extract_context_from_messages(self, messages: List[AnyMessage], 
                                    max_messages: int = 10) -> str:
        """
        从消息历史中提取上下文信息
        
        Args:
            messages: 消息列表
            max_messages: 最多提取的消息数
            
        Returns:
            格式化的上下文字符串
        """
        # 获取最近的消息
        recent_messages = messages[-max_messages:] if len(messages) > max_messages else messages
        
        context_parts = []
        for msg in recent_messages:
            # 确定角色
            if isinstance(msg, HumanMessage):
                role = "用户"
            elif isinstance(msg, AIMessage):
                role = "AI"
            else:
                role = "系统"
            
            # 获取内容并截断
            content = msg.content if hasattr(msg, 'content') else str(msg)
            if len(content) > 200:
                content = content[:200] + "..."
            
            context_parts.append(f"{role}: {content}")
        
        return "\n".join(context_parts)
    
    def get_summary_stats(self, messages: List[AnyMessage]) -> Dict[str, Any]:
        """
        获取消息统计信息
        
        Returns:
            包含统计信息的字典
        """
        stats = {
            "total_messages": len(messages),
            "needs_summary": len(messages) > self.threshold,
            "threshold": self.threshold,
            "keep_recent": self.keep_recent,
            "user_messages": sum(1 for m in messages if isinstance(m, HumanMessage)),
            "ai_messages": sum(1 for m in messages if isinstance(m, AIMessage)),
            "system_messages": sum(1 for m in messages if isinstance(m, SystemMessage))
        }
        
        return stats


# 创建全局实例（使用默认配置）
_global_summarizer = None


def get_message_summarizer(threshold: Optional[int] = None, 
                          keep_recent: Optional[int] = None) -> MessageSummarizer:
    """
    获取消息总结器实例
    
    Args:
        threshold: 触发总结的消息数量阈值
        keep_recent: 总结后保留的最近消息数
        
    Returns:
        MessageSummarizer 实例
    """
    global _global_summarizer
    
    # 如果需要自定义配置，创建新实例
    if threshold is not None or keep_recent is not None:
        return MessageSummarizer(
            threshold=threshold or 20,
            keep_recent=keep_recent or 5
        )
    
    # 否则返回全局实例
    if _global_summarizer is None:
        # 尝试从配置读取
        try:
            from src.config import get_config_manager
            config_manager = get_config_manager()
            
            # 获取消息管理配置（如果存在）
            # 注意：这需要在 config 中添加相应的配置项
            message_config = config_manager.get_message_config()
            _global_summarizer = MessageSummarizer(
                threshold=message_config.summary_threshold,
                keep_recent=message_config.keep_recent_count
            )
        except:
            # 使用默认配置
            _global_summarizer = MessageSummarizer()
    
    return _global_summarizer