"""
意图识别节点
基于大语言模型的用户意图深度识别
"""

import logging
from langchain.schema.messages import HumanMessage, AIMessage
from langchain.output_parsers import PydanticOutputParser
from src.models.states import EDWState
from src.models.edw_models import RefinementIntentAnalysis

logger = logging.getLogger(__name__)


def refinement_intent_node(state: EDWState):
    """基于大语言模型的用户意图深度识别节点"""
    
    user_input = state.get("user_refinement_input", "")
    user_id = state.get("user_id", "")
    messages = state.get("messages", [])
    
    # 获取消息总结器和配置
    from src.graph.message_summarizer import get_message_summarizer
    from src.config import get_config_manager
    
    config_manager = get_config_manager()
    message_config = config_manager.get_message_config()
    
    # 使用消息总结器处理消息历史
    summarizer = get_message_summarizer()
    try:
        # 先进行消息总结（如果需要）
        summarized_messages = summarizer.summarize_if_needed(messages)
    except Exception as e:
        logger.warning(f"消息总结失败，使用原始消息: {e}")
        summarized_messages = messages
    
    # 使用 LangChain 的 PydanticOutputParser
    parser = PydanticOutputParser(pydantic_object=RefinementIntentAnalysis)
    
    # 使用动态上下文的意图分析提示词
    intent_analysis_prompt = f"""你是一个专业的用户意图分析专家，需要结合聊天历史的上下文深度理解用户对代码增强结果的真实想法和需求。

**用户刚刚说**: "{user_input}"

**任务**: 请深度分析用户的真实意图，考虑语义、情感、上下文等多个维度。

**意图分类标准**:

1. **REFINEMENT_NEEDED** - 用户希望对代码进行调整/改进
   识别场景：
   - 明确提出修改建议（如"能不能优化一下"、"这里逻辑有问题"）
   - 表达不满意或疑虑（如"感觉性能不够好"、"这样写对吗"）
   - 提出新的要求（如"能加个异常处理吗"、"可以添加注释吗"）
   - 询问是否可以改进（如"还能更好吗"、"有没有别的写法"）

2. **SATISFIED_CONTINUE** - 用户对结果满意，希望继续后续流程
   识别场景：
   - 表达满意（如"不错"、"可以"、"很好"、"满意"）
   - 确认继续（如"继续吧"、"可以进行下一步"、"没问题"）
   - 赞同认可（如"就这样"、"挺好的"、"符合预期"）

3. **UNRELATED_TOPIC** - 用户说的内容与当前代码增强任务无关
   识别场景：
   - 日常闲聊（如"今天天气如何"、"你好"）
   - 询问其他技术问题（如"Python怎么学"）
   - 完全无关的话题

**分析要求**:
- 重点理解用户的**真实情感倾向**和**实际需求**
- 考虑**语境和上下文**，不要只看字面意思
- 对于模糊或间接的表达，要推断其深层含义
- 如果用户表达含糊，倾向于理解为需要进一步沟通

{parser.get_format_instructions()}
"""
    
    try:
        # 使用专门的意图分析代理（无记忆）
        from src.agent.edw_agents import create_intent_analysis_agent
        
        intent_agent = create_intent_analysis_agent()
        
        response = intent_agent.invoke(
            {"messages": summarized_messages + [HumanMessage(intent_analysis_prompt)]}
        )
        
        # 使用 LangChain parser 解析响应
        analysis_content = response["messages"][-1].content
        intent_result = parser.parse(analysis_content)
        
        logger.info(f"LLM意图分析结果: {intent_result}")
        
        result = {
            "user_intent": intent_result.intent,
            "intent_confidence": intent_result.confidence_score,
            "intent_reasoning": intent_result.reasoning,
            "refinement_requirements": intent_result.extracted_requirements,
            "user_emotion": intent_result.user_emotion,
            "suggested_response": intent_result.suggested_response,
            "user_id": user_id
        }
        
        # 准备要添加的消息列表
        messages_to_add = []
        
        # 添加用户的最新输入
        if user_input:
            messages_to_add.append(HumanMessage(content=user_input))
        
        # 将意图分析结果格式化为用户友好的消息
        intent_summary = f"📊 意图分析完成：{intent_result.intent} (置信度: {intent_result.confidence_score})"
        # 如果消息被总结了，使用总结后的消息作为基础
        if len(summarized_messages) != len(messages):
            result["messages"] = summarized_messages + messages_to_add
            logger.info(f"消息已总结：{len(messages)} -> {len(summarized_messages)} 条，添加用户输入")
        else:
            # 否则只添加用户输入
            result["messages"] = messages_to_add
        
        return result
        
    except Exception as e:
        # 解析失败时的优雅降级
        logger.error(f"意图识别解析失败: {e}")
        result = {
            "user_intent": "SATISFIED_CONTINUE",  # 默认继续
            "intent_confidence": 0.5,
            "intent_reasoning": f"解析失败，使用默认判断: {str(e)}",
            "refinement_requirements": "",
            "user_emotion": "neutral",
            "suggested_response": "",
            "user_id": user_id
        }
        
        # 即使解析失败，也要处理消息总结和用户输入
        try:
            summarized_messages = summarizer.summarize_if_needed(messages)
            
            # 准备要添加的消息列表
            messages_to_add = []
            
            # 添加用户的最新输入
            if user_input:
                messages_to_add.append(HumanMessage(content=user_input))
            
            # 如果消息被总结了，使用总结后的消息作为基础
            if len(summarized_messages) != len(messages):
                result["messages"] = summarized_messages + messages_to_add
                logger.info(f"消息已总结（异常处理）：{len(messages)} -> {len(summarized_messages)} 条，添加用户输入")
            else:
                # 否则只添加用户输入
                result["messages"] = messages_to_add
                
        except Exception as summary_error:
            logger.warning(f"异常处理中的消息总结也失败: {summary_error}")
            # 至少保存用户输入
            if user_input:
                result["messages"] = [HumanMessage(content=user_input)]
        
        return result