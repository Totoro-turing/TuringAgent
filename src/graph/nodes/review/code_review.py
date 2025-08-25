"""
代码Review节点模块
实现代码质量评估和自动改进机制
完全符合LangGraph框架设计模式
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from langchain.schema.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt

from src.graph.utils.message_sender import (
    send_node_message,
    send_tool_message,
    send_code_message
)
from src.models.states import EDWState
from src.agent.edw_agents import get_shared_llm, get_shared_checkpointer

logger = logging.getLogger(__name__)


def code_review_node(state: EDWState) -> dict:
    """
    代码质量评估节点
    使用LLM对生成的代码进行多维度评估
    包括需求符合度评估
    """
    try:
        # 提取需要review的代码
        enhanced_code = state.get("enhance_code", "")
        table_name = state.get("table_name", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        review_round = state.get("review_round", 0) + 1
        
        # 获取socket队列
        session_id = state.get("session_id", "unknown")
        from src.server.socket_manager import get_session_socket
        socket_queue = get_session_socket(session_id)
        
        # 🎯 使用MessageSummarizer格式化消息历史（不使用memory）
        from langchain.output_parsers import PydanticOutputParser
        from src.models.edw_models import RequirementUnderstanding
        from src.graph.message_summarizer import MessageSummarizer
        
        # 创建需求理解的解析器
        requirement_parser = PydanticOutputParser(pydantic_object=RequirementUnderstanding)
        
        # 从state中获取完整的消息历史
        messages = state.get("messages", [])
        
        # 使用MessageSummarizer格式化消息历史
        summarizer = MessageSummarizer()
        # 提取最近10条消息的上下文
        conversation_context = summarizer.extract_context_from_messages(messages, max_messages=50)
        
        # 构建需求理解提示
        requirement_prompt = f"""基于以下对话历史，请总结用户的需求。

对话历史：
{conversation_context}

当前任务：对表 {table_name} 进行 {state.get("enhancement_type", "增强")}

{requirement_parser.get_format_instructions()}

注意：
1. 如果没有明确的需求，返回空
2. 只提取用户明确表达的需求，不要推测
3. 用一段话简洁总结
"""
        send_node_message(state, "AI", "processing", "我需要对生成的代码进行review...", 0.1)

        user_original_request = ""
        try:
            # 🤖 发送需求理解开始消息

            # 直接使用LLM，传入完整的消息历史作为上下文
            llm = get_shared_llm()
            import time
            req_start_time = time.time()
            
            # 构建包含历史的消息列表
            llm_messages = []
            # 添加系统消息说明任务
            llm_messages.append(SystemMessage(content="你是一个需求分析专家，请从对话历史中提取用户的需求。"))
            # 添加需求提取提示
            llm_messages.append(HumanMessage(content=requirement_prompt))
            
            # 使用LLM处理
            from src.graph.utils.session import SessionManager
            config = SessionManager.get_config_with_monitor(
                user_id=user_id,
                agent_type="requirement_analysis",
                state=state,
                node_name="code_review_requirement",
                enhanced_monitoring=False  # 不需要详细监控
            )
            
            requirement_response = llm.invoke(llm_messages, config)
            req_duration = time.time() - req_start_time
            
            # 🤖 发送需求理解完成消息
            if socket_queue:
                try:
                    socket_queue.send_message(
                        session_id,
                        "tool_progress",
                        {
                            "action": "complete",
                            "tool_name": "requirement_analysis",
                            "duration": round(req_duration, 2),
                            "message": f"✅ 需求理解完成 ({round(req_duration, 2)}秒)"
                        }
                    )
                except Exception as e:
                    logger.debug(f"发送需求理解完成消息失败: {e}")
            
            # 使用解析器解析响应
            requirement_content = requirement_response.content if hasattr(requirement_response, 'content') else str(requirement_response)
            requirement_understanding = requirement_parser.parse(requirement_content)
            
            # 提取总结性需求
            user_original_request = requirement_understanding.requirement_summary
            logger.info(f"需求总结: {user_original_request}")
            
        except Exception as e:
            logger.warning(f"需求理解失败，使用简单提取: {e}")
            # 回退到原始方法 - 直接从messages中提取用户消息
            if messages:
                for msg in messages:
                    if isinstance(msg, HumanMessage):
                        user_original_request = msg.content
                        break
        
        # 提取需求相关信息
        requirement_description = state.get("requirement_description", "")
        logic_detail = state.get("logic_detail", "")
        enhancement_type = state.get("enhancement_type", "")
        
        if not enhanced_code:
            logger.warning("没有需要review的代码")
            return {
                "review_score": 100,
                "review_feedback": "没有代码需要评估",
                "review_round": review_round,
                "user_id": user_id
            }
        
        # 获取代码语言
        code_language = state.get("code_language", "sql")
        
        # 构建review提示词（包含需求符合度评估）
        review_prompt = _build_review_prompt(
            enhanced_code=enhanced_code,
            table_name=table_name,
            fields=fields,
            review_round=review_round,
            user_request=user_original_request,
            logic_detail=logic_detail,
            requirement_description=requirement_description,
            code_language=code_language
        )
        
        # 使用LLM进行代码评估
        llm = get_shared_llm()
        # 延迟导入避免循环依赖
        from src.graph.utils.session import SessionManager
        config = SessionManager.get_config_with_monitor(
            user_id=user_id,
            agent_type="code_review",
            state=state,
            node_name="code_review",
            enhanced_monitoring=True
        )
        
        # 🤖 发送LLM调用开始消息 - 使用统一接口
        send_tool_message(
            state=state,
            action="start",
            tool_name="llm_invoke",
            message="🤖 正在调用AI模型评审代码质量..."
        )

        import time
        start_time = time.time()
        response = llm.invoke(review_prompt)
        duration = time.time() - start_time
        
        # 🤖 发送LLM调用完成消息 - 使用统一接口
        send_tool_message(
            state=state,
            action="complete",
            tool_name="llm_invoke",
            message=f"✅ AI评审完成 ({round(duration, 2)}秒)",
            duration=round(duration, 2)
        )
        review_result = _parse_review_response(response.content if hasattr(response, 'content') else str(response))
        
        # 更新review历史
        review_history = state.get("review_history", [])
        review_history.append({
            "round": review_round,
            "score": review_result["score"],
            "feedback": review_result["feedback"],
            "suggestions": review_result["suggestions"],
            "has_syntax_errors": review_result.get("has_syntax_errors", False),
            "timestamp": datetime.now().isoformat()
        })
        
        logger.info(f"代码Review完成 - 轮次: {review_round}, 评分: {review_result['score']}")
        
        # 发送review报告到前端 - 使用统一接口
        requirement_report = review_result.get("requirement_fulfillment_report", {})
        success = send_code_message(
            state=state,
            code_type="review_report",
            content="",  # review报告通过元数据传递
            table_name=table_name,
            review_round=review_round,
            score=review_result["score"],
            requirement_fulfilled=requirement_report.get("is_fulfilled", True),
            fulfillment_score=requirement_report.get("fulfillment_score", 100),
            missing_requirements=requirement_report.get("missing_requirements", []),
            suggestions=review_result["suggestions"]
        )
        
        if success:
            logger.info(f"✅ 统一接口发送review报告成功: {table_name}")
        else:
            logger.warning(f"❌ 统一接口发送review报告失败: {table_name}")
        
        return {
            "review_score": review_result["score"],
            "review_feedback": review_result["feedback"],
            "review_suggestions": review_result["suggestions"],
            "has_syntax_errors": review_result.get("has_syntax_errors", False),
            "review_round": review_round,
            "review_history": review_history,
            "requirement_fulfillment_report": review_result.get("requirement_fulfillment_report", {}),
            "user_id": user_id
        }
        
    except Exception as e:
        logger.error(f"代码review失败: {e}")
        return {
            "review_score": 0,
            "review_feedback": f"Review过程出错: {str(e)}",
            "review_round": state.get("review_round", 0) + 1,
            "user_id": state.get("user_id", ""),
            "error_message": str(e)
        }


async def code_regenerate_node(state: EDWState) -> dict:
    """
    代码重新生成节点
    根据review反馈重新生成改进的代码
    特别关注需求符合度问题
    """
    try:
        # 提取状态信息
        review_feedback = state.get("review_feedback", "")
        review_suggestions = state.get("review_suggestions", [])
        current_code = state.get("enhance_code", "")
        table_name = state.get("table_name", "")
        source_code = state.get("source_code", "")
        fields = state.get("fields", [])
        logic_detail = state.get("logic_detail", "")
        user_id = state.get("user_id", "")
        adb_code_path = state.get("adb_code_path", "")
        code_path = state.get("code_path", "")
        
        # 🔍 调试：检查review结果是否存在于state中
        logger.info(f"🔍 Review重新生成调试信息:")
        logger.info(f"  - review_feedback存在: {bool(review_feedback)}, 长度: {len(review_feedback) if review_feedback else 0}")
        logger.info(f"  - review_suggestions存在: {bool(review_suggestions)}, 数量: {len(review_suggestions) if review_suggestions else 0}")
        if review_feedback:
            logger.info(f"  - review_feedback前100字符: {review_feedback[:100]}...")
        if review_suggestions:
            logger.info(f"  - review_suggestions示例: {review_suggestions[:2]}")
        
        # 检查是否因为需求不符而需要重新生成
        requirement_report = state.get("requirement_fulfillment_report", {})
        is_requirement_fulfilled = requirement_report.get("is_fulfilled", True)
        
        # 统计需求不符的重新生成次数
        requirement_regeneration_count = state.get("requirement_regeneration_count", 0)
        if not is_requirement_fulfilled:
            requirement_regeneration_count += 1
            logger.info(f"因需求不符进行第{requirement_regeneration_count}次重新生成")
        
        # 获取代码语言
        code_language = state.get("code_language", "sql")
        
        # 🎯 优化版本：直接传递state，大大简化参数传递
        from src.graph.utils.enhancement import execute_code_enhancement_task
        
        logger.info(f"调用统一代码增强接口进行review重新生成: {table_name}")
        send_node_message(state, "AI", "processing", "按review的结果进行代码重生成...", 0.1)

        # 简化调用：只传递state和mode，所有参数都从state获取
        regeneration_result = await execute_code_enhancement_task(
            state=state,
            enhancement_mode="review_improvement"
        )
        
        if regeneration_result.get("success"):
            logger.info(f"代码重新生成成功 - 表: {table_name}")
            
            # 🎯 发送重新生成的代码到前端显示 - 使用统一接口
            success = send_code_message(
                state=state,
                code_type="enhanced",
                content=regeneration_result.get("enhanced_code"),
                table_name=table_name,
                enhancement_mode="review_improvement",  # 标记为review改进模式
                create_table_sql=regeneration_result.get("new_table_ddl", state.get("create_table_sql")),
                alter_table_sql=regeneration_result.get("alter_statements", state.get("alter_table_sql")),
                fields_count=len(fields) if fields else 0,
                enhancement_type=state.get("enhancement_type", ""),
                model_name=state.get("model_attribute_name", ""),
                file_path=code_path,
                adb_path=adb_code_path,
                optimization_summary=regeneration_result.get("optimization_summary", ""),
                review_round=state.get("review_round", 1)
            )
            
            if success:
                logger.info(f"✅ 统一接口发送review改进代码成功: {table_name}")
            else:
                logger.warning(f"❌ 统一接口发送review改进代码失败: {table_name}")
            
            return {
                "enhance_code": regeneration_result.get("enhanced_code"),
                "create_table_sql": regeneration_result.get("new_table_ddl", state.get("create_table_sql")),
                "alter_table_sql": regeneration_result.get("alter_statements", state.get("alter_table_sql")),
                "optimization_summary": regeneration_result.get("optimization_summary", ""),
                "user_id": user_id,
                "status": "regenerated",
                "status_message": "代码已根据review建议重新生成",
                "requirement_regeneration_count": requirement_regeneration_count  # 记录需求不符的重新生成次数
            }
        else:
            error_msg = regeneration_result.get("error", "重新生成失败")
            logger.error(f"代码重新生成失败: {error_msg}")
            
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": f"重新生成失败: {error_msg}",
                "error_message": error_msg
            }
            
    except Exception as e:
        logger.error(f"代码重新生成节点失败: {e}")
        return {
            "user_id": state.get("user_id", ""),
            "status": "error",
            "status_message": f"节点处理失败: {str(e)}",
            "error_message": str(e)
        }


def review_decision_routing(state: EDWState) -> str:
    """
    Review决策路由函数
    根据review结果决定下一步流向
    """
    review_score = state.get("review_score", 0)
    review_round = state.get("review_round", 1)
    has_syntax_errors = state.get("has_syntax_errors", False)
    max_rounds = state.get("max_review_rounds", 3)
    
    # 🎯 获取需求符合度报告
    requirement_report = state.get("requirement_fulfillment_report", {})
    is_requirement_fulfilled = requirement_report.get("is_fulfilled", True)
    
    logger.info(f"Review路由决策 - 评分: {review_score}, 轮次: {review_round}/{max_rounds}, 语法错误: {has_syntax_errors}")
    logger.info(f"需求是否满足: {is_requirement_fulfilled}")
    
    # 达到最大轮次，强制结束
    if review_round >= max_rounds:
        logger.info("达到最大review轮次，结束review流程")
        return END
    
    # 🎯 检查需求符合度 - 如果需求未满足，必须重新生成
    if not is_requirement_fulfilled:
        logger.info(f"需求未满足，需要重新生成代码")
        return "regenerate"
    
    # 有语法错误，需要重新生成
    if has_syntax_errors:
        logger.info("存在语法错误，需要重新生成代码")
        return "regenerate"
    
    # 评分低于阈值，需要重新生成
    if review_score < 70:
        logger.info(f"评分过低({review_score}分)，需要重新生成代码")
        return "regenerate"
    
    # 所有检查通过，结束review
    logger.info("代码质量合格且满足用户需求，结束review流程")
    return END


def create_review_subgraph():
    """
    创建代码review子图
    使用LangGraph标准模式构建
    """
    from src.agent.edw_agents import get_shared_checkpointer
    
    logger.info("创建代码review子图")
    
    return (
        StateGraph(EDWState)
        .add_node("review", code_review_node)
        .add_node("regenerate", code_regenerate_node)
        .add_edge(START, "review")
        .add_conditional_edges("review", review_decision_routing, ["regenerate", END])
        .add_edge("regenerate", "review")  # 重新生成后循环回review
        .compile(checkpointer=get_shared_checkpointer())  # 使用共享checkpointer支持状态持久化
    )


def _build_review_prompt(enhanced_code: str, table_name: str, fields: list, review_round: int,
                        user_request: str, logic_detail: str, requirement_description: str,
                        code_language: str = "sql") -> str:
    """构建简化的代码review提示词"""
    from langchain.output_parsers import PydanticOutputParser
    from src.models.edw_models import ReviewResult
    
    # 创建解析器获取格式说明
    parser = PydanticOutputParser(pydantic_object=ReviewResult)
    
    # 格式化字段信息
    fields_info = ""
    if fields:
        for field in fields:
            if isinstance(field, dict):
                physical_name = field.get('physical_name', '')
                attribute_name = field.get('attribute_name', '')
            else:
                physical_name = getattr(field, 'physical_name', '')
                attribute_name = getattr(field, 'attribute_name', '')
            fields_info += f"- {physical_name} ({attribute_name})\n"
    
    return f"""你是代码评审专家，请检查以下代码。

**任务信息**：
表名: {table_name}
用户需求: {user_request if user_request else logic_detail}
{f"新增字段:\n{fields_info}" if fields_info else ""}

**待评估代码**：
```{code_language}
{enhanced_code}
```

**请检查以下三个方面**：

1. **需求符合度** (40分)
   - 代码是否实现了用户的需求？

2. **语法检查** (30分)
   - 代码有无语法错误？

3. **代码质量** (30分)
   - 整体代码质量是否OK？

**输出格式**：
{parser.get_format_instructions()}

**注意**：
- score: 总分0-100
- dimension_scores只需包含: requirement_match, syntax_check, code_quality
- has_syntax_errors: 如果有语法错误设为true
- requirement_fulfillment_report.is_fulfilled: 需求是否满足
- requirement_fulfillment_report.summary: 简要说明需求符合情况"""


def _build_improvement_prompt(current_code: str, review_feedback: str, 
                              review_suggestions: list, original_requirements: dict,
                              requirement_report: dict = None, code_language: str = "sql") -> str:
    """构建代码改进提示词（包含需求不符处理）"""
    
    suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"
    
    # 强调需求不符
    requirement_focus = ""
    if requirement_report and not requirement_report.get("is_fulfilled", True):
        summary = requirement_report.get("summary", "")
        if summary:
            requirement_focus = f"\n**需求问题**：{summary}\n"
    
    return f"""根据review反馈改进代码。

**Review反馈**：
{review_feedback}

**改进建议**：
{suggestions_text}
{requirement_focus}
**原始需求**：
- 表名: {original_requirements.get('table_name', '')}
- 逻辑: {original_requirements.get('logic_detail', '')}

**当前代码**：
```{code_language}
{current_code}
```

**任务**：
1. 修复所有问题
2. 确保满足用户需求
3. 提升代码质量

**输出要求**：严格按JSON格式返回
{{
    "enhanced_code": "改进后的完整代码",
    "new_table_ddl": "CREATE TABLE语句（如有变化）",
    "alter_statements": "ALTER语句（如有变化）",
    "optimization_summary": "本次改进的说明"
}}"""


def _parse_review_response(content: str) -> dict:
    """使用PydanticOutputParser解析review响应"""
    from langchain.output_parsers import PydanticOutputParser, OutputFixingParser
    from src.models.edw_models import ReviewResult
    
    # 创建解析器
    parser = PydanticOutputParser(pydantic_object=ReviewResult)
    
    default_result = {
        "score": 0,
        "feedback": "解析失败",
        "suggestions": [],
        "has_syntax_errors": False,
        "dimension_scores": {},
        "requirement_fulfillment_report": {
            "is_fulfilled": True,
            "summary": ""
        }
    }
    
    try:
        # 使用解析器解析
        review_result = parser.parse(content)
        
        # 转换为字典格式（保持向后兼容）
        result_dict = review_result.model_dump()
        
        # 确保requirement_fulfillment_report是字典格式
        if "requirement_fulfillment_report" in result_dict:
            report = result_dict["requirement_fulfillment_report"]
            if hasattr(report, 'model_dump'):
                result_dict["requirement_fulfillment_report"] = report.model_dump()
        
        return result_dict
        
    except Exception as e:
        logger.warning(f"Review响应解析失败，尝试修复: {e}")
        
        # 尝试使用OutputFixingParser修复
        try:
            fixing_parser = OutputFixingParser.from_llm(
                parser=parser,
                llm=get_shared_llm()
            )
            review_result = fixing_parser.parse(content)
            
            # 转换为字典格式
            result_dict = review_result.model_dump()
            
            # 确保requirement_fulfillment_report是字典格式
            if "requirement_fulfillment_report" in result_dict:
                report = result_dict["requirement_fulfillment_report"]
                if hasattr(report, 'model_dump'):
                    result_dict["requirement_fulfillment_report"] = report.model_dump()
            
            logger.info("使用修复解析器成功解析review响应")
            return result_dict
            
        except Exception as fix_error:
            logger.error(f"修复解析失败: {fix_error}")
            logger.error(f"原始内容前200字符: {content[:200]}...")
            return default_result


# 注意：_build_git_diff_improvement_prompt 函数已删除
# 该功能已合并到 src/graph/utils/enhancement.py 中的 GitDiffEnhancer 类