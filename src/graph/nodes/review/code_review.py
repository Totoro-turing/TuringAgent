"""
代码Review节点模块
实现代码质量评估和自动改进机制
完全符合LangGraph框架设计模式
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from langchain.schema.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt

from src.models.states import EDWState
from src.agent.edw_agents import get_shared_llm, get_shared_checkpointer

logger = logging.getLogger(__name__)


def code_review_node(state: EDWState) -> dict:
    """
    代码质量评估节点
    使用LLM对生成的代码进行多维度评估
    """
    try:
        # 提取需要review的代码
        enhanced_code = state.get("enhance_code", "")
        table_name = state.get("table_name", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        review_round = state.get("review_round", 0) + 1
        
        if not enhanced_code:
            logger.warning("没有需要review的代码")
            return {
                "review_score": 100,
                "review_feedback": "没有代码需要评估",
                "review_round": review_round,
                "user_id": user_id
            }
        
        # 构建review提示词
        review_prompt = _build_review_prompt(
            enhanced_code=enhanced_code,
            table_name=table_name,
            fields=fields,
            review_round=review_round
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
        
        response = llm.invoke(review_prompt)
        review_result = _parse_review_response(response.content if hasattr(response, 'content') else str(response))
        
        # 更新review历史
        review_history = state.get("review_history", [])
        review_history.append({
            "round": review_round,
            "score": review_result["score"],
            "feedback": review_result["feedback"],
            "suggestions": review_result["suggestions"],
            "has_critical_issues": review_result["has_critical_issues"],
            "timestamp": datetime.now().isoformat()
        })
        
        logger.info(f"代码Review完成 - 轮次: {review_round}, 评分: {review_result['score']}")
        
        return {
            "review_score": review_result["score"],
            "review_feedback": review_result["feedback"],
            "review_suggestions": review_result["suggestions"],
            "has_critical_issues": review_result["has_critical_issues"],
            "review_round": review_round,
            "review_history": review_history,
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
        
        # 构建改进提示词
        improvement_prompt = _build_improvement_prompt(
            current_code=current_code,
            review_feedback=review_feedback,
            review_suggestions=review_suggestions,
            original_requirements={
                "table_name": table_name,
                "fields": fields,
                "logic_detail": logic_detail
            }
        )
        
        # 直接await异步执行代码重新生成
        from src.graph.utils.enhancement import execute_code_enhancement_task
        
        regeneration_result = await execute_code_enhancement_task(
            enhancement_mode="review_improvement",
            current_code=current_code,
            improvement_prompt=improvement_prompt,
            table_name=table_name,
            source_code=source_code,
            adb_code_path=adb_code_path,
            fields=fields,
            logic_detail=logic_detail,
            code_path=code_path,
            user_id=user_id,
            review_feedback=review_feedback,
            review_suggestions=review_suggestions,
            state=state  # 传递state以支持Socket发送
        )
        
        if regeneration_result.get("success"):
            logger.info(f"代码重新生成成功 - 表: {table_name}")
            
            # 🎯 发送重新生成的代码到前端显示
            session_id = state.get("session_id", "unknown")
            from src.server.socket_manager import get_session_socket
            from datetime import datetime
            
            socket_queue = get_session_socket(session_id)
            if socket_queue:
                try:
                    socket_queue.send_message(
                        session_id,
                        "enhanced_code",
                        {
                            "type": "enhanced_code",
                            "content": regeneration_result.get("enhanced_code"),
                            "table_name": table_name,
                            "create_table_sql": regeneration_result.get("new_table_ddl", state.get("create_table_sql")),
                            "alter_table_sql": regeneration_result.get("alter_statements", state.get("alter_table_sql")),
                            "fields_count": len(fields) if fields else 0,
                            "enhancement_type": state.get("enhancement_type", ""),
                            "enhancement_mode": "review_improvement",  # 标记为review改进模式
                            "model_name": state.get("model_attribute_name", ""),
                            "file_path": code_path,
                            "adb_path": adb_code_path,
                            "optimization_summary": regeneration_result.get("optimization_summary", ""),
                            "review_round": state.get("review_round", 1),
                            "timestamp": datetime.now().isoformat()
                        }
                    )
                    logger.info(f"✅ Socket发送review改进代码成功: {table_name}")
                except Exception as e:
                    logger.warning(f"Socket发送review改进代码失败: {e}")
            
            return {
                "enhance_code": regeneration_result.get("enhanced_code"),
                "create_table_sql": regeneration_result.get("new_table_ddl", state.get("create_table_sql")),
                "alter_table_sql": regeneration_result.get("alter_statements", state.get("alter_table_sql")),
                "optimization_summary": regeneration_result.get("optimization_summary", ""),
                "user_id": user_id,
                "status": "regenerated",
                "status_message": "代码已根据review建议重新生成"
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
    has_critical = state.get("has_critical_issues", False)
    max_rounds = state.get("max_review_rounds", 3)
    
    logger.info(f"Review路由决策 - 评分: {review_score}, 轮次: {review_round}/{max_rounds}, 严重问题: {has_critical}")
    
    # 达到最大轮次，强制结束
    if review_round >= max_rounds:
        logger.info("达到最大review轮次，结束review流程")
        return END
    
    # 评分低于阈值或有严重问题，需要重新生成
    if review_score < 70 or has_critical:
        logger.info(f"需要重新生成代码 - 原因: {'评分过低' if review_score < 70 else '存在严重问题'}")
        return "regenerate"
    
    # 评分合格，结束review
    logger.info("代码质量合格，结束review流程")
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


def _build_review_prompt(enhanced_code: str, table_name: str, fields: list, review_round: int) -> str:
    """构建代码review提示词"""
    
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
    
    return f"""你是一个专业的代码质量评估专家，负责评估Databricks代码的质量。

**评估任务**：
表名: {table_name}
Review轮次: 第{review_round}轮
新增字段:
{fields_info}

**待评估代码**：
```python
{enhanced_code}
```

**评估维度**（每项20分，总分100分）：
1. **语法正确性** (20分)
   - 代码语法是否正确
   - 是否能正常执行
   - 有无明显错误

2. **逻辑完整性** (20分)
   - 业务逻辑是否完整
   - 字段处理是否正确
   - 数据流是否合理

3. **代码质量** (20分)
   - 代码结构是否清晰
   - 是否遵循最佳实践
   - 性能是否优化

4. **可维护性** (20分)
   - 代码可读性如何
   - 注释是否充分
   - 命名是否规范

5. **业务契合度** (20分)
   - 是否满足业务需求
   - 字段定义是否准确
   - 数据类型是否合适

**输出格式**（严格按JSON格式）：
{{
    "score": 总分(0-100),
    "feedback": "整体评价",
    "suggestions": ["改进建议1", "改进建议2", ...],
    "has_critical_issues": true/false,
    "critical_issues": ["严重问题1", "严重问题2", ...],
    "dimension_scores": {{
        "syntax": 分数,
        "logic": 分数,
        "quality": 分数,
        "maintainability": 分数,
        "business": 分数
    }}
}}

请进行专业评估并给出改进建议。"""


def _build_improvement_prompt(current_code: str, review_feedback: str, 
                              review_suggestions: list, original_requirements: dict) -> str:
    """构建代码改进提示词"""
    
    suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"
    
    return f"""你是一个代码优化专家，需要根据review反馈改进代码。

**Review反馈**：
{review_feedback}

**改进建议**：
{suggestions_text}

**原始需求**：
- 表名: {original_requirements.get('table_name', '')}
- 增强逻辑: {original_requirements.get('logic_detail', '')}
- 字段数量: {len(original_requirements.get('fields', []))}

**当前代码**：
```python
{current_code}
```

**任务**：
1. 根据review反馈和建议改进代码
2. 修复所有指出的问题
3. 保持原有功能不变
4. 提升代码质量

**输出要求**：严格按JSON格式返回
{{
    "enhanced_code": "改进后的完整代码",
    "new_table_ddl": "CREATE TABLE语句（如有变化）",
    "alter_statements": "ALTER语句（如有变化）",
    "optimization_summary": "本次改进的说明"
}}"""


def _parse_review_response(content: str) -> dict:
    """解析LLM的review响应"""
    import json
    import re
    
    default_result = {
        "score": 0,
        "feedback": "解析失败",
        "suggestions": [],
        "has_critical_issues": False,
        "critical_issues": []
    }
    
    try:
        # 尝试直接解析JSON
        result = json.loads(content.strip())
        return {
            "score": result.get("score", 0),
            "feedback": result.get("feedback", ""),
            "suggestions": result.get("suggestions", []),
            "has_critical_issues": result.get("has_critical_issues", False),
            "critical_issues": result.get("critical_issues", [])
        }
    except json.JSONDecodeError:
        # 尝试提取JSON代码块
        json_match = re.search(r'```json\n(.*?)\n```', content, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group(1).strip())
                return {
                    "score": result.get("score", 0),
                    "feedback": result.get("feedback", ""),
                    "suggestions": result.get("suggestions", []),
                    "has_critical_issues": result.get("has_critical_issues", False),
                    "critical_issues": result.get("critical_issues", [])
                }
            except json.JSONDecodeError:
                logger.warning("JSON代码块解析失败")
        
        # 尝试提取花括号内容
        brace_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        if brace_match:
            try:
                result = json.loads(brace_match.group(0))
                return {
                    "score": result.get("score", 0),
                    "feedback": result.get("feedback", ""),
                    "suggestions": result.get("suggestions", []),
                    "has_critical_issues": result.get("has_critical_issues", False),
                    "critical_issues": result.get("critical_issues", [])
                }
            except json.JSONDecodeError:
                logger.warning("花括号内容解析失败")
        
        # 解析失败，返回默认值
        logger.error(f"无法解析review响应: {content[:200]}...")
        return default_result