"""
代码增强工具函数
包含代码增强、微调和改进的核心逻辑
"""

import logging
from typing import Dict, Any
from langchain.schema.messages import HumanMessage
from src.graph.utils.session import SessionManager
from src.graph.utils.code import parse_agent_response

logger = logging.getLogger(__name__)


async def execute_code_enhancement_task(enhancement_mode: str, **kwargs) -> dict:
    """统一的代码增强执行引擎 - 支持不同模式的提示词"""
    try:
        # 首先获取state，以便在构建提示词时使用
        state = kwargs.get("state")  # 尝试获取state，可能为None
        
        # 根据模式选择不同的提示词构建策略
        if enhancement_mode == "initial_enhancement":
            task_message = build_initial_enhancement_prompt(**kwargs)
        elif enhancement_mode == "refinement":
            # 从state中获取微调所需的参数
            if state:
                current_code = state.get("enhance_code", "")
                table_name = state.get("table_name", "")
                user_feedback = state.get("refinement_requirements", "")
                original_context = {
                    "logic_detail": state.get("logic_detail", ""),
                    "fields_info": format_fields_info(state.get("fields", []))
                }
                task_message = build_refinement_prompt(
                    current_code=current_code,
                    user_feedback=user_feedback,
                    table_name=table_name,
                    original_context=original_context,
                    **kwargs
                )
            else:
                task_message = build_refinement_prompt(**kwargs)
        elif enhancement_mode == "review_improvement":
            task_message = build_review_improvement_prompt(**kwargs)
        else:
            raise ValueError(f"不支持的增强模式: {enhancement_mode}")
        
        # 从智能体管理器获取代码增强智能体
        from src.agent.edw_agents import get_code_enhancement_agent, get_code_enhancement_tools
        enhancement_agent = get_code_enhancement_agent()
        tools = get_code_enhancement_tools()
        
        # 使用配置管理器获取配置 - 为每个用户生成独立的thread_id
        # 优先从state中获取参数，如果state不存在则从kwargs中获取
        if state:
            table_name = state.get("table_name", "unknown")
            user_id = state.get("user_id", "")
        else:
            table_name = kwargs.get("table_name", "unknown")
            user_id = kwargs.get("user_id", "")
        config = SessionManager.get_config_with_monitor(
            user_id=user_id,
            agent_type=f"enhancement_{table_name}",
            state=state,
            node_name="code_enhancement",
            enhanced_monitoring=True
        )
        
        # 调用全局智能体执行增强任务（异步调用以支持MCP工具）
        result = await enhancement_agent.ainvoke(
            {"messages": [HumanMessage(task_message)]},
            config
        )
        
        # 解析智能体的响应
        response_content = result["messages"][-1].content
        enhancement_result = parse_agent_response(response_content)
        
        if enhancement_result.get("enhanced_code"):
            logger.info(f"代码增强成功 ({enhancement_mode}): {table_name}")
            
            # 🎯 发送增强代码到前端显示（适用于所有增强模式）
            if state:
                session_id = state.get("session_id", "unknown")
                from src.server.socket_manager import get_session_socket
                from datetime import datetime
                
                socket_queue = get_session_socket(session_id)
                
                if socket_queue:
                    try:
                        # 获取额外信息
                        fields = state.get("fields", kwargs.get("fields", []))
                        fields_count = len(fields) if fields else 0
                        enhancement_type = state.get("enhancement_type", "")
                        model_name = state.get("model_attribute_name", "")
                        code_path = kwargs.get("code_path", state.get("code_path", ""))
                        adb_path = kwargs.get("adb_code_path", state.get("adb_code_path", ""))
                        
                        socket_queue.send_message(
                            session_id,
                            "enhanced_code",
                            {
                                "type": "enhanced_code",
                                "content": enhancement_result.get("enhanced_code"),
                                "table_name": table_name,
                                "create_table_sql": enhancement_result.get("new_table_ddl"),
                                "alter_table_sql": enhancement_result.get("alter_statements"),
                                "fields_count": fields_count,
                                "enhancement_type": enhancement_type,
                                "enhancement_mode": enhancement_mode,  # 标记是初始增强还是微调
                                "model_name": model_name,
                                "file_path": code_path,
                                "adb_path": adb_path,
                                "optimization_summary": enhancement_result.get("optimization_summary", ""),
                                "timestamp": datetime.now().isoformat()
                            }
                        )
                        logger.info(f"✅ Socket发送增强代码成功: {table_name} (模式: {enhancement_mode}, 长度: {len(enhancement_result.get('enhanced_code', ''))} 字符)")
                    except Exception as e:
                        logger.warning(f"Socket发送增强代码失败: {e}")
                else:
                    if not socket_queue:
                        logger.debug(f"Socket队列不存在: {session_id}")
            
            return {
                "success": True,
                "enhanced_code": enhancement_result.get("enhanced_code"),
                "new_table_ddl": enhancement_result.get("new_table_ddl"),
                "alter_statements": enhancement_result.get("alter_statements"),
                "table_comment": enhancement_result.get("table_comment"),
                "optimization_summary": enhancement_result.get("optimization_summary", ""),
                "field_mappings": kwargs.get("fields", [])
            }
        else:
            error_msg = f"智能体未能生成有效的增强代码 ({enhancement_mode})"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
    
    except Exception as e:
        error_msg = f"执行代码增强时发生异常 ({enhancement_mode}): {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    finally:
        # MCP客户端使用上下文管理器，无需手动清理
        logger.debug(f"代码增强任务完成 ({enhancement_mode})")


def build_initial_enhancement_prompt(table_name: str, source_code: str, adb_code_path: str,
                                     fields: list, logic_detail: str, code_path: str = "", **kwargs) -> str:
    """构建初始模型增强的提示词 - 完整流程"""
    
    # 判断代码类型
    file_path = code_path or adb_code_path or ""
    if file_path.endswith('.sql'):
        code_language = "sql"
        code_type_desc = "SQL"
    else:
        code_language = "python"
        code_type_desc = "Python"
    
    # 构造字段信息字符串
    fields_info = []
    for field in fields:
        if isinstance(field, dict):
            physical_name = field['physical_name']
            attribute_name = field['attribute_name']
        else:
            physical_name = field.physical_name
            attribute_name = field.attribute_name
        fields_info.append(f"{physical_name} ({attribute_name})")
    
    return f"""你是一个Databricks代码增强专家，负责为数据模型添加新字段。

**任务目标**: 为表 {table_name} 创建增强版本的{code_type_desc}代码

**增强需求**: {logic_detail}

**新增字段**:
{chr(10).join(fields_info)}

**原始源代码**:
```{code_language.lower()}
{source_code}
```

**执行步骤**:
1.  使用execute_sql工具查询目标表结构: `DESCRIBE {table_name}`
2. 分析源代码中的底表，查询底表结构结合用户逻辑来推断新字段的数据类型
3. 基于原始代码生成增强版本，确保新字段逻辑正确
4. 生成完整的CREATE TABLE和ALTER TABLE语句

**输出要求**: 严格按JSON格式返回
{{
    "enhanced_code": "完整的增强后{code_type_desc}代码",
    "new_table_ddl": "包含新字段的CREATE TABLE语句", 
    "alter_statements": "ADD COLUMN的ALTER语句"
}}"""


def build_refinement_prompt(current_code: str, user_feedback: str, table_name: str,
                           original_context: dict, **kwargs) -> str:
    """构建代码微调的提示词 - 针对性优化"""
    
    return f"""你是一个代码优化专家，负责根据用户反馈修改AI生成的代码。
**用户反馈**: "{user_feedback}"

**优化指导原则**:
1. 重点关注用户的具体反馈，精准响应用户需求
2. 如需查询额外信息，可使用工具
3. 优化可能包括：性能改进、代码可读性、异常处理、注释补充等、属性名称修改、字段顺序修改

**注意事项**:
- 不要重新设计整体架构，只做针对性改进
- 保持与原代码的语言风格一致
- 确保修改后的代码逻辑正确且可执行
- ALTER语句如果有需要请重新生成，需满足alter table ** add column ** comment '' after '';

**输出格式**: 严格按JSON格式返回
{{
    "enhanced_code": "优化后的代码",
    "new_table_ddl": "CREATE TABLE语句（如有需要）",
    "alter_statements": "ALTER语句（如有需要）",
    "optimization_summary": "本次优化的具体改进点说明"
}}"""


def build_review_improvement_prompt(improvement_prompt: str, **kwargs) -> str:
    """构建基于review反馈的代码改进提示词"""
    # 如果已经提供了完整的improvement_prompt，直接使用
    if improvement_prompt:
        return improvement_prompt
    
    # 否则构建默认的改进提示词
    current_code = kwargs.get("current_code", "")
    review_feedback = kwargs.get("review_feedback", "")
    review_suggestions = kwargs.get("review_suggestions", [])
    table_name = kwargs.get("table_name", "")
    
    suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"
    
    return f"""你是一个代码质量改进专家，负责根据代码review反馈改进代码。

**Review反馈**: {review_feedback}

**改进建议**:
{suggestions_text}

**表名**: {table_name}

**当前代码**:
```python
{current_code}
```

**改进要求**:
1. 根据review反馈修复所有问题
2. 实施所有合理的改进建议
3. 保持代码功能不变
4. 提升代码质量和可维护性
5. 如需查询额外信息，可使用工具

**输出格式**: 严格按JSON格式返回
{{
    "enhanced_code": "改进后的完整代码",
    "new_table_ddl": "CREATE TABLE语句（如有变化）",
    "alter_statements": "ALTER语句（如有变化）",
    "optimization_summary": "本次改进的具体内容说明"
}}"""


def format_fields_info(fields: list) -> str:
    """格式化字段信息为字符串"""
    if not fields:
        return "无字段信息"
    
    fields_info = []
    for field in fields:
        if isinstance(field, dict):
            name = field.get('physical_name', '')
            attr = field.get('attribute_name', '')
        else:
            name = getattr(field, 'physical_name', '')
            attr = getattr(field, 'attribute_name', '')
        
        if name and attr:
            fields_info.append(f"{name} ({attr})")
        elif name:
            fields_info.append(name)
    
    return ', '.join(fields_info) if fields_info else "无字段信息"