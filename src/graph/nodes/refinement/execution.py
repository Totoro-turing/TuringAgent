"""
代码微调执行节点
执行代码微调任务
"""

import logging
import asyncio
from datetime import datetime
from src.models.states import EDWState

logger = logging.getLogger(__name__)


def code_refinement_node(state: EDWState):
    """代码微调执行节点 - 复用增强引擎"""
    
    # 获取微调需求
    refinement_requirements = state.get("refinement_requirements", "")
    current_code = state.get("enhance_code", "")
    table_name = state.get("table_name", "")
    user_id = state.get("user_id", "")
    
    # 构建原始上下文信息
    original_context = {
        "logic_detail": state.get("logic_detail", ""),
        "fields_info": _format_fields_info(state.get("fields", []))
    }
    
    try:
        # 导入执行任务函数（避免循环导入）
        from src.graph.edw_graph import _execute_code_enhancement_task
        
        # 使用微调模式的增强引擎
        refinement_result = asyncio.run(_execute_code_enhancement_task(
            enhancement_mode="refinement",
            current_code=current_code,
            user_feedback=refinement_requirements,
            table_name=table_name,
            original_context=original_context,
            user_id=user_id
        ))
        
        if refinement_result.get("success"):
            # 更新微调轮次
            current_round = state.get("current_refinement_round", 1)
            
            # 记录微调历史
            refinement_history = state.get("refinement_history", [])
            refinement_history.append({
                "round": current_round,
                "user_feedback": refinement_requirements,
                "old_code": current_code[:200] + "...",
                "optimization_summary": refinement_result.get("optimization_summary", ""),
                "timestamp": datetime.now().isoformat()
            })
            
            return {
                "enhance_code": refinement_result["enhanced_code"],  # 更新代码
                "create_table_sql": refinement_result.get("new_table_ddl", state.get("create_table_sql")),
                "alter_table_sql": refinement_result.get("alter_statements", state.get("alter_table_sql")),
                "refinement_completed": True,
                "current_refinement_round": current_round + 1,
                "refinement_history": refinement_history,
                "optimization_summary": refinement_result.get("optimization_summary", ""),
                "user_id": user_id
            }
        else:
            # 微调失败，使用原代码
            error_msg = refinement_result.get("error", "微调失败")
            logger.error(f"代码微调失败: {error_msg}")
            
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": f"代码微调失败: {error_msg}",
                "status_details": {"refinement_result": refinement_result},
                "error_message": error_msg  # 向后兼容
            }
            
    except Exception as e:
        error_msg = f"微调节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "user_id": user_id,
            "status": "error",
            "status_message": error_msg,
            "status_details": {"exception": str(e)},
            "error_message": error_msg  # 向后兼容
        }


def _format_fields_info(fields: list) -> str:
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