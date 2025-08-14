"""
代码微调执行节点
执行代码微调任务
"""

import logging
from datetime import datetime
from src.models.states import EDWState

logger = logging.getLogger(__name__)


async def code_refinement_node(state: EDWState):
    """代码微调执行节点 - 复用增强引擎"""
    
    user_id = state.get("user_id", "")
    
    try:
        # 导入执行任务函数（从独立的utils模块）
        from src.graph.utils.enhancement import execute_code_enhancement_task
        
        # 使用微调模式的增强引擎 - 参数从state中获取
        refinement_result = await execute_code_enhancement_task(
            enhancement_mode="refinement",
            state=state
        )
        
        if refinement_result.get("success"):
            # 更新微调轮次
            current_round = state.get("current_refinement_round", 1)
            
            # 记录微调历史
            refinement_history = state.get("refinement_history", [])
            refinement_history.append({
                "round": current_round,
                "user_feedback": state.get("refinement_requirements", ""),
                "old_code": (state.get("enhance_code", "") or "")[:200] + "...",
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


