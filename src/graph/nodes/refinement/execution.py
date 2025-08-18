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
            
            # 🎯 发送微调后的代码到前端显示
            session_id = state.get("session_id", "unknown")
            from src.server.socket_manager import get_session_socket
            
            socket_queue = get_session_socket(session_id)
            if socket_queue:
                try:
                    socket_queue.send_message(
                        session_id,
                        "enhanced_code",
                        {
                            "type": "enhanced_code",
                            "content": refinement_result.get("enhanced_code"),
                            "table_name": state.get("table_name", ""),
                            "create_table_sql": refinement_result.get("new_table_ddl", state.get("create_table_sql")),
                            "alter_table_sql": refinement_result.get("alter_statements", state.get("alter_table_sql")),
                            "fields_count": len(state.get("fields", [])),
                            "enhancement_type": state.get("enhancement_type", ""),
                            "enhancement_mode": "refinement",  # 标记为微调模式
                            "model_name": state.get("model_attribute_name", ""),
                            "file_path": state.get("code_path", ""),
                            "adb_path": state.get("adb_code_path", ""),
                            "optimization_summary": refinement_result.get("optimization_summary", ""),
                            "refinement_round": current_round,
                            "timestamp": datetime.now().isoformat()
                        }
                    )
                    logger.info(f"✅ Socket发送微调代码成功 (第{current_round}轮)")
                except Exception as e:
                    logger.warning(f"Socket发送微调代码失败: {e}")
            
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


