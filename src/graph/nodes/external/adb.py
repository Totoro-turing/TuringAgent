"""
ADB更新节点

使用异步工具更新Databricks笔记本
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, Any

from src.graph.tools.adb_tools import update_adb_notebook, detect_code_language
from src.graph.utils.progress import send_node_start, send_node_processing, send_node_completed, send_node_failed

logger = logging.getLogger(__name__)


def edw_adb_update_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    ADB更新节点 - 使用新的异步工具
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    # 🎯 发送节点开始进度
    send_node_start(state, "adb_update", "开始更新ADB笔记本...")
    
    try:
        # 提取状态中的信息
        adb_code_path = state.get("adb_code_path")
        enhanced_code = state.get("enhance_code")
        code_path = state.get("code_path")
        source_code = state.get("source_code", "")
        user_id = state.get("user_id", "")
        table_name = state.get("table_name")
        
        # 🎯 发送验证进度
        send_node_processing(state, "adb_update", "验证ADB更新参数...", 0.1)
        
        # 验证必要参数
        if not adb_code_path:
            error_msg = "缺少ADB代码路径"
            logger.error(error_msg)
            send_node_failed(state, "adb_update", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        if not enhanced_code:
            error_msg = "缺少增强后的代码"
            logger.error(error_msg)
            send_node_failed(state, "adb_update", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        # 🎯 发送检测进度
        send_node_processing(state, "adb_update", "检测代码语言和准备更新...", 0.3)
        
        # 检测代码语言
        language = detect_code_language(code_path or adb_code_path, source_code)
        logger.info(f"检测到代码语言: {language}")
        
        # 🎯 发送更新进度
        send_node_processing(state, "adb_update", f"正在更新ADB笔记本: {adb_code_path}", 0.7)
        
        # 异步执行ADB更新
        update_result = asyncio.run(update_adb_notebook(
            path=adb_code_path,
            content=enhanced_code,
            language=language,
            overwrite=True
        ))
        
        if update_result["success"]:
            logger.info(f"ADB笔记本更新成功: {adb_code_path}")
            
            # 🎯 发送成功进度
            send_node_completed(
                state, 
                "adb_update", 
                f"成功更新ADB笔记本: {adb_code_path}",
                extra_data={
                    "adb_path": adb_code_path,
                    "language": language,
                    "table_name": table_name
                }
            )
            
            return {
                "user_id": user_id,
                "adb_update_result": update_result,
                "adb_path_updated": adb_code_path,
                "code_language": language,
                "update_timestamp": datetime.now().isoformat(),
                "session_state": "adb_update_completed"
            }
        else:
            error_msg = update_result.get("error", "未知错误")
            logger.error(f"ADB更新失败: {error_msg}")
            # 🎯 发送失败进度
            send_node_failed(state, "adb_update", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "adb_path": adb_code_path
            }
            
    except Exception as e:
        error_msg = f"ADB更新节点处理失败: {str(e)}"
        logger.error(error_msg)
        # 🎯 发送异常失败进度
        send_node_failed(state, "adb_update", error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


async def edw_adb_update_node_async(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    ADB更新节点的异步版本
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    try:
        # 提取状态中的信息
        adb_code_path = state.get("adb_code_path")
        enhanced_code = state.get("enhance_code")
        code_path = state.get("code_path")
        source_code = state.get("source_code", "")
        user_id = state.get("user_id", "")
        
        # 验证必要参数
        if not adb_code_path:
            error_msg = "缺少ADB代码路径"
            logger.error(error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        if not enhanced_code:
            error_msg = "缺少增强后的代码"
            logger.error(error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        # 检测代码语言
        language = detect_code_language(code_path or adb_code_path, source_code)
        
        # 异步执行ADB更新
        update_result = await update_adb_notebook(
            path=adb_code_path,
            content=enhanced_code,
            language=language,
            overwrite=True
        )
        
        if update_result["success"]:
            logger.info(f"ADB笔记本更新成功: {adb_code_path}")
            
            return {
                "user_id": user_id,
                "adb_update_result": update_result,
                "adb_path_updated": adb_code_path,
                "code_language": language,
                "update_timestamp": datetime.now().isoformat(),
                "session_state": "adb_update_completed"
            }
        else:
            error_msg = update_result.get("error", "未知错误")
            logger.error(f"ADB更新失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "adb_path": adb_code_path
            }
            
    except Exception as e:
        error_msg = f"ADB更新节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


__all__ = ['edw_adb_update_node', 'edw_adb_update_node_async']