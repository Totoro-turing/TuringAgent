"""
邮件发送节点

使用异步工具发送模型评审邮件
"""

import logging
import asyncio
from typing import Dict, Any

from src.graph.tools.email_tools import send_model_review_email
from src.graph.utils.progress import send_node_start, send_node_processing, send_node_completed, send_node_failed

logger = logging.getLogger(__name__)


def edw_email_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    邮件发送节点 - 使用新的异步工具
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    # 🎯 发送节点开始进度
    send_node_start(state, "email", "开始发送模型评审邮件...")
    
    try:
        # 从state中获取相关信息
        table_name = state.get("table_name", "未知表")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        confluence_page_url = state.get("confluence_page_url", "")
        confluence_title = state.get("confluence_title", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        
        # 🎯 发送验证进度
        send_node_processing(state, "email", "验证邮件发送参数...", 0.1)
        
        logger.info(f"准备发送模型评审邮件: {table_name}")
        
        # 🎯 发送邮件准备进度
        send_node_processing(state, "email", f"正在为{table_name}准备评审邮件...", 0.3)
        
        # 异步执行邮件发送
        send_result = asyncio.run(send_model_review_email(
            table_name=table_name,
            model_name=model_name,
            fields=fields,
            confluence_url=confluence_page_url,
            confluence_title=confluence_title,
            enhancement_type=enhancement_type
        ))
        
        if send_result["success"]:
            logger.info(f"邮件发送成功: {table_name}")
            
            # 🎯 发送成功进度
            send_node_completed(
                state, 
                "email", 
                f"成功发送{table_name}的模型评审邮件",
                extra_data={
                    "table_name": table_name,
                    "email_subject": send_result.get("metadata", {}).get("email_subject", f"Model Review Request - {model_name or table_name}"),
                    "confluence_included": bool(confluence_page_url)
                }
            )
            
            # 构建成功响应
            return {
                "user_id": user_id,
                "email_sent": True,
                "email_format": "HTML",
                "email_subject": send_result.get("metadata", {}).get("email_subject", f"Model Review Request - {model_name or table_name}"),
                "confluence_link_included": bool(confluence_page_url),
                "confluence_page_url": confluence_page_url,
                "session_state": "email_sent"
            }
        else:
            error_msg = send_result.get("error", "邮件发送失败")
            logger.error(f"邮件发送失败: {error_msg}")
            # 🎯 发送失败进度
            send_node_failed(state, "email", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "email_sent": False
            }
            
    except Exception as e:
        error_msg = f"邮件节点处理失败: {str(e)}"
        logger.error(error_msg)
        # 🎯 发送异常失败进度
        send_node_failed(state, "email", error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "email_sent": False
        }


async def edw_email_node_async(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    邮件发送节点的异步版本
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    try:
        # 从state中获取相关信息
        table_name = state.get("table_name", "未知表")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        confluence_page_url = state.get("confluence_page_url", "")
        confluence_title = state.get("confluence_title", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        
        logger.info(f"准备发送模型评审邮件: {table_name}")
        
        # 异步执行邮件发送
        send_result = await send_model_review_email(
            table_name=table_name,
            model_name=model_name,
            fields=fields,
            confluence_url=confluence_page_url,
            confluence_title=confluence_title,
            enhancement_type=enhancement_type
        )
        
        if send_result["success"]:
            logger.info(f"邮件发送成功: {table_name}")
            
            # 构建成功响应
            return {
                "user_id": user_id,
                "email_sent": True,
                "email_format": "HTML",
                "email_subject": send_result.get("metadata", {}).get("email_subject", f"Model Review Request - {model_name or table_name}"),
                "confluence_link_included": bool(confluence_page_url),
                "confluence_page_url": confluence_page_url,
                "session_state": "email_sent"
            }
        else:
            error_msg = send_result.get("error", "邮件发送失败")
            logger.error(f"邮件发送失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "email_sent": False
            }
            
    except Exception as e:
        error_msg = f"邮件节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "email_sent": False
        }


__all__ = ['edw_email_node', 'edw_email_node_async']