"""
Confluence文档节点

使用异步工具创建和管理Confluence文档
"""

import logging
import asyncio
from typing import Dict, Any

from src.graph.tools.confluence_tools import create_model_documentation
from src.graph.utils.progress import send_node_start, send_node_processing, send_node_completed, send_node_failed

logger = logging.getLogger(__name__)


def edw_confluence_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Confluence文档节点 - 使用新的异步工具
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    # 🎯 发送节点开始进度
    send_node_start(state, "confluence", "开始创建Confluence文档...")
    
    try:
        # 提取状态中的信息
        table_name = state.get("table_name", "")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        enhanced_code = state.get("enhance_code", "")
        alter_table_sql = state.get("alter_table_sql", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        base_tables = state.get("base_tables", [])
        
        # 🎯 发送验证进度
        send_node_processing(state, "confluence", "验证文档创建参数...", 0.1)
        
        # 验证必要信息
        if not table_name:
            error_msg = "缺少表名信息，无法创建Confluence文档"
            logger.error(error_msg)
            send_node_failed(state, "confluence", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        if not enhanced_code:
            logger.warning("缺少增强代码，将创建基础文档")
        
        if not fields:
            logger.warning("没有新增字段信息，将创建基础文档")
        
        logger.info(f"准备创建Confluence文档: {table_name}")
        
        # 🎯 发送文档创建进度
        send_node_processing(state, "confluence", f"正在为{table_name}创建Confluence文档...", 0.5)
        
        # 异步执行Confluence文档创建
        confluence_result = asyncio.run(create_model_documentation(
            table_name=table_name,
            model_name=model_name,
            enhanced_code=enhanced_code,
            fields=fields,
            alter_table_sql=alter_table_sql,
            enhancement_type=enhancement_type,
            base_tables=base_tables,
            user_id=user_id
        ))
        
        if confluence_result["success"]:
            logger.info("Confluence文档创建成功")
            
            # 获取文档信息
            result_data = confluence_result.get("result", {})
            confluence_page_url = result_data.get("page_url", "") if isinstance(result_data, dict) else ""
            confluence_page_id = result_data.get("page_id", "") if isinstance(result_data, dict) else ""
            confluence_title = result_data.get("page_title", "") if isinstance(result_data, dict) else ""
            
            # 从metadata中也尝试获取
            metadata = confluence_result.get("metadata", {})
            if not confluence_page_url:
                confluence_page_url = metadata.get("page_url", "")
            if not confluence_page_id:
                confluence_page_id = metadata.get("page_id", "")
            if not confluence_title:
                confluence_title = metadata.get("page_title", "")
            
            # 🎯 发送成功进度
            send_node_completed(
                state, 
                "confluence", 
                f"成功创建Confluence文档: {confluence_title or table_name}",
                extra_data={
                    "page_url": confluence_page_url,
                    "page_id": confluence_page_id,
                    "table_name": table_name
                }
            )
            
            return {
                "user_id": user_id,
                # 将Confluence信息保存到state中供后续节点使用
                "confluence_page_url": confluence_page_url,
                "confluence_page_id": confluence_page_id,
                "confluence_title": confluence_title,
                # 其他详细结果
                "confluence_result": confluence_result,
                "confluence_creation_time": metadata.get("creation_time", ""),
                "session_state": "confluence_completed"
            }
        else:
            error_msg = confluence_result.get("error", "未知错误")
            logger.error(f"Confluence文档创建失败: {error_msg}")
            # 🎯 发送失败进度
            send_node_failed(state, "confluence", error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "confluence_attempted": True
            }
            
    except Exception as e:
        error_msg = f"Confluence节点处理失败: {str(e)}"
        logger.error(error_msg)
        # 🎯 发送异常失败进度
        send_node_failed(state, "confluence", error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


async def edw_confluence_node_async(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Confluence文档节点的异步版本
    
    Args:
        state: EDW状态字典
    
    Returns:
        更新后的状态
    """
    try:
        # 提取状态中的信息
        table_name = state.get("table_name", "")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        enhanced_code = state.get("enhance_code", "")
        alter_table_sql = state.get("alter_table_sql", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        base_tables = state.get("base_tables", [])
        
        # 验证必要信息
        if not table_name:
            error_msg = "缺少表名信息，无法创建Confluence文档"
            logger.error(error_msg)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        logger.info(f"准备创建Confluence文档: {table_name}")
        
        # 异步执行Confluence文档创建
        confluence_result = await create_model_documentation(
            table_name=table_name,
            model_name=model_name,
            enhanced_code=enhanced_code,
            fields=fields,
            alter_table_sql=alter_table_sql,
            enhancement_type=enhancement_type,
            base_tables=base_tables,
            user_id=user_id
        )
        
        if confluence_result["success"]:
            logger.info("Confluence文档创建成功")
            
            # 获取文档信息
            result_data = confluence_result.get("result", {})
            confluence_page_url = result_data.get("page_url", "") if isinstance(result_data, dict) else ""
            confluence_page_id = result_data.get("page_id", "") if isinstance(result_data, dict) else ""
            confluence_title = result_data.get("page_title", "") if isinstance(result_data, dict) else ""
            
            # 从metadata中也尝试获取
            metadata = confluence_result.get("metadata", {})
            if not confluence_page_url:
                confluence_page_url = metadata.get("page_url", "")
            if not confluence_page_id:
                confluence_page_id = metadata.get("page_id", "")
            if not confluence_title:
                confluence_title = metadata.get("page_title", "")
            
            return {
                "user_id": user_id,
                # 将Confluence信息保存到state中供后续节点使用
                "confluence_page_url": confluence_page_url,
                "confluence_page_id": confluence_page_id,
                "confluence_title": confluence_title,
                # 其他详细结果
                "confluence_result": confluence_result,
                "confluence_creation_time": metadata.get("creation_time", ""),
                "session_state": "confluence_completed"
            }
        else:
            error_msg = confluence_result.get("error", "未知错误")
            logger.error(f"Confluence文档创建失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "confluence_attempted": True
            }
            
    except Exception as e:
        error_msg = f"Confluence节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


__all__ = ['edw_confluence_node', 'edw_confluence_node_async']