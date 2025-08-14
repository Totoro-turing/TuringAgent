"""
JIRA工具模块

提供JIRA问题跟踪和状态管理的异步工具
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from .base import AsyncBaseTool, create_tool_result, run_with_timeout

logger = logging.getLogger(__name__)


async def update_jira_task_status(
    issue_key: str,
    status: str,
    context: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    异步更新JIRA任务状态
    
    Args:
        issue_key: JIRA问题键值
        status: EDW状态 (pending, in_progress, review, testing, completed, etc.)
        context: 工作流上下文信息
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备更新JIRA任务状态: {issue_key} -> {status}")
        
        from src.basic.jira.jira_tools import JiraWorkflowTools
        
        # 创建JIRA工具实例
        tools = JiraWorkflowTools()
        
        # 更新任务状态
        result = await run_with_timeout(
            tools.update_edw_task_status(issue_key, status, context),
            timeout=30.0,
            timeout_message="更新JIRA任务状态超时"
        )
        
        if result["success"]:
            logger.info(f"JIRA任务状态更新成功: {issue_key}")
            return create_tool_result(
                True,
                result=result,
                issue_key=issue_key,
                new_status=status,
                update_time=datetime.now().isoformat()
            )
        else:
            error_msg = result.get("error", "状态更新失败")
            logger.error(f"JIRA任务状态更新失败: {error_msg}")
            return create_tool_result(False, error=error_msg)
            
    except TimeoutError as e:
        error_msg = str(e)
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)
    except Exception as e:
        error_msg = f"更新JIRA任务状态失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def add_jira_progress_comment(
    issue_key: str,
    action: str,
    details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    异步添加JIRA进度评论
    
    Args:
        issue_key: JIRA问题键值
        action: 执行的操作
        details: 详细信息
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备添加JIRA进度评论: {issue_key}")
        
        from src.basic.jira.jira_tools import JiraWorkflowTools
        
        # 创建JIRA工具实例
        tools = JiraWorkflowTools()
        
        # 添加进度评论
        result = await run_with_timeout(
            tools.add_edw_progress_comment(issue_key, action, details),
            timeout=30.0,
            timeout_message="添加JIRA评论超时"
        )
        
        if result["success"]:
            logger.info(f"JIRA进度评论添加成功: {issue_key}")
            return create_tool_result(
                True,
                result=result,
                issue_key=issue_key,
                action=action,
                comment_time=datetime.now().isoformat()
            )
        else:
            error_msg = result.get("error", "评论添加失败")
            logger.error(f"JIRA进度评论添加失败: {error_msg}")
            return create_tool_result(False, error=error_msg)
            
    except TimeoutError as e:
        error_msg = str(e)
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)
    except Exception as e:
        error_msg = f"添加JIRA进度评论失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def create_model_enhancement_comment(
    issue_key: str,
    table_name: str,
    enhancement_details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    异步创建模型增强评论
    
    Args:
        issue_key: JIRA问题键值
        table_name: 表名
        enhancement_details: 增强详情
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备创建模型增强评论: {issue_key} - {table_name}")
        
        from src.basic.jira.jira_tools import JiraWorkflowTools
        
        # 创建JIRA工具实例
        tools = JiraWorkflowTools()
        
        # 创建模型增强评论
        result = await run_with_timeout(
            tools.create_model_enhancement_comment(issue_key, table_name, enhancement_details),
            timeout=30.0,
            timeout_message="创建模型增强评论超时"
        )
        
        if result["success"]:
            logger.info(f"模型增强评论创建成功: {issue_key}")
            return create_tool_result(
                True,
                result=result,
                issue_key=issue_key,
                table_name=table_name,
                comment_time=datetime.now().isoformat()
            )
        else:
            error_msg = result.get("error", "模型增强评论创建失败")
            logger.error(f"模型增强评论创建失败: {error_msg}")
            return create_tool_result(False, error=error_msg)
            
    except TimeoutError as e:
        error_msg = str(e)
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)
    except Exception as e:
        error_msg = f"创建模型增强评论失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def complete_edw_task(
    issue_key: str,
    completion_details: Dict[str, Any]
) -> Dict[str, Any]:
    """
    异步完成EDW任务
    
    Args:
        issue_key: JIRA问题键值
        completion_details: 完成详情
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备完成EDW任务: {issue_key}")
        
        from src.basic.jira.jira_tools import JiraWorkflowTools
        
        # 创建JIRA工具实例
        tools = JiraWorkflowTools()
        
        # 完成EDW任务
        result = await run_with_timeout(
            tools.complete_edw_task(issue_key, completion_details),
            timeout=60.0,
            timeout_message="完成EDW任务超时"
        )
        
        if result["success"]:
            logger.info(f"EDW任务完成成功: {issue_key}")
            return create_tool_result(
                True,
                result=result,
                issue_key=issue_key,
                completion_time=datetime.now().isoformat()
            )
        else:
            error_msg = result.get("error", "EDW任务完成失败")
            logger.error(f"EDW任务完成失败: {error_msg}")
            return create_tool_result(False, error=error_msg)
            
    except TimeoutError as e:
        error_msg = str(e)
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)
    except Exception as e:
        error_msg = f"完成EDW任务失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


class JiraStatusUpdateInput(BaseModel):
    """JIRA状态更新工具的输入参数"""
    issue_key: str = Field(description="JIRA问题键值")
    status: str = Field(description="EDW状态")
    table_name: Optional[str] = Field(default="", description="相关表名")
    message: Optional[str] = Field(default="", description="状态更新消息")


class JiraStatusUpdateTool(AsyncBaseTool):
    """
    JIRA状态更新工具
    
    用于更新JIRA问题的状态
    """
    name: str = "update_jira_status"
    description: str = "更新JIRA问题状态"
    args_schema: type[BaseModel] = JiraStatusUpdateInput
    
    async def _arun(
        self,
        issue_key: str,
        status: str,
        table_name: str = "",
        message: str = "",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行状态更新"""
        context = {
            "table_name": table_name,
            "message": message
        }
        
        result = await update_jira_task_status(issue_key, status, context)
        
        if result["success"]:
            return f"JIRA状态更新成功: {issue_key} -> {status}"
        else:
            return f"JIRA状态更新失败: {result.get('error', '未知错误')}"


class JiraCommentInput(BaseModel):
    """JIRA评论工具的输入参数"""
    issue_key: str = Field(description="JIRA问题键值")
    action: str = Field(description="执行的操作")
    table_name: Optional[str] = Field(default="", description="相关表名")
    success: bool = Field(default=True, description="操作是否成功")
    description: Optional[str] = Field(default="", description="详细描述")
    fields_added: Optional[int] = Field(default=0, description="新增字段数量")
    confluence_url: Optional[str] = Field(default="", description="Confluence文档链接")


class JiraCommentTool(AsyncBaseTool):
    """
    JIRA评论工具
    
    用于为JIRA问题添加进度评论
    """
    name: str = "add_jira_comment"
    description: str = "为JIRA问题添加进度评论"
    args_schema: type[BaseModel] = JiraCommentInput
    
    async def _arun(
        self,
        issue_key: str,
        action: str,
        table_name: str = "",
        success: bool = True,
        description: str = "",
        fields_added: int = 0,
        confluence_url: str = "",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行评论添加"""
        details = {
            "table_name": table_name,
            "success": success,
            "description": description,
            "fields_added": fields_added,
            "confluence_url": confluence_url
        }
        
        result = await add_jira_progress_comment(issue_key, action, details)
        
        if result["success"]:
            return f"JIRA评论添加成功: {issue_key}"
        else:
            return f"JIRA评论添加失败: {result.get('error', '未知错误')}"


class JiraTaskCompletionInput(BaseModel):
    """JIRA任务完成工具的输入参数"""
    issue_key: str = Field(description="JIRA问题键值")
    table_name: Optional[str] = Field(default="", description="相关表名")
    summary: Optional[str] = Field(default="", description="完成总结")
    confluence_url: Optional[str] = Field(default="", description="Confluence文档链接")
    fields_added: Optional[int] = Field(default=0, description="新增字段数量")
    code_lines: Optional[int] = Field(default=0, description="生成代码行数")


class JiraTaskCompletionTool(AsyncBaseTool):
    """
    JIRA任务完成工具
    
    用于完成EDW任务（更新状态为完成并添加总结评论）
    """
    name: str = "complete_jira_task"
    description: str = "完成EDW任务"
    args_schema: type[BaseModel] = JiraTaskCompletionInput
    
    async def _arun(
        self,
        issue_key: str,
        table_name: str = "",
        summary: str = "",
        confluence_url: str = "",
        fields_added: int = 0,
        code_lines: int = 0,
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行任务完成"""
        completion_details = {
            "table_name": table_name,
            "summary": summary,
            "confluence_url": confluence_url,
            "fields_added": fields_added,
            "code_lines": code_lines,
            "success": True
        }
        
        result = await complete_edw_task(issue_key, completion_details)
        
        if result["success"]:
            return f"EDW任务完成成功: {issue_key}"
        else:
            return f"EDW任务完成失败: {result.get('error', '未知错误')}"


__all__ = [
    'update_jira_task_status',
    'add_jira_progress_comment', 
    'create_model_enhancement_comment',
    'complete_edw_task',
    'JiraStatusUpdateTool',
    'JiraCommentTool',
    'JiraTaskCompletionTool'
]