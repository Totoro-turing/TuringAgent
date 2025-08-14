"""
JIRA集成工具

为工作流提供JIRA问题跟踪和状态管理功能
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from .jira_operate import JiraManager

logger = logging.getLogger(__name__)


class JiraWorkflowTools:
    """JIRA工作流集成工具"""
    
    def __init__(self):
        """初始化JIRA工具"""
        # 从环境变量读取配置信息
        self.jira_url = os.getenv("JIRA_URL")
        self.username = os.getenv("JIRA_USERNAME", "longyu3")
        self.token = os.getenv("JIRA_TOKEN")
        
        # 验证必需的配置
        if not self.jira_url:
            logger.error("JIRA_URL 环境变量未设置")
            raise ValueError("JIRA_URL 环境变量未设置")
        if not self.token:
            logger.error("JIRA_TOKEN 环境变量未设置")
            raise ValueError("JIRA_TOKEN 环境变量未设置")
        
        # EDW项目配置
        self.edw_projects = {
            "EDW": "EDW数据仓库项目",
            "DWH": "数据仓库项目", 
            "FIN": "财务数据项目",
            "HR": "人力资源数据项目",
            "SCM": "供应链管理项目"
        }
        
        # 状态映射 - EDW工作流到JIRA状态
        self.edw_status_mapping = {
            "pending": "To Do",
            "in_progress": "In Progress", 
            "review": "Code Review",
            "testing": "Testing",
            "completed": "Done",
            "deployed": "Deployed",
            "failed": "Failed"
        }
        
        self.jira_manager = None
    
    def _get_jira_manager(self) -> JiraManager:
        """获取JIRA管理器实例"""
        if not self.jira_manager:
            self.jira_manager = JiraManager(
                self.jira_url,
                self.username,
                self.token
            )
        return self.jira_manager

    async def update_edw_task_status(self, issue_key: str, edw_status: str, 
                                   context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        更新EDW任务状态
        
        Args:
            issue_key: JIRA问题键值
            edw_status: EDW状态 (pending, in_progress, review, testing, completed, etc.)
            context: 工作流上下文信息
            
        Returns:
            操作结果字典
        """
        try:
            logger.info(f"🎯 准备更新EDW任务状态: {issue_key} -> {edw_status}")
            
            jm = self._get_jira_manager()
            
            # 映射EDW状态到JIRA状态
            jira_status = self.edw_status_mapping.get(edw_status, edw_status)
            
            # 构建状态更新评论
            comment = self._build_status_update_comment(edw_status, context)
            
            # 更新JIRA状态
            success = jm.update_issue_status(
                issue_key=issue_key,
                status_name=jira_status,
                comment=comment
            )
            
            if success:
                logger.info(f"✅ EDW任务状态更新成功: {issue_key}")
                return {
                    "success": True,
                    "issue_key": issue_key,
                    "edw_status": edw_status,
                    "jira_status": jira_status,
                    "update_time": datetime.now().isoformat()
                }
            else:
                error_msg = f"JIRA状态更新失败: {issue_key}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "issue_key": issue_key
                }
                
        except Exception as e:
            error_msg = f"更新EDW任务状态失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "issue_key": issue_key
            }

    async def add_edw_progress_comment(self, issue_key: str, action: str, 
                                     details: Dict[str, Any]) -> Dict[str, Any]:
        """
        添加EDW进度评论
        
        Args:
            issue_key: JIRA问题键值
            action: 执行的操作
            details: 详细信息
            
        Returns:
            操作结果字典
        """
        try:
            logger.info(f"📝 准备添加EDW进度评论: {issue_key}")
            
            jm = self._get_jira_manager()
            
            # 构建EDW格式的评论
            comment_body = self._build_edw_progress_comment(action, details)
            
            # 添加评论
            success = jm.add_comment(issue_key, comment_body)
            
            if success:
                logger.info(f"✅ EDW进度评论添加成功: {issue_key}")
                return {
                    "success": True,
                    "issue_key": issue_key,
                    "action": action,
                    "comment_time": datetime.now().isoformat()
                }
            else:
                error_msg = f"评论添加失败: {issue_key}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "issue_key": issue_key
                }
                
        except Exception as e:
            error_msg = f"添加EDW进度评论失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "issue_key": issue_key
            }

    async def create_model_enhancement_comment(self, issue_key: str, 
                                             table_name: str, enhancement_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建模型增强专用评论
        
        Args:
            issue_key: JIRA问题键值
            table_name: 表名
            enhancement_details: 增强详情
            
        Returns:
            操作结果字典
        """
        try:
            logger.info(f"🚀 准备创建模型增强评论: {issue_key} - {table_name}")
            
            jm = self._get_jira_manager()
            
            # 构建模型增强评论
            comment_body = self._build_model_enhancement_comment(table_name, enhancement_details)
            
            # 添加评论
            success = jm.add_comment(issue_key, comment_body)
            
            if success:
                logger.info(f"✅ 模型增强评论创建成功: {issue_key}")
                return {
                    "success": True,
                    "issue_key": issue_key,
                    "table_name": table_name,
                    "enhancement_type": enhancement_details.get("type", "unknown"),
                    "comment_time": datetime.now().isoformat()
                }
            else:
                error_msg = f"模型增强评论创建失败: {issue_key}"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "issue_key": issue_key
                }
                
        except Exception as e:
            error_msg = f"创建模型增强评论失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "issue_key": issue_key
            }

    async def complete_edw_task(self, issue_key: str, completion_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        完成EDW任务 - 更新状态为完成并添加总结评论
        
        Args:
            issue_key: JIRA问题键值
            completion_details: 完成详情
            
        Returns:
            操作结果字典
        """
        try:
            logger.info(f"🎉 准备完成EDW任务: {issue_key}")
            
            # 1. 添加完成总结评论
            comment_result = await self.add_edw_progress_comment(
                issue_key, 
                "任务完成", 
                completion_details
            )
            
            # 2. 更新状态为完成
            status_result = await self.update_edw_task_status(
                issue_key, 
                "completed", 
                completion_details
            )
            
            if comment_result["success"] and status_result["success"]:
                logger.info(f"✅ EDW任务完成成功: {issue_key}")
                return {
                    "success": True,
                    "issue_key": issue_key,
                    "completion_time": datetime.now().isoformat(),
                    "comment_added": True,
                    "status_updated": True
                }
            else:
                # 部分成功的情况
                logger.warning(f"⚠️ EDW任务完成部分成功: {issue_key}")
                return {
                    "success": False,
                    "issue_key": issue_key,
                    "comment_added": comment_result["success"],
                    "status_updated": status_result["success"],
                    "errors": [
                        comment_result.get("error", "") if not comment_result["success"] else "",
                        status_result.get("error", "") if not status_result["success"] else ""
                    ]
                }
                
        except Exception as e:
            error_msg = f"完成EDW任务失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "issue_key": issue_key
            }

    def _build_status_update_comment(self, edw_status: str, context: Dict[str, Any] = None) -> str:
        """构建状态更新评论"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 状态中文映射
            status_chinese = {
                "pending": "待处理",
                "in_progress": "进行中", 
                "review": "代码审查中",
                "testing": "测试中",
                "completed": "已完成",
                "deployed": "已部署",
                "failed": "失败"
            }
            
            status_text = status_chinese.get(edw_status, edw_status)
            
            comment = f"""
🔄 **EDW任务状态更新**

**新状态:** {status_text}
**更新时间:** {timestamp}
"""
            
            # 添加上下文信息
            if context:
                table_name = context.get("table_name", "")
                if table_name:
                    comment += f"**相关表:** {table_name}\n"
                
                node_name = context.get("node_name", "")
                if node_name:
                    comment += f"**当前节点:** {node_name}\n"
                
                message = context.get("message", "")
                if message:
                    comment += f"**详细信息:** {message}\n"
            
            comment += "\n---\n*此状态由EDW自动化系统更新*"
            
            return comment
            
        except Exception as e:
            logger.error(f"构建状态更新评论失败: {e}")
            return f"EDW任务状态更新为: {edw_status}"

    def _build_edw_progress_comment(self, action: str, details: Dict[str, Any]) -> str:
        """构建EDW进度评论"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 操作状态标识
            success = details.get("success", True)
            status_emoji = "✅" if success else "❌"
            status_text = "成功" if success else "失败"
            
            comment = f"""
{status_emoji} **EDW自动化操作 - {action} {status_text}**

**时间:** {timestamp}
"""
            
            # 添加表名信息
            table_name = details.get("table_name", "")
            if table_name:
                comment += f"**表名:** {table_name}\n"
            
            # 添加详细信息
            if "fields_added" in details:
                comment += f"**新增字段:** {details['fields_added']} 个\n"
            
            if "code_lines" in details:
                comment += f"**代码行数:** {details['code_lines']} 行\n"
            
            if "confluence_url" in details:
                comment += f"**文档链接:** {details['confluence_url']}\n"
            
            if "error_message" in details:
                comment += f"**错误信息:** {details['error_message']}\n"
            
            # 添加描述
            description = details.get("description", "")
            if description:
                comment += f"\n**详细描述:**\n{description}\n"
            
            comment += "\n---\n*此评论由EDW自动化系统生成*"
            
            return comment
            
        except Exception as e:
            logger.error(f"构建EDW进度评论失败: {e}")
            return f"EDW操作: {action} - {timestamp}"

    def _build_model_enhancement_comment(self, table_name: str, enhancement_details: Dict[str, Any]) -> str:
        """构建模型增强评论"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            enhancement_type = enhancement_details.get("type", "模型增强")
            
            comment = f"""
🚀 **EDW模型增强完成**

**表名:** {table_name}
**增强类型:** {enhancement_type}
**完成时间:** {timestamp}

**增强内容:**
"""
            
            # 新增字段信息
            if "new_fields" in enhancement_details:
                new_fields = enhancement_details["new_fields"]
                comment += f"• 新增字段: {len(new_fields)} 个\n"
                for field in new_fields[:5]:  # 显示前5个字段
                    field_name = field.get("physical_name", "unknown")
                    field_type = field.get("data_type", "string")
                    comment += f"  - {field_name} ({field_type})\n"
                if len(new_fields) > 5:
                    comment += f"  - ... 还有 {len(new_fields) - 5} 个字段\n"
            
            # 代码信息
            if "enhanced_code" in enhancement_details:
                code = enhancement_details["enhanced_code"]
                if code:
                    lines = len(code.split('\n'))
                    comment += f"• 生成代码: {lines} 行\n"
            
            # ALTER SQL信息
            if "alter_sql" in enhancement_details:
                alter_sql = enhancement_details["alter_sql"]
                if alter_sql:
                    comment += f"• DDL语句: 已生成\n"
            
            # 文档链接
            if "confluence_url" in enhancement_details:
                comment += f"• 文档链接: {enhancement_details['confluence_url']}\n"
            
            # 基表信息
            if "base_tables" in enhancement_details:
                base_tables = enhancement_details["base_tables"]
                if base_tables:
                    comment += f"• 依赖基表: {', '.join(base_tables)}\n"
            
            comment += "\n---\n*此增强由EDW自动化系统完成*"
            
            return comment
            
        except Exception as e:
            logger.error(f"构建模型增强评论失败: {e}")
            return f"EDW模型增强完成: {table_name} - {timestamp}"

    async def get_edw_task_info(self, issue_key: str) -> Dict[str, Any]:
        """
        获取EDW任务信息
        
        Args:
            issue_key: JIRA问题键值
            
        Returns:
            任务信息字典
        """
        try:
            logger.info(f"📊 获取EDW任务信息: {issue_key}")
            
            jm = self._get_jira_manager()
            
            # 获取问题详情
            issue = jm.get_issue(issue_key)
            if not issue:
                return {
                    "success": False,
                    "error": f"未找到问题: {issue_key}"
                }
            
            # 提取关键信息
            task_info = {
                "success": True,
                "issue_key": issue_key,
                "title": issue['fields']['summary'],
                "status": issue['fields']['status']['name'],
                "assignee": issue['fields']['assignee']['displayName'] if issue['fields']['assignee'] else "未分配",
                "reporter": issue['fields']['reporter']['displayName'] if issue['fields']['reporter'] else "未知",
                "created": issue['fields']['created'],
                "updated": issue['fields']['updated'],
                "description": issue['fields']['description'] if issue['fields']['description'] else "",
                "project": issue['fields']['project']['key'],
                "issue_type": issue['fields']['issuetype']['name']
            }
            
            # 获取评论
            comments = jm.get_issue_comments(issue_key)
            task_info["comments_count"] = len(comments)
            task_info["latest_comment"] = comments[-1]['body'] if comments else ""
            
            logger.info(f"✅ EDW任务信息获取成功: {issue_key}")
            return task_info
            
        except Exception as e:
            error_msg = f"获取EDW任务信息失败: {str(e)}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "issue_key": issue_key
            }


# 工具函数
async def update_jira_task_status(issue_key: str, status: str, context: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    更新JIRA任务状态的工具函数
    
    Args:
        issue_key: JIRA问题键值
        status: EDW状态
        context: 工作流上下文
        
    Returns:
        操作结果
    """
    try:
        tools = JiraWorkflowTools()
        return await tools.update_edw_task_status(issue_key, status, context)
    except Exception as e:
        logger.error(f"❌ 更新JIRA任务状态失败: {e}")
        return {"success": False, "error": str(e), "issue_key": issue_key}


async def add_jira_comment(issue_key: str, action: str, details: Dict[str, Any]) -> Dict[str, Any]:
    """
    添加JIRA评论的工具函数
    
    Args:
        issue_key: JIRA问题键值
        action: 执行的操作
        details: 详细信息
        
    Returns:
        操作结果
    """
    try:
        tools = JiraWorkflowTools()
        return await tools.add_edw_progress_comment(issue_key, action, details)
    except Exception as e:
        logger.error(f"❌ 添加JIRA评论失败: {e}")
        return {"success": False, "error": str(e), "issue_key": issue_key}