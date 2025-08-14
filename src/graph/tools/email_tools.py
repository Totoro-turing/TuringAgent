"""
邮件工具模块

提供发送模型评审邮件的异步工具
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from .base import AsyncBaseTool, create_tool_result

logger = logging.getLogger(__name__)

# EDW Schema对应的问候语映射
EDW_EMAIL_GREETING_MAP = {
    "dwd_fi": "Finance Team",
    "cam_fi": "Campaign Finance Team",
    "dws_fi": "Finance Summary Team",
    "ads_fi": "Finance Analytics Team",
    "dwd_sc": "Supply Chain Team",
    "cam_sc": "Campaign Supply Chain Team", 
    "dws_sc": "Supply Chain Summary Team",
    "ads_sc": "Supply Chain Analytics Team",
    "dwd_mk": "Marketing Team",
    "cam_mk": "Campaign Marketing Team",
    "dws_mk": "Marketing Summary Team",
    "ads_mk": "Marketing Analytics Team",
    "default": "Data Team"
}


async def build_email_template(
    table_name: str,
    model_name: str = "",
    fields: List[Dict] = None,
    confluence_url: str = "",
    confluence_title: str = "",
    enhancement_type: str = "add_field"
) -> str:
    """
    构建邮件HTML模板
    
    Args:
        table_name: 表名
        model_name: 模型名称
        fields: 字段列表
        confluence_url: Confluence文档链接
        confluence_title: Confluence文档标题
        enhancement_type: 增强类型
    
    Returns:
        HTML格式的邮件内容
    """
    # 解析schema信息
    schema = "default"
    if '.' in table_name:
        schema = table_name.split('.')[0]
    
    # 确定问候语
    greeting = EDW_EMAIL_GREETING_MAP.get(schema.lower(), EDW_EMAIL_GREETING_MAP["default"])
    
    # 构建模型全名
    if model_name:
        model_full_name = f"{schema}.{model_name}"
    else:
        table_suffix = table_name.split('.')[-1] if '.' in table_name else table_name
        formatted_name = table_suffix.replace('_', ' ').title()
        model_full_name = f"{schema}.{formatted_name}"
    
    # 构建字段列表HTML
    fields_html = ""
    if fields:
        fields_list = []
        for field in fields:
            if isinstance(field, dict):
                physical_name = field.get('physical_name', '')
                attribute_name = field.get('attribute_name', '')
                data_type = field.get('data_type', 'string')
                comment = field.get('comment', '')
            else:
                physical_name = getattr(field, 'physical_name', '')
                attribute_name = getattr(field, 'attribute_name', '')
                data_type = getattr(field, 'data_type', 'string')
                comment = getattr(field, 'comment', '')
            
            field_desc = f"<strong>{physical_name}</strong> ({data_type})"
            if attribute_name:
                field_desc += f" - {attribute_name}"
            if comment:
                field_desc += f": {comment}"
            
            fields_list.append(f"<li>{field_desc}</li>")
        
        fields_html = f"""
            <div style="background: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <h3 style="color: #323130; margin-top: 0;">新增字段:</h3>
                <ul style="color: #605e5c;">
                    {''.join(fields_list)}
                </ul>
            </div>"""
    
    # 构建Review链接HTML
    review_link_html = ""
    if confluence_url:
        review_link_html = f"""
            <div style="margin: 25px 0;">
                <a href="{confluence_url}"
                   style="background: linear-gradient(135deg, #0078d4, #106ebe);
                          color: white;
                          padding: 12px 24px;
                          text-decoration: none;
                          border-radius: 6px;
                          display: inline-block;
                          font-weight: 600;
                          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                          transition: all 0.3s ease;">
                    Review Changes
                </a>
            </div>
            <p style="color: #605e5c; font-size: 14px; margin: 10px 0;">
                Review log: <a href="{confluence_url}" style="color: #0078d4;">{confluence_url}</a>
            </p>"""
    else:
        review_link_html = '<p style="color: #d13438;">Review链接暂不可用，请联系技术支持。</p>'
    
    # 构建完整的HTML模板
    html_content = f"""
    <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                max-width: 800px; 
                margin: 0 auto;
                background: white;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
        
        <!-- Header -->
        <div style="background: linear-gradient(135deg, #0078d4, #106ebe); 
                    padding: 30px;
                    color: white;">
            <h1 style="margin: 0; font-size: 28px;">Model Review Request</h1>
            <p style="margin: 10px 0 0 0; opacity: 0.95; font-size: 16px;">
                AI-Powered EDW Model Enhancement
            </p>
        </div>
        
        <!-- Content -->
        <div style="padding: 30px;">
            <p style="color: #323130; font-size: 16px; line-height: 1.6;">
                Dear {greeting},
            </p>
            
            <p style="color: #323130; font-size: 16px; line-height: 1.6;">
                The EDW model <strong style="color: #0078d4;">{model_full_name}</strong> has been enhanced 
                using AI automation. Please review the changes below:
            </p>
            
            <!-- Table Info -->
            <div style="background: #f0f2f5; padding: 15px; border-radius: 8px; margin: 20px 0;">
                <p style="margin: 5px 0;"><strong>Target Table:</strong> {table_name}</p>
                <p style="margin: 5px 0;"><strong>Enhancement Type:</strong> {enhancement_type.replace('_', ' ').title()}</p>
                <p style="margin: 5px 0;"><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            {fields_html}
            
            <!-- Action Required -->
            <div style="background: #fff4ce; 
                        border-left: 4px solid #ffb900; 
                        padding: 15px;
                        margin: 20px 0;
                        border-radius: 4px;">
                <h3 style="color: #323130; margin-top: 0;">Action Required</h3>
                <p style="color: #605e5c; margin: 5px 0;">
                    Please review the model changes and provide your feedback.
                </p>
            </div>
            
            {review_link_html}
            
            <!-- Footer -->
            <div style="margin-top: 30px; 
                        padding-top: 20px; 
                        border-top: 1px solid #edebe9;">
                <p style="color: #a19f9d; font-size: 14px; margin: 5px 0;">
                    This is an automated message generated by the EDW AI Assistant.
                </p>
                <p style="color: #a19f9d; font-size: 14px; margin: 5px 0;">
                    For questions or concerns, please contact the EDW team.
                </p>
            </div>
        </div>
    </div>
    """
    
    return html_content


async def send_model_review_email(
    table_name: str,
    model_name: str = "",
    fields: List[Dict] = None,
    confluence_url: str = "",
    confluence_title: str = "",
    enhancement_type: str = "add_field",
    recipients: List[str] = None
) -> Dict[str, Any]:
    """
    异步发送模型评审邮件
    
    Args:
        table_name: 表名
        model_name: 模型名称
        fields: 字段列表
        confluence_url: Confluence文档链接
        confluence_title: Confluence文档标题
        enhancement_type: 增强类型
        recipients: 收件人列表
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备发送模型评审邮件: {table_name}")
        
        # 构建邮件内容
        html_content = await build_email_template(
            table_name=table_name,
            model_name=model_name,
            fields=fields,
            confluence_url=confluence_url,
            confluence_title=confluence_title,
            enhancement_type=enhancement_type
        )
        
        # 发送邮件
        result = await _send_email_via_metis(
            html_content=html_content,
            model_name=model_name or table_name,
            table_name=table_name,
            recipients=recipients
        )
        
        if result["success"]:
            logger.info(f"邮件发送成功: {table_name}")
            return create_tool_result(
                True,
                result="邮件发送成功",
                email_sent=True,
                email_format="HTML",
                email_subject=f"Model Review Request - {model_name or table_name} [AI Generated]",
                confluence_link_included=bool(confluence_url),
                confluence_page_url=confluence_url
            )
        else:
            error_msg = result.get("error", "邮件发送失败")
            logger.error(error_msg)
            return create_tool_result(False, error=error_msg)
            
    except Exception as e:
        error_msg = f"发送邮件失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def _send_email_via_metis(
    html_content: str,
    model_name: str,
    table_name: str,
    recipients: List[str] = None
) -> Dict[str, Any]:
    """
    通过Metis系统发送邮件
    
    Args:
        html_content: HTML邮件内容
        model_name: 模型名称
        table_name: 表名
        recipients: 收件人列表
    
    Returns:
        发送结果
    """
    try:
        from src.basic.metis.email import Email, EmailParam
        from src.basic.config import settings
        
        # 检查邮件token
        if not settings.EMAIL_TOKEN or settings.EMAIL_TOKEN == "":
            return {
                "success": False,
                "error": "EMAIL_TOKEN未配置，请检查环境变量"
            }
        
        # 构建邮件参数
        email_params = {
            "MOType": "EDW",
            "MOName": "ModelReview",
            "AlertName": f"Model Review Request - {model_name} [AI Generated]",
            "AlertDescription": html_content,
            "Priority": "P3",
            "Assignee": "reviewers"
        }
        
        # 如果指定了收件人，添加到参数中
        if recipients:
            email_params["Recipients"] = recipients
        
        # 创建邮件参数对象
        param = EmailParam(**email_params)
        
        # 发送邮件
        email = Email()
        response = email.send(param)
        
        if response and "error" not in str(response).lower():
            logger.info(f"Metis邮件发送成功: {model_name}")
            return {
                "success": True,
                "response": str(response)
            }
        else:
            error_msg = f"Metis邮件发送失败: {response}"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }
            
    except ImportError as e:
        error_msg = f"Metis邮件模块导入失败: {e}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    except Exception as e:
        error_msg = f"Metis邮件发送异常: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }


class EmailSendInput(BaseModel):
    """邮件发送工具的输入参数"""
    table_name: str = Field(description="表名")
    model_name: Optional[str] = Field(default="", description="模型名称")
    fields: Optional[List[Dict]] = Field(default=None, description="字段列表")
    confluence_url: Optional[str] = Field(default="", description="Confluence文档链接")
    enhancement_type: str = Field(default="add_field", description="增强类型")


class EmailSendTool(AsyncBaseTool):
    """
    邮件发送工具
    
    用于发送模型评审邮件
    """
    name: str = "send_review_email"
    description: str = "发送模型评审邮件给相关团队"
    args_schema: type[BaseModel] = EmailSendInput
    
    async def _arun(
        self,
        table_name: str,
        model_name: str = "",
        fields: List[Dict] = None,
        confluence_url: str = "",
        enhancement_type: str = "add_field",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行邮件发送"""
        result = await send_model_review_email(
            table_name=table_name,
            model_name=model_name,
            fields=fields,
            confluence_url=confluence_url,
            enhancement_type=enhancement_type
        )
        
        if result["success"]:
            return f"邮件发送成功: {table_name}"
        else:
            return f"邮件发送失败: {result.get('error', '未知错误')}"