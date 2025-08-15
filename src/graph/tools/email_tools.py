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
    
    # 构建字段表格HTML
    fields_table_html = ""
    if fields:
        for field in fields:
            if isinstance(field, dict):
                physical_name = field.get('physical_name', '')
                attribute_name = field.get('attribute_name', '')
            else:
                physical_name = getattr(field, 'physical_name', '')
                attribute_name = getattr(field, 'attribute_name', '')
            
            if physical_name:
                display_name = attribute_name if attribute_name else physical_name.replace('_', ' ').title()
                fields_table_html += f"""
                <tr>
                    <td style="padding: 8px 12px; border-left: 3px solid #0078d4; background-color: #f8f9fa;">
                        <span style="font-weight: 600; color: #323130;">{physical_name}</span>
                        <span style="color: #605e5c; margin-left: 8px;">({display_name})</span>
                    </td>
                </tr>"""
    
    # 构建审查链接HTML
    review_link_section = ""
    if confluence_url:
        review_link_section = f"""
        <div class="review-log-title">Review log:</div>
        <div style="margin: 25px 0;">
            <a href="{confluence_url}" style="background: linear-gradient(135deg, #0078d4, #106ebe); color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block; font-weight: 600; box-shadow: 0 2px 8px rgba(0,120,212,0.3); transition: all 0.3s ease;">
                📋 Review Log
            </a>
        </div>
        <p style="color: #605e5c; font-size: 14px; margin: 10px 0;">
            Review log: <a href="{confluence_url}" style="color: #0078d4;">{confluence_url}</a>
        </p>"""
    else:
        review_link_section = """
        <div class="review-log-title">Review log:</div>
        <p style="color: #d13438; font-weight: 500;">Review链接暂不可用，请联系技术支持。</p>"""
    
    # 构建完整的HTML模板
    html_content = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🤖 EDW Model Review Request [AI Generated]</title>
    <style>
        body {{
            font-family: 'Segoe UI', 'Microsoft YaHei', Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 0;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 600px;
            margin: 20px auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: #0078d4;
            color: white;
            padding: 20px;
            text-align: center;
        }}
        .content {{
            padding: 30px;
        }}
        .greeting {{
            font-size: 16px;
            color: #323130;
            margin-bottom: 20px;
            font-weight: 500;
        }}
        .model-name {{
            font-size: 20px;
            font-weight: 700;
            color: #0078d4;
            margin: 20px 0;
            padding: 15px;
            background: #f0f6ff;
            border-left: 4px solid #0078d4;
            border-radius: 4px;
        }}
        .fields-section {{
            margin: 25px 0;
        }}
        .fields-title {{
            font-size: 16px;
            font-weight: 600;
            color: #323130;
            margin-bottom: 15px;
        }}
        .fields-table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        .review-log-title {{
            font-size: 16px;
            font-weight: 600;
            color: #323130;
            margin: 25px 0 15px 0;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #605e5c;
            font-size: 14px;
            border-top: 1px solid #e1dfdd;
        }}
        a:hover {{
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,120,212,0.4) !important;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1 style="margin: 0; font-size: 24px;">🤖 EDW Model Review Request [AI Generated]</h1>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">Enterprise Data Warehouse</p>
        </div>
        <div class="content">
            <!-- AI生成提示框 - 移到最上面 -->
            <div style="background: #f0f8ff; border: 2px solid #4a90e2; border-radius: 8px; padding: 15px; margin-bottom: 20px; text-align: center;">
                <p style="margin: 0; color: #2c5aa0; font-weight: 600; font-size: 14px;">
                    🤖 本邮件内容由智能体发出 | AI Generated Content
                </p>
            </div>
            <div class="greeting">Hello {greeting},</div>
            <div class="model-name">
                请帮忙review {model_full_name} 模型增强
            </div>
            <div class="fields-section">
                <div class="fields-title">新增字段如下：</div>
                <table class="fields-table">
                    {fields_table_html}
                </table>
            </div>
            {review_link_section}
        </div>
        <div class="footer">
            <p style="margin: 0; color: #4a90e2; font-weight: 600;">🤖 This email was automatically generated by EDW Intelligent Assistant</p>
            <p style="margin: 5px 0 0 0; color: #4a90e2; font-size: 13px;">
                AI Generated Content | Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </div>
    </div>
</body>
</html>"""
    
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
        param = EmailParam(email_params)
        
        # 发送邮件
        email = Email(param.get_param(), settings.EMAIL_TOKEN)
        response = email.send()
        
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