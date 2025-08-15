"""
Confluence文档工具模块

提供创建和更新Confluence文档的异步工具
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from .base import AsyncBaseTool, create_tool_result, run_with_timeout

logger = logging.getLogger(__name__)


def _get_model_stakeholders(schema: str) -> Dict[str, str]:
    """
    根据schema获取相关人员信息
    
    Args:
        schema: 数据库schema
    
    Returns:
        相关人员信息字典
    """
    stakeholder_map = {
        "dwd_fi": {
            "owner": "Finance Data Team",
            "reviewer": "Finance Analytics Team",
            "approver": "Finance Manager"
        },
        "cam_fi": {
            "owner": "Campaign Finance Team",
            "reviewer": "Finance Analytics Team",
            "approver": "Campaign Manager"
        },
        "dwd_sc": {
            "owner": "Supply Chain Data Team",
            "reviewer": "Supply Chain Analytics Team",
            "approver": "Supply Chain Manager"
        },
        "dwd_mk": {
            "owner": "Marketing Data Team",
            "reviewer": "Marketing Analytics Team",
            "approver": "Marketing Manager"
        },
        "default": {
            "owner": "Data Engineering Team",
            "reviewer": "Analytics Team",
            "approver": "Data Manager"
        }
    }
    
    return stakeholder_map.get(schema, stakeholder_map["default"])


async def create_model_documentation(
    table_name: str,
    model_name: str = "",
    enhanced_code: str = "",
    fields: List[Dict] = None,
    alter_table_sql: str = "",
    enhancement_type: str = "add_field",
    base_tables: List[str] = None,
    user_id: str = ""
) -> Dict[str, Any]:
    """
    异步创建模型文档
    
    Args:
        table_name: 表名
        model_name: 模型名称
        enhanced_code: 增强后的代码
        fields: 字段列表
        alter_table_sql: ALTER TABLE SQL语句
        enhancement_type: 增强类型
        base_tables: 依赖的基表列表
        user_id: 用户ID
    
    Returns:
        执行结果字典
    """
    try:
        # 验证table_name
        logger.info(f"准备创建Confluence文档: {table_name}")
        
        from src.basic.confluence.confluence_tools import ConfluenceWorkflowTools
        
        # 解析表名获取schema信息
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'default'
            table = table_name
        
        # 标准化字段信息格式
        standardized_fields = []
        if fields:
            for field in fields:
                if isinstance(field, dict):
                    physical_name = field.get('physical_name', field.get('name', ''))
                    attribute_name = field.get('attribute_name', '')
                    data_type = field.get('data_type', field.get('type', 'string'))
                    comment = field.get('comment', field.get('description', ''))
                else:
                    physical_name = getattr(field, 'physical_name', getattr(field, 'name', ''))
                    attribute_name = getattr(field, 'attribute_name', '')
                    data_type = getattr(field, 'data_type', getattr(field, 'type', 'string'))
                    comment = getattr(field, 'comment', getattr(field, 'description', ''))
                
                if physical_name:
                    standardized_fields.append({
                        'physical_name': physical_name,
                        'attribute_name': attribute_name or physical_name,
                        'data_type': data_type,
                        'comment': comment,
                        'source': 'Enhanced'
                    })
        
        # 构建用于Confluence的上下文，直接传递字段信息
        context = {
            "table_name": table_name,
            "model_name": model_name,
            "enhanced_code": enhanced_code,
            "explanation": f"为表{table_name} 增加了 {len(standardized_fields)} 个新字段",
            "improvements": [f"增加字段: {field['physical_name']}" for field in standardized_fields],
            "alter_sql": alter_table_sql,
            "fields": standardized_fields  # 直接传递字段信息
        }
        
        # 创建Confluence工具实例
        tools = ConfluenceWorkflowTools()
        
        # 收集文档信息，传递字段信息
        doc_info = await run_with_timeout(
            tools.collect_model_documentation_info(context),
            timeout=30.0,
            timeout_message="收集文档信息超时"
        )
        
        if "error" in doc_info:
            error_msg = f"收集文档信息失败: {doc_info['error']}"
            logger.error(error_msg)
            return create_tool_result(False, error=error_msg)
        
        # 确保doc_info包含必要的schema_info
        if "schema_info" not in doc_info:
            error_msg = "文档信息缺少schema_info"
            logger.error(error_msg)
            return create_tool_result(False, error=error_msg)
        
        # 获取相关人员信息
        stakeholders = _get_model_stakeholders(schema)
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 根据enhancement_type确定操作类型
        operation_type = "Enhance" if enhancement_type in ["add_field", "modify_logic", "optimize_query"] else "New"
        
        # 根据schema确定业务域
        domain_map = {
            "dwd_fi": "Finance",
            "cam_fi": "Finance",
            "dws_fi": "Finance",
            "ads_fi": "Finance",
            "dwd_sc": "Supply Chain",
            "cam_sc": "Supply Chain",
            "dws_sc": "Supply Chain",
            "ads_sc": "Supply Chain",
            "dwd_mk": "Marketing",
            "cam_mk": "Marketing",
            "dws_mk": "Marketing",
            "ads_mk": "Marketing"
        }
        business_domain = domain_map.get(schema, "General")
        
        # 构建model_config
        model_config = {
            "code": enhanced_code or "",
            "table_name": table_name,
            "model_name": model_name or table,
            "business_domain": business_domain,
            "data_source": ", ".join(base_tables) if base_tables else "EDW",
            "update_frequency": "Daily",
            "owner": stakeholders["owner"],
            "reviewer": stakeholders["reviewer"],
            "approver": stakeholders["approver"],
            "created_date": current_date,
            "last_modified": current_date,
            "version": "1.0.0",
            "status": "In Review",
            "description": doc_info.get("description", f"Model for {table_name}"),
            "business_logic": doc_info.get("business_logic", ""),
            "data_quality_checks": doc_info.get("data_quality", []),
            "dependencies": base_tables or [],
            "fields": []
        }
        
        # 添加字段信息 - 使用已标准化的字段信息
        for field in standardized_fields:
            field_info = {
                "physical_name": field["physical_name"],
                "attribute_name": field["attribute_name"],
                "data_type": field["data_type"],
                "comment": field["comment"],
                "name": field["physical_name"],  # 保持兼容性
                "type": field["data_type"],  # 保持兼容性
                "description": field["comment"] or field["attribute_name"],
                "business_meaning": field["attribute_name"],
                "source": field["source"]
            }
            model_config["fields"].append(field_info)
        
        # 创建Confluence页面
        logger.info(f"正在创建Confluence页面: {model_config['model_name']}")
        
        # 将model_config转换为doc_info格式
        doc_info["model_config"] = model_config
        doc_info["operation_type"] = operation_type
        
        page_result = await run_with_timeout(
            tools.create_confluence_page(doc_info),
            timeout=60.0,
            timeout_message="创建Confluence页面超时"
        )
        
        if page_result.get("success"):
            page_url = page_result.get("page_url", "")
            page_id = page_result.get("page_id", "")
            page_title = page_result.get("page_title", f"Model Documentation - {model_name or table}")
            
            logger.info(f"Confluence文档创建成功: {page_url}")
            
            return create_tool_result(
                True,
                result={
                    "page_url": page_url,
                    "page_id": page_id,
                    "page_title": page_title
                },
                page_url=page_url,
                page_id=page_id,
                page_title=page_title,
                creation_time=datetime.now().isoformat()
            )
        else:
            error_msg = page_result.get("error", "创建页面失败")
            logger.error(f"Confluence页面创建失败: {error_msg}")
            return create_tool_result(False, error=error_msg)
            
    except TimeoutError as e:
        error_msg = str(e)
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)
    except Exception as e:
        error_msg = f"创建Confluence文档失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def update_model_documentation(
    page_id: str,
    content: str,
    title: Optional[str] = None,
    version_comment: str = "AI自动更新"
) -> Dict[str, Any]:
    """
    异步更新模型文档
    
    Args:
        page_id: Confluence页面ID
        content: 更新的内容
        title: 新的标题（可选）
        version_comment: 版本注释
    
    Returns:
        执行结果字典
    """
    try:
        logger.info(f"准备更新Confluence页面: {page_id}")
        
        from src.basic.confluence.confluence_tools import ConfluenceWorkflowTools
        
        tools = ConfluenceWorkflowTools()
        confluence_manager = tools._get_confluence_manager()
        
        # 更新页面
        # 注意：ConfluenceManager.update_page 是同步方法，需要包装
        import asyncio
        
        async def async_update_page():
            try:
                result = confluence_manager.update_page(
                    page_id=page_id,
                    title=title or "",  # 如果没有新标题，使用空字符串
                    content=content
                )
                return {"success": True, "result": result}
            except Exception as e:
                return {"success": False, "error": str(e)}
        
        update_result = await run_with_timeout(
            async_update_page(),
            timeout=30.0,
            timeout_message="更新Confluence页面超时"
        )
        
        if update_result.get("success"):
            logger.info(f"Confluence页面更新成功: {page_id}")
            return create_tool_result(
                True,
                result=update_result,
                page_id=page_id,
                update_time=datetime.now().isoformat()
            )
        else:
            error_msg = update_result.get("error", "更新页面失败")
            logger.error(error_msg)
            return create_tool_result(False, error=error_msg)
            
    except Exception as e:
        error_msg = f"更新Confluence文档失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


class ConfluenceCreateInput(BaseModel):
    """Confluence创建工具的输入参数"""
    table_name: str = Field(description="表名")
    model_name: Optional[str] = Field(default="", description="模型名称")
    enhanced_code: Optional[str] = Field(default="", description="增强后的代码")
    fields: Optional[List[Dict]] = Field(default=None, description="字段列表")
    alter_table_sql: Optional[str] = Field(default="", description="ALTER TABLE SQL")
    enhancement_type: str = Field(default="add_field", description="增强类型")


class ConfluenceDocTool(AsyncBaseTool):
    """
    Confluence文档工具
    
    用于创建和管理模型文档
    """
    name: str = "create_confluence_doc"
    description: str = "创建模型的Confluence文档"
    args_schema: type[BaseModel] = ConfluenceCreateInput
    
    async def _arun(
        self,
        table_name: str,
        model_name: str = "",
        enhanced_code: str = "",
        fields: List[Dict] = None,
        alter_table_sql: str = "",
        enhancement_type: str = "add_field",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行文档创建"""
        result = await create_model_documentation(
            table_name=table_name,
            model_name=model_name,
            enhanced_code=enhanced_code,
            fields=fields,
            alter_table_sql=alter_table_sql,
            enhancement_type=enhancement_type
        )
        
        if result["success"]:
            page_url = result.get("metadata", {}).get("page_url", "")
            return f"文档创建成功: {page_url}"
        else:
            return f"文档创建失败: {result.get('error', '未知错误')}"


class ConfluenceUpdateInput(BaseModel):
    """Confluence更新工具的输入参数"""
    page_id: str = Field(description="页面ID")
    content: str = Field(description="更新内容")
    title: Optional[str] = Field(default=None, description="新标题")
    version_comment: str = Field(default="AI自动更新", description="版本注释")


class ConfluenceUpdateTool(AsyncBaseTool):
    """
    Confluence更新工具
    
    用于更新已有的Confluence文档
    """
    name: str = "update_confluence_doc"
    description: str = "更新已有的Confluence文档"
    args_schema: type[BaseModel] = ConfluenceUpdateInput
    
    async def _arun(
        self,
        page_id: str,
        content: str,
        title: Optional[str] = None,
        version_comment: str = "AI自动更新",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行文档更新"""
        result = await update_model_documentation(
            page_id=page_id,
            content=content,
            title=title,
            version_comment=version_comment
        )
        
        if result["success"]:
            return f"文档更新成功: {page_id}"
        else:
            return f"文档更新失败: {result.get('error', '未知错误')}"