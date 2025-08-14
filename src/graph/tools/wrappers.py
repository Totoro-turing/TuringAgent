"""
工具包装器模块

将异步工具函数包装为LangChain工具
"""

import logging
from typing import List, Any
from langchain_core.tools import StructuredTool

from .adb_tools import (
    update_adb_notebook,
    read_adb_notebook,
    ADBUpdateTool,
    ADBReadTool
)

from .email_tools import (
    send_model_review_email,
    build_email_template,
    EmailSendTool
)

from .confluence_tools import (
    create_model_documentation,
    update_model_documentation,
    ConfluenceDocTool,
    ConfluenceUpdateTool
)

from .naming_tools import (
    suggest_attribute_names,
    batch_standardize_field_names,
    evaluate_attribute_name,
    NamingSuggestionTool,
    FieldStandardizationTool
)

logger = logging.getLogger(__name__)


def create_adb_tools() -> List[StructuredTool]:
    """
    创建ADB相关工具
    
    Returns:
        ADB工具列表
    """
    tools = []
    
    # ADB更新工具
    tools.append(StructuredTool.from_function(
        coroutine=update_adb_notebook,
        name="update_adb_notebook",
        description="更新Azure Databricks笔记本"
    ))
    
    # ADB读取工具
    tools.append(StructuredTool.from_function(
        coroutine=read_adb_notebook,
        name="read_adb_notebook",
        description="读取Azure Databricks笔记本内容"
    ))
    
    return tools


def create_email_tools() -> List[StructuredTool]:
    """
    创建邮件相关工具
    
    Returns:
        邮件工具列表
    """
    tools = []
    
    # 发送评审邮件工具
    tools.append(StructuredTool.from_function(
        coroutine=send_model_review_email,
        name="send_review_email",
        description="发送模型评审邮件给相关团队"
    ))
    
    # 构建邮件模板工具
    tools.append(StructuredTool.from_function(
        coroutine=build_email_template,
        name="build_email_template",
        description="构建HTML格式的邮件模板"
    ))
    
    return tools


def create_confluence_tools() -> List[StructuredTool]:
    """
    创建Confluence相关工具
    
    Returns:
        Confluence工具列表
    """
    tools = []
    
    # 创建文档工具
    tools.append(StructuredTool.from_function(
        coroutine=create_model_documentation,
        name="create_confluence_doc",
        description="创建模型的Confluence文档"
    ))
    
    # 更新文档工具
    tools.append(StructuredTool.from_function(
        coroutine=update_model_documentation,
        name="update_confluence_doc",
        description="更新已有的Confluence文档"
    ))
    
    return tools


def create_naming_tools() -> List[StructuredTool]:
    """
    创建命名相关工具
    
    Returns:
        命名工具列表
    """
    tools = []
    
    # 属性名称建议工具
    tools.append(StructuredTool.from_function(
        coroutine=suggest_attribute_names,
        name="suggest_attribute_names",
        description="为物理字段名提供属性名称建议和评分"
    ))
    
    # 批量字段标准化工具
    tools.append(StructuredTool.from_function(
        coroutine=batch_standardize_field_names,
        name="batch_standardize_fields",
        description=(
            "批量将属性名转换为标准物理字段名。"
            "参数fields是字典列表，每个字典必须包含attribute_name键。"
            "格式：[{'attribute_name': '用户姓名'}, {'attribute_name': '创建时间'}]"
        )
    ))
    
    # 属性名评估工具
    tools.append(StructuredTool.from_function(
        coroutine=evaluate_attribute_name,
        name="evaluate_attribute_name",
        description="评估属性名称的质量和规范性"
    ))
    
    return tools


def create_all_tools() -> List[StructuredTool]:
    """
    创建所有可用工具
    
    Returns:
        完整的工具列表
    """
    all_tools = []
    
    # 添加各类工具
    # all_tools.extend(create_adb_tools())
    all_tools.extend(create_email_tools())
    all_tools.extend(create_confluence_tools())
    all_tools.extend(create_naming_tools())
    
    logger.info(f"创建了 {len(all_tools)} 个LangChain工具")
    
    return all_tools


def create_tool_instances() -> List[Any]:
    """
    创建工具类实例
    
    Returns:
        工具实例列表
    """
    instances = [
        ADBUpdateTool(),
        ADBReadTool(),
        EmailSendTool(),
        ConfluenceDocTool(),
        ConfluenceUpdateTool(),
        NamingSuggestionTool(),
        FieldStandardizationTool()
    ]
    
    logger.info(f"创建了 {len(instances)} 个工具实例")
    
    return instances


def get_tools_by_category(category: str) -> List[StructuredTool]:
    """
    根据类别获取工具
    
    Args:
        category: 工具类别 (adb, email, confluence, naming, all)
    
    Returns:
        对应类别的工具列表
    """
    category_map = {
        "adb": create_adb_tools,
        "email": create_email_tools,
        "confluence": create_confluence_tools,
        "naming": create_naming_tools,
        "all": create_all_tools
    }
    
    if category in category_map:
        return category_map[category]()
    else:
        logger.warning(f"未知的工具类别: {category}")
        return []


def create_agent_toolset(
    include_adb: bool = True,
    include_email: bool = True,
    include_confluence: bool = True,
    include_naming: bool = True
) -> List[StructuredTool]:
    """
    创建自定义的工具集
    
    Args:
        include_adb: 是否包含ADB工具
        include_email: 是否包含邮件工具
        include_confluence: 是否包含Confluence工具
        include_naming: 是否包含命名工具
    
    Returns:
        自定义工具集
    """
    tools = []
    
    if include_adb:
        tools.extend(create_adb_tools())
    
    if include_email:
        tools.extend(create_email_tools())
    
    if include_confluence:
        tools.extend(create_confluence_tools())
    
    if include_naming:
        tools.extend(create_naming_tools())
    
    logger.info(f"创建自定义工具集，包含 {len(tools)} 个工具")
    
    return tools