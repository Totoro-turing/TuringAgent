"""
ADB（Azure Databricks）工具模块

提供与Databricks笔记本交互的异步工具
"""

import logging
import os
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

from .base import AsyncBaseTool, create_tool_result, run_with_timeout

logger = logging.getLogger(__name__)


def detect_code_language(code_path: Optional[str], source_code: str = "") -> str:
    """
    检测代码语言
    
    Args:
        code_path: 代码文件路径
        source_code: 源代码内容
    
    Returns:
        语言类型: PYTHON, SQL, SCALA, R
    """
    # 基于文件扩展名检测
    if code_path:
        ext = os.path.splitext(code_path)[1].lower()
        ext_map = {
            '.py': 'PYTHON',
            '.sql': 'SQL',
            '.scala': 'SCALA',
            '.r': 'R',
            '.ipynb': 'PYTHON'  # Jupyter notebooks默认为Python
        }
        if ext in ext_map:
            return ext_map[ext]
    
    # 基于内容检测
    if source_code:
        # SQL关键字检测
        sql_keywords = ['SELECT', 'FROM', 'WHERE', 'CREATE', 'INSERT', 'UPDATE', 'DELETE']
        if any(keyword in source_code.upper() for keyword in sql_keywords):
            # 检查是否是嵌入在Python中的SQL
            if 'spark.sql(' in source_code or 'pd.read_sql' in source_code:
                return 'PYTHON'
            return 'SQL'
        
        # Python特征检测
        if 'import ' in source_code or 'def ' in source_code or 'class ' in source_code:
            return 'PYTHON'
        
        # Scala特征检测
        if 'val ' in source_code or 'var ' in source_code or 'object ' in source_code:
            return 'SCALA'
        
        # R特征检测
        if '<-' in source_code or 'library(' in source_code:
            return 'R'
    
    # 默认返回Python
    return 'PYTHON'


async def update_adb_notebook(
    path: str,
    content: str,
    language: Optional[str] = None,
    overwrite: bool = True
) -> Dict[str, Any]:
    """
    异步更新ADB笔记本
    
    Args:
        path: 笔记本路径
        content: 笔记本内容
        language: 代码语言（可选，会自动检测）
        overwrite: 是否覆盖已存在的笔记本
    
    Returns:
        执行结果字典
    """
    try:
        # 自动检测语言
        if not language:
            language = detect_code_language(path, content)
        
        logger.info(f"准备更新ADB笔记本: {path} (语言: {language})")
        
        # 获取MCP客户端
        from src.mcp.mcp_client import get_mcp_client
        
        async with get_mcp_client() as client:
            if not client:
                error_msg = "无法连接到MCP服务"
                logger.error(error_msg)
                return create_tool_result(False, error=error_msg)
            
            try:
                # 获取所有MCP工具
                tools = await run_with_timeout(
                    client.get_tools(),
                    timeout=10.0,
                    timeout_message="获取MCP工具超时"
                )
                
                # 查找import_notebook工具
                import_tool = None
                for tool in tools:
                    if hasattr(tool, 'name') and 'import' in tool.name.lower() and 'notebook' in tool.name.lower():
                        import_tool = tool
                        logger.info(f"找到导入工具: {tool.name}")
                        break
                
                if not import_tool:
                    error_msg = "未找到import_notebook相关的MCP工具"
                    logger.error(error_msg)
                    return create_tool_result(False, error=error_msg)
                
                # 调用import_notebook方法
                logger.info(f"正在导入笔记本到: {path}")
                result = await run_with_timeout(
                    import_tool.ainvoke({
                        "path": path,
                        "content": content,
                        "language": language,
                        "overwrite": overwrite
                    }),
                    timeout=30.0,
                    timeout_message=f"导入笔记本超时: {path}"
                )
                
                logger.info(f"ADB笔记本更新成功: {path}")
                return create_tool_result(
                    True,
                    result=str(result),
                    adb_path=path,
                    language=language
                )
                
            except TimeoutError as e:
                error_msg = str(e)
                logger.error(error_msg)
                return create_tool_result(False, error=error_msg)
            except Exception as e:
                error_msg = f"MCP工具调用失败: {str(e)}"
                logger.error(error_msg)
                return create_tool_result(False, error=error_msg)
                
    except Exception as e:
        error_msg = f"更新ADB笔记本失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def read_adb_notebook(path: str) -> Dict[str, Any]:
    """
    异步读取ADB笔记本内容
    
    Args:
        path: 笔记本路径
    
    Returns:
        执行结果字典，包含笔记本内容
    """
    try:
        logger.info(f"准备读取ADB笔记本: {path}")
        
        from src.mcp.mcp_client import get_mcp_client
        
        async with get_mcp_client() as client:
            if not client:
                error_msg = "无法连接到MCP服务"
                logger.error(error_msg)
                return create_tool_result(False, error=error_msg)
            
            # 获取工具并查找export_notebook工具
            tools = await client.get_tools()
            export_tool = None
            
            for tool in tools:
                if hasattr(tool, 'name') and 'export' in tool.name.lower() and 'notebook' in tool.name.lower():
                    export_tool = tool
                    break
            
            if not export_tool:
                error_msg = "未找到export_notebook相关的MCP工具"
                logger.error(error_msg)
                return create_tool_result(False, error=error_msg)
            
            # 调用export_notebook方法
            result = await export_tool.ainvoke({"path": path})
            
            return create_tool_result(
                True,
                result=str(result),
                adb_path=path
            )
            
    except Exception as e:
        error_msg = f"读取ADB笔记本失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


class ADBUpdateInput(BaseModel):
    """ADB更新工具的输入参数"""
    path: str = Field(description="笔记本路径")
    content: str = Field(description="笔记本内容")
    language: Optional[str] = Field(default=None, description="代码语言")
    overwrite: bool = Field(default=True, description="是否覆盖")


class ADBUpdateTool(AsyncBaseTool):
    """
    ADB笔记本更新工具
    
    用于更新Databricks笔记本
    """
    name: str = "adb_update"
    description: str = "更新Azure Databricks笔记本"
    args_schema: type[BaseModel] = ADBUpdateInput
    
    async def _arun(
        self,
        path: str,
        content: str,
        language: Optional[str] = None,
        overwrite: bool = True,
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行ADB更新"""
        result = await update_adb_notebook(path, content, language, overwrite)
        
        if result["success"]:
            return f"成功更新ADB笔记本: {path}"
        else:
            return f"更新失败: {result.get('error', '未知错误')}"


class ADBReadInput(BaseModel):
    """ADB读取工具的输入参数"""
    path: str = Field(description="笔记本路径")


class ADBReadTool(AsyncBaseTool):
    """
    ADB笔记本读取工具
    
    用于读取Databricks笔记本内容
    """
    name: str = "adb_read"
    description: str = "读取Azure Databricks笔记本内容"
    args_schema: type[BaseModel] = ADBReadInput
    
    async def _arun(
        self,
        path: str,
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行ADB读取"""
        result = await read_adb_notebook(path)
        
        if result["success"]:
            return f"笔记本内容:\n{result['result']}"
        else:
            return f"读取失败: {result.get('error', '未知错误')}"