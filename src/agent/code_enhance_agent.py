import asyncio
import re
import logging
from typing import Dict, Any, List, Optional
from langchain_core.tools import BaseTool
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel, Field
from src.config import get_config_manager
from src.mcp.mcp_client import get_mcp_tools

logger = logging.getLogger(__name__)

class SQLQueryTool(BaseTool):
    """执行SQL查询的工具（兼容旧版本）"""
    name: str = "sql_query"
    description: str = "执行SQL查询语句，获取表结构、字段信息等"
    mcp_client: Any = Field(exclude=True)
    
    class Args(BaseModel):
        query: str = Field(description="要执行的SQL查询语句")
    
    async def _arun(self, query: str) -> str:
        """异步执行SQL查询"""
        try:
            result = await self.mcp_client.execute_sql(query)
            logger.info(f"SQL查询成功: {query[:100]}...")
            return result
        except Exception as e:
            error_msg = f"SQL查询失败: {str(e)}"
            logger.error(error_msg)
            return f"错误: {error_msg}"
    
    def _run(self, query: str) -> str:
        """同步执行SQL查询"""
        return asyncio.run(self._arun(query))

class EnhancedSQLQueryTool(BaseTool):
    """增强的SQL查询工具，使用MCP客户端"""
    name: str = "sql_query"
    description: str = "执行SQL查询语句，获取表结构、字段类型等信息。支持DESCRIBE, SHOW CREATE TABLE, SELECT等SQL命令"
    
    class Args(BaseModel):
        query: str = Field(description="要执行的SQL查询语句，如 DESCRIBE table_name 或 SHOW CREATE TABLE table_name")
    
    async def _arun(self, query: str) -> str:
        """异步执行SQL查询"""
        try:
            from src.mcp.mcp_client import execute_sql_via_mcp
            
            logger.info(f"执行MCP SQL查询: {query[:100]}...")
            result = await execute_sql_via_mcp(query)
            
            if result and "错误" not in result:
                logger.info(f"MCP SQL查询成功，返回 {len(result)} 字符")
                return result
            else:
                return result or "查询执行成功，但无返回结果"
                
        except Exception as e:
            error_msg = f"MCP SQL查询失败: {str(e)}"
            logger.error(error_msg)
            return f"错误: {error_msg}"
    
    def _run(self, query: str) -> str:
        """同步执行SQL查询"""
        return asyncio.run(self._arun(query))

class NotebookExportTool(BaseTool):
    """导出笔记本代码的工具（兼容旧版本）"""
    name: str = "notebook_export"
    description: str = "导出指定路径的笔记本源代码"
    mcp_client: Any = Field(exclude=True)
    
    class Args(BaseModel):
        path: str = Field(description="笔记本路径")
        format: str = Field(default="SOURCE", description="导出格式")
    
    async def _arun(self, path: str, format: str = "SOURCE") -> str:
        """异步导出笔记本"""
        try:
            result = await self.mcp_client.export_notebook(path, format)
            logger.info(f"笔记本导出成功: {path}")
            return result
        except Exception as e:
            error_msg = f"笔记本导出失败: {str(e)}"
            logger.error(error_msg)
            return f"错误: {error_msg}"
    
    def _run(self, path: str, format: str = "SOURCE") -> str:
        """同步导出笔记本"""
        return asyncio.run(self._arun(path, format))

class EnhancedNotebookExportTool(BaseTool):
    """增强的笔记本导出工具，使用MCP客户端"""
    name: str = "notebook_export"
    description: str = "导出指定路径的Databricks笔记本源代码。支持Python、SQL、Scala等格式"
    
    class Args(BaseModel):
        path: str = Field(description="笔记本在Databricks中的完整路径，如 /path/to/notebook")
        format: str = Field(default="SOURCE", description="导出格式：SOURCE（源代码）、HTML、JUPYTER等")
    
    async def _arun(self, path: str, format: str = "SOURCE") -> str:
        """异步导出笔记本"""
        try:
            from src.mcp.mcp_client import get_mcp_client
            
            logger.info(f"执行MCP笔记本导出: {path} (格式: {format})")
            
            # 使用MCP客户端查找并调用笔记本导出工具
            async with get_mcp_client() as client:
                if client:
                    tools = client.get_tools()
                    export_tool = None
                    for tool in tools:
                        if hasattr(tool, 'name') and 'export' in tool.name.lower():
                            export_tool = tool
                            break
                    
                    if export_tool:
                        result = await export_tool.ainvoke({"path": path, "format": format})
                        if result:
                            logger.info(f"MCP笔记本导出成功: {path}，内容长度: {len(str(result))}")
                            return str(result)
                        else:
                            return f"笔记本导出完成，但文件为空: {path}"
                    else:
                        return "错误: 未找到笔记本导出工具"
                else:
                    return "错误: MCP客户端未连接"
                
        except Exception as e:
            error_msg = f"MCP笔记本导出失败 {path}: {str(e)}"
            logger.error(error_msg)
            return f"错误: {error_msg}"
    
    def _run(self, path: str, format: str = "SOURCE") -> str:
        """同步导出笔记本"""
        return asyncio.run(self._arun(path, format))

class CodeAnalysisTool(BaseTool):
    """分析代码提取表信息的工具"""
    name: str = "code_analysis"
    description: str = "分析Python/SQL代码，提取其中引用的表名"
    
    class Args(BaseModel):
        code: str = Field(description="要分析的代码")
        code_type: str = Field(default="python", description="代码类型：python或sql")
    
    def _run(self, code: str, code_type: str = "python") -> str:
        """分析代码提取表信息"""
        try:
            tables = self._extract_tables_from_code(code, code_type)
            return f"找到以下表引用: {', '.join(tables)}"
        except Exception as e:
            return f"代码分析失败: {str(e)}"
    
    def _extract_tables_from_code(self, code: str, code_type: str) -> List[str]:
        """从代码中提取表名"""
        tables = set()
        
        if code_type.lower() == "python":
            # 查找 spark.table("xxx") 或 spark.sql("SELECT ... FROM xxx")
            table_patterns = [
                r'spark\.table\(["\']([^"\']+)["\']\)',
                r'spark\.sql\(["\'][^"\']*FROM\s+([^\s"\';]+)',
                r'df\s*=\s*spark\.table\(["\']([^"\']+)["\']\)',
                r'\.read\.[^(]*\(["\']([^"\']+)["\']\)'
            ]
        else:  # SQL
            # 查找 FROM 语句
            table_patterns = [
                r'FROM\s+([^\s;,\)]+)',
                r'JOIN\s+([^\s;,\)]+)',
                r'UPDATE\s+([^\s;,\)]+)',
                r'INSERT\s+INTO\s+([^\s;,\)]+)'
            ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, code, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                # 清理表名（去除引号、括号等）
                table_name = re.sub(r'["\';()]', '', match.strip())
                if '.' in table_name:  # 格式如 schema.table
                    tables.add(table_name)
                elif len(table_name) > 2:  # 避免太短的匹配
                    tables.add(table_name)
        
        return list(tables)

class CodeEnhanceAgent:
    """代码增强智能代理"""
    
    def __init__(self, llm):
        self.llm = llm
        self.agent = None
        self.checkpointer = InMemorySaver()
        
    async def _initialize_agent(self):
        """初始化智能体和工具"""
        if self.agent is None:
            config_manager = get_config_manager()
            tools = []
            
            try:
                logger.info("正在初始化MCP工具...")
                
                # 尝试获取MCP工具
                async with get_mcp_tools() as mcp_tools:
                    if mcp_tools:
                        # 直接使用MCP工具
                        tools.extend(mcp_tools)
                        logger.info(f"成功获取 {len(mcp_tools)} 个MCP工具")
                    else:
                        # 如果无法获取MCP工具，创建本地增强工具
                        logger.info("无法获取MCP工具，使用本地增强工具")
                        tools = [
                            EnhancedSQLQueryTool(),
                            EnhancedNotebookExportTool()
                        ]
                
                # 添加代码分析工具
                tools.append(CodeAnalysisTool())
                logger.info(f"总共初始化 {len(tools)} 个工具")
                
            except Exception as e:
                logger.error(f"MCP工具初始化失败: {e}")
                # 最终备用方案：仅使用代码分析工具
                tools = [CodeAnalysisTool()]
                logger.info("回退到基础代码分析模式")
            
            # 从配置文件获取提示词
            react_prompt = config_manager.get_prompt("react_agent_prompt")
            
            # 创建ReAct智能体
            self.agent = create_react_agent(
                model=self.llm,
                tools=tools,
                prompt=react_prompt,
                checkpointer=self.checkpointer
            )
            
    async def enhance_model_code(self, enhancement_request: Dict[str, Any]) -> Dict[str, Any]:
        """增强模型代码的主要方法"""
        
        try:
            # 初始化智能体
            await self._initialize_agent()
            
            # 提取请求信息
            table_name = enhancement_request.get("table_name")
            source_code = enhancement_request.get("source_code")
            adb_code_path = enhancement_request.get("adb_code_path")
            fields = enhancement_request.get("fields", [])
            logic_detail = enhancement_request.get("logic_detail")
            
            logger.info(f"开始增强模型: {table_name}")
            
            # 判断代码类型
            if adb_code_path and adb_code_path.endswith('.sql'):
                code_type = "SQL"
            else:
                code_type = "Python"
            
            # 构造字段信息
            fields_info = []
            for field in fields:
                field_desc = f"- {field.get('physical_name')} ({field.get('attribute_name')})"
                if field.get('data_type'):
                    field_desc += f" - 类型: {field.get('data_type')}"
                fields_info.append(field_desc)
            
            # 构造增强请求消息
            request_message = f"""请为以下数据模型进行代码增强：

**目标表**: {table_name}
**代码类型**: {code_type}
**增强需求**: {logic_detail}

**新增字段**:
{chr(10).join(fields_info)}

**源代码**:
```{code_type.lower()}
{source_code}
```

请按以下步骤执行：
1. 使用sql_query工具查询目标表 {table_name} 的结构信息
2. 使用code_analysis工具分析源代码，提取底表名称
3. 对重要的底表使用sql_query工具查询结构，用于推断新字段的数据类型
4. 生成增强后的{code_type}代码、新建表DDL和ALTER语句

最终请严格按照JSON格式返回：
{{
  "enhanced_code": "增强后的{code_type}代码",
  "new_table_ddl": "包含新字段的完整CREATE TABLE语句",
  "alter_statements": "ALTER TABLE语句"
}}"""

            # 调用智能体
            config = {"configurable": {"thread_id": f"enhance_{table_name}"}}
            result = await self.agent.ainvoke(
                {"messages": [HumanMessage(content=request_message)]},
                config
            )
            
            # 解析智能体响应
            response_content = result["messages"][-1].content
            enhancement_result = self._parse_response(response_content)
            
            # 验证结果
            if enhancement_result.get("enhanced_code"):
                logger.info(f"代码增强成功: {table_name}")
                return {
                    "success": True,
                    "enhanced_code": enhancement_result.get("enhanced_code"),
                    "new_table_ddl": enhancement_result.get("new_table_ddl"),
                    "alter_statements": enhancement_result.get("alter_statements"),
                    "field_mappings": fields
                }
            else:
                error_msg = "智能体未能生成有效的增强代码"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }
                
        except Exception as e:
            logger.error(f"代码增强失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    def _parse_response(self, content: str) -> Dict[str, str]:
        """解析智能体响应"""
        import json
        import re
        
        try:
            # 尝试直接解析JSON
            return json.loads(content.strip())
        except json.JSONDecodeError:
            # 尝试提取JSON代码块
            json_match = re.search(r'```json\n(.*?)\n```', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1).strip())
                except json.JSONDecodeError:
                    pass
            
            # 尝试提取花括号内容
            brace_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
            if brace_match:
                try:
                    return json.loads(brace_match.group(0))
                except json.JSONDecodeError:
                    pass
            
            logger.warning(f"无法解析智能体响应为JSON: {content[:200]}...")
            return {"enhanced_code": "", "new_table_ddl": "", "alter_statements": ""}
            
    async def close(self):
        """关闭资源"""
        # 由于使用上下文管理器，MCP客户端会自动关闭，这里只需要重置agent
        if self.agent:
            logger.info("清理代码增强智能体资源")
            self.agent = None