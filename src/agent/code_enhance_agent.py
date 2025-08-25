import re
import logging
from typing import List
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

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
            logger.error(f"代码分析失败: {str(e)}")
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

