"""
代码处理工具函数
"""

import re
import os
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def extract_tables_from_code(code: str) -> list:
    """从代码中提取引用的表名"""
    tables = set()
    
    # Python Spark 代码模式
    if "spark" in code.lower() or "pyspark" in code.lower():
        patterns = [
            r'spark\.table\(["\']([^"\']+)["\']\)',
            r'spark\.sql\(["\'][^"\']*FROM\s+([^\s"\';),]+)',
            r'spark\.read\.table\(["\']([^"\']+)["\']\)',
            r'\.read\.[^(]*\(["\']([^"\']+)["\']\)'
        ]
    else:  # SQL 代码模式
        patterns = [
            r'FROM\s+([^\s;,)\n]+)',
            r'JOIN\s+([^\s;,)\n]+)',
            r'UPDATE\s+([^\s;,)\n]+)',
            r'INSERT\s+INTO\s+([^\s;,)\n]+)'
        ]
    
    for pattern in patterns:
        matches = re.findall(pattern, code, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            table_name = re.sub(r'["\';()]', '', match.strip())
            if '.' in table_name and len(table_name) > 5:
                tables.add(table_name)
    
    return list(tables)


def search_table_cd(table_name: str, branch_name: str = None) -> dict:
    """
    查询某个表的源代码（支持GitHub和本地搜索切换）
    :param table_name: 必要参数，具体表名比如dwd_fi.fi_invoice_item
    :param branch_name: 代码分支名称，如：main, dev, feature/xxx
    :return: 返回结果字典，包含状态和源代码信息
    """
    # 通过环境变量控制使用哪种搜索方式
    use_github = os.getenv("USE_GITHUB_SEARCH", "true").lower() == "true"
    
    if use_github:
        try:
            # 使用GitHub工具进行搜索（传入分支参数）
            from src.basic.github import GitHubTool
            github_tool = GitHubTool(branch=branch_name) if branch_name else GitHubTool()
            return github_tool.search_table_code(table_name)
        except Exception as e:
            logger.error(f"GitHub搜索失败: {e}")
            # 如果配置了回退到本地搜索
            if os.getenv("FALLBACK_TO_LOCAL", "false").lower() == "true":
                logger.info("回退到本地文件搜索")
                return _search_table_cd_local(table_name)
            return {"status": "error", "message": f"GitHub搜索失败: {str(e)}"}
    else:
        # 使用本地文件系统搜索
        return _search_table_cd_local(table_name)


def _search_table_cd_local(table_name: str) -> dict:
    """
    本地文件系统搜索实现（原始版本）
    """
    from src.basic.filesystem.file_operate import FileSystemTool
    from datetime import datetime
    
    system = FileSystemTool()
    schema = table_name.split(".")[0]
    name = table_name.split(".")[1]
    logger.info(f"正在本地查找表: {table_name} 代码")
    
    files = system.search_files_by_name("nb_" + name)
    if not files:
        return {"status": "error", "message": f"未找到表 {table_name} 的相关代码"}
    file = [i for i in files if schema in str(i)][0]
    if file.name.endswith(('.sql', '.py')):
        file_path = os.path.join(os.getenv("LOCAL_REPO_PATH"), str(file))
        last_modified = os.path.getmtime(file_path)
        language = 'sql' if file.name.endswith('.sql') else 'python'
        size = os.path.getsize(file_path)
        file_info = {
            'status': 'success',
            'table_name': table_name,
            'description': f"{table_name}表的数据加工代码",
            'code': system.read_file(str(file)),
            'language': language,
            'file_name': file.name,
            'file_path': str(file.absolute()),
            'file_size': size,
            'file_info': {
                'name': file.name,
                'language': language,
                'size': size,
                'last_modified': datetime.fromtimestamp(last_modified).strftime('%Y-%m-%d %H:%M:%S')
            },
            'timestamp': datetime.now().isoformat(),
            'source': 'local'  # 标记数据来源
        }
        return file_info
    return {"status": "error", "message": f"暂不支持的代码文件格式: {file.name}"}


def convert_to_adb_path(code_path: str) -> str:
    """
    将本地代码路径转换为ADB路径格式
    例如: D:\\code\\Finance\\Magellan-Finance-Databricks\\Magellan-Finance\\cam_fi\\Notebooks\\nb_daas_booking_actual_data_autoflow.py
    转换为: /Magellan-Finance/cam_fi/Notebooks/nb_daas_booking_actual_data_autoflow
    """
    if not code_path:
        return ""
    
    # 标准化路径分隔符
    normalized_path = code_path.replace("\\", "/")
    
    # 查找Magellan-Finance的位置
    magellan_index = normalized_path.find("Magellan-Finance")
    if magellan_index == -1:
        logger.warning(f"路径中未找到Magellan-Finance: {code_path}")
        # 如果没找到，尝试返回最后几个路径组件
        path_parts = normalized_path.split("/")
        # 去掉文件扩展名
        if path_parts[-1].endswith(('.py', '.sql')):
            path_parts[-1] = os.path.splitext(path_parts[-1])[0]
        # 返回最后4个组件
        return "/" + "/".join(path_parts[-4:]) if len(path_parts) >= 4 else "/" + "/".join(path_parts)
    
    # 获取从Magellan-Finance开始的路径
    adb_path = normalized_path[magellan_index:]
    
    # 去掉文件扩展名
    if adb_path.endswith(('.py', '.sql')):
        adb_path = os.path.splitext(adb_path)[0]
    
    # 确保路径以/开头
    if not adb_path.startswith("/"):
        adb_path = "/" + adb_path
    
    logger.info(f"路径转换: {code_path} -> {adb_path}")
    return adb_path


def detect_code_language(code_path: str, source_code: str = "") -> str:
    """检测代码语言"""
    if code_path:
        if code_path.endswith('.sql'):
            return 'SQL'  # Databricks SQL笔记本通常使用SCALA语言标识
        elif code_path.endswith('.py'):
            return 'PYTHON'
        elif code_path.endswith('.scala'):
            return 'SCALA'
        elif code_path.endswith('.r'):
            return 'R'
    
    # 从源代码内容推断
    if source_code:
        source_code_lower = source_code.lower()
        if 'spark.sql' in source_code_lower or 'pyspark' in source_code_lower or 'import ' in source_code_lower:
            return 'PYTHON'
        elif 'select ' in source_code_lower or 'create table' in source_code_lower:
            return 'SQL'
    
    # 默认返回Python
    return 'PYTHON'


def parse_agent_response(content: str) -> dict:
    """解析智能体响应，提取JSON结果"""
    
    default_result = {
        "enhanced_code": "",
        "new_table_ddl": "",
        "alter_statements": "",
        "table_comment": ""  # 表comment信息（模型名称）
    }
    
    try:
        # 尝试直接解析JSON
        result = json.loads(content.strip())
        return result
    except json.JSONDecodeError:
        # 如果解析失败，尝试提取JSON代码块
        json_match = re.search(r'```json\n(.*?)\n```', content, re.DOTALL)
        if json_match:
            try:
                result = json.loads(json_match.group(1).strip())
                return result
            except json.JSONDecodeError:
                logger.warning("JSON代码块解析失败")
        
        # 尝试找到花括号包围的内容
        brace_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        if brace_match:
            try:
                result = json.loads(brace_match.group(0))
                return result
            except json.JSONDecodeError:
                logger.warning("花括号内容解析失败")
        
        # 如果JSON解析都失败，尝试回退到原来的markdown解析
        logger.warning("JSON解析失败，回退到markdown解析")
        # 尝试提取代码块（python或sql）
        code_match = re.search(r'```(?:python|sql)\n(.*?)\n```', content, re.DOTALL)
        if code_match:
            default_result["enhanced_code"] = code_match.group(1).strip()
        
        sql_matches = re.findall(r'```sql\n(.*?)\n```', content, re.DOTALL)
        if len(sql_matches) >= 1:
            default_result["new_table_ddl"] = sql_matches[0].strip()
        if len(sql_matches) >= 2:
            default_result["alter_statements"] = sql_matches[1].strip()
        
        return default_result