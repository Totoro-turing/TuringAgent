import time

from src.agent.edw_agents import (
    get_agent_manager,
    get_navigation_agent,
    get_chat_agent,
    get_validation_agent,
    get_shared_llm,
    get_shared_parser,
    get_shared_checkpointer
)
from src.models.edw_models import FieldDefinition, ModelEnhanceRequest
from src.cache import get_cache_manager
from src.config import get_config_manager
from langchain.prompts import PromptTemplate
from langgraph.graph import StateGraph, START, END
from langgraph.config import get_stream_writer
from langchain_core.messages import AnyMessage, HumanMessage
from typing import List, TypedDict, Annotated, Optional
from operator import add
from dotenv import load_dotenv
from langgraph.prebuilt import create_react_agent
from src.basic.filesystem.file_operate import FileSystemTool
from src.agent.code_enhance_agent import CodeEnhanceAgent
import hashlib
import uuid
import logging
import os
import json
import asyncio
import re
from datetime import datetime
from difflib import SequenceMatcher

# 初始化配置管理器
config_manager = get_config_manager()
system_config = config_manager.get_system_config()

# 配置日志
log_level = getattr(logging, system_config.log_level.upper(), logging.INFO)
logging.basicConfig(level=log_level)
logger = logging.getLogger(__name__)

# 初始化缓存管理器（使用配置文件中的设置）
from src.cache import init_cache_manager
cache_config = config_manager.get_cache_config()
if cache_config.enabled:
    cache_manager = init_cache_manager(
        ttl_seconds=cache_config.ttl_seconds,
        max_entries=cache_config.max_entries
    )
    logger.info(f"缓存管理器初始化完成 - TTL: {cache_config.ttl_seconds}秒, 最大条目: {cache_config.max_entries}")
else:
    cache_manager = None
    logger.info("缓存已禁用")



class SessionManager:
    """统一管理用户会话，特别是线程ID管理"""

    @staticmethod
    def generate_thread_id(user_id: str, agent_type: str = "default") -> str:
        """基于user_id和agent_type生成唯一的thread_id"""
        if not user_id or user_id.strip() == "":
            # 如果没有user_id，生成一个随机ID
            return str(uuid.uuid4())

        # 使用user_id和agent_type的组合生成thread_id，确保不同智能体的会话隔离
        combined_id = f"{user_id}_{agent_type}"
        thread_id_length = system_config.thread_id_length
        return hashlib.md5(combined_id.encode()).hexdigest()[:thread_id_length]

    @staticmethod
    def get_config(user_id: str = "", agent_type: str = "default") -> dict:
        """获取标准配置，不同agent_type的智能体会有独立的memory"""
        thread_id = SessionManager.generate_thread_id(user_id, agent_type)
        return {
            "configurable": {
                "thread_id": thread_id
            }
        }


def extract_tables_from_code(code: str) -> list:
    """从代码中提取引用的表名"""
    tables = set()

    # Python Spark 代码模式
    if "spark" in code.lower() or "pyspark" in code.lower():
        patterns = [
            r'spark\.table\(["\']([^"\']+)["\']\)',
            r'spark\.sql\(["\'][^"\']*FROM\s+([^\s"\';\),]+)',
            r'spark\.read\.table\(["\']([^"\']+)["\']\)',
            r'\.read\.[^(]*\(["\']([^"\']+)["\']\)'
        ]
    else:  # SQL 代码模式
        patterns = [
            r'FROM\s+([^\s;,\)\n]+)',
            r'JOIN\s+([^\s;,\)\n]+)',
            r'UPDATE\s+([^\s;,\)\n]+)',
            r'INSERT\s+INTO\s+([^\s;,\)\n]+)'
        ]

    for pattern in patterns:
        matches = re.findall(pattern, code, re.IGNORECASE | re.MULTILINE)
        for match in matches:
            table_name = re.sub(r'["\';()]', '', match.strip())
            if '.' in table_name and len(table_name) > 5:
                tables.add(table_name)

    return list(tables)


async def _fetch_table_fields_from_db(table_name: str) -> dict:
    """从数据库直接获取表字段信息（不使用缓存的原始函数）"""
    try:
        from src.mcp.mcp_client import execute_sql_via_mcp

        # 查询表结构
        desc_query = f"DESCRIBE {table_name}"
        result = await execute_sql_via_mcp(desc_query)

        if result and "错误" not in result.lower():
            # 解析字段信息
            fields = []
            lines = result.split('\n')
            for line in lines[1:]:  # 跳过标题行
                if line.strip():
                    parts = line.split('\t') if '\t' in line else line.split()
                    if len(parts) >= 2:
                        field_name = parts[0].strip()
                        field_type = parts[1].strip()
                        fields.append({
                            "name": field_name,
                            "type": field_type
                        })

            return {"status": "success", "fields": fields}
        else:
            return {"status": "error", "message": result or "查询无返回结果"}

    except Exception as e:
        logger.error(f"查询表字段失败 {table_name}: {e}")
        return {"status": "error", "message": str(e)}


async def get_table_fields_info(table_name: str) -> dict:
    """查询表的字段信息（带智能缓存）"""
    try:
        if cache_manager:
            # 使用缓存管理器获取表字段信息
            result = await cache_manager.get_table_fields(table_name, _fetch_table_fields_from_db)
            
            # 添加缓存命中统计到日志
            stats = cache_manager.get_stats()
            logger.debug(f"表字段查询完成: {table_name} | 缓存统计 - 命中率: {stats['hit_rate']}, 总请求: {stats['total_requests']}")
        else:
            # 直接查询（无缓存）
            result = await _fetch_table_fields_from_db(table_name)
        
        return result
        
    except Exception as e:
        logger.error(f"查询表字段失败 {table_name}: {e}")
        return {"status": "error", "message": str(e)}


def find_similar_fields(input_field: str, available_fields: list, threshold: Optional[float] = None) -> list:
    """查找相似的字段名"""
    if threshold is None:
        validation_config = config_manager.get_validation_config()
        threshold = validation_config.similarity_threshold
    
    similar_fields = []

    for field in available_fields:
        # 计算字符串相似度
        similarity = SequenceMatcher(None, input_field.lower(), field.lower()).ratio()
        if similarity >= threshold:
            similar_fields.append({
                "field_name": field,
                "similarity": similarity
            })

    # 按相似度排序
    similar_fields.sort(key=lambda x: x["similarity"], reverse=True)
    
    validation_config = config_manager.get_validation_config()
    max_suggestions = validation_config.max_suggestions
    return similar_fields[:max_suggestions]


async def validate_fields_against_base_tables(fields: list, base_tables: list, source_code: str) -> dict:
    """验证新增字段是否基于底表中的现有字段"""
    validation_result = {
        "valid": True,
        "invalid_fields": [],
        "suggestions": {},
        "base_tables_info": {}
    }

    # 获取所有底表的字段信息
    all_base_fields = []
    
    # 记录开始时间和缓存状态
    start_time = datetime.now()
    initial_stats = cache_manager.get_stats()

    for table_name in base_tables:
        logger.info(f"正在查询底表字段信息: {table_name}")
        table_info = await get_table_fields_info(table_name)

        if table_info["status"] == "success":
            table_fields = [field["name"] for field in table_info["fields"]]
            all_base_fields.extend(table_fields)
            validation_result["base_tables_info"][table_name] = table_fields
            logger.info(f"底表 {table_name} 包含字段: {table_fields}")
        else:
            logger.warning(f"无法获取底表 {table_name} 的字段信息: {table_info['message']}")
            validation_result["base_tables_info"][table_name] = []
    
    # 记录结束时间和缓存统计
    end_time = datetime.now()
    final_stats = cache_manager.get_stats()
    
    # 计算本次验证的缓存效果
    cache_hits_delta = final_stats['cache_hits'] - initial_stats['cache_hits']
    cache_requests_delta = final_stats['total_requests'] - initial_stats['total_requests']
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"底表查询完成 - 耗时: {duration:.2f}秒, 查询了{len(base_tables)}个表, 缓存命中: {cache_hits_delta}/{cache_requests_delta}")
    validation_result["cache_performance"] = {
        "duration_seconds": round(duration, 2),
        "tables_queried": len(base_tables),
        "cache_hits": cache_hits_delta,
        "cache_requests": cache_requests_delta,
        "overall_hit_rate": final_stats['hit_rate']
    }

    if not all_base_fields:
        logger.warning("未能获取到任何底表字段信息，跳过字段验证")
        return validation_result

    logger.info(f"所有底表字段: {all_base_fields}")

    # 检查每个新增字段
    for field in fields:
        physical_name = field.get("physical_name", "")

        # 检查是否在底表中存在相似字段
        similar_fields = find_similar_fields(physical_name, all_base_fields)

        if not similar_fields:
            validation_result["valid"] = False
            validation_result["invalid_fields"].append(physical_name)
            # 提供基于字段名称模式的建议
            pattern_suggestions = _generate_pattern_suggestions(physical_name, all_base_fields)
            if pattern_suggestions:
                validation_result["suggestions"][physical_name] = pattern_suggestions
            logger.warning(f"字段 {physical_name} 在底表中未找到相似字段")
        else:
            # 如果相似度不够高，也提供建议
            if similar_fields[0]["similarity"] < 0.8:
                validation_result["suggestions"][physical_name] = similar_fields
                logger.info(f"字段 {physical_name} 找到相似字段: {[f['field_name'] for f in similar_fields[:3]]}")

    return validation_result


def _generate_pattern_suggestions(field_name: str, available_fields: list) -> list:
    """基于字段名称模式生成建议"""
    suggestions = []
    field_parts = field_name.lower().split('_')

    for available_field in available_fields:
        available_parts = available_field.lower().split('_')

        # 检查是否有共同的词汇
        common_parts = set(field_parts) & set(available_parts)
        if common_parts:
            suggestions.append({
                "field_name": available_field,
                "reason": f"包含相同词汇: {', '.join(common_parts)}"
            })

    return suggestions[:3]


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


# 获取共享的agents和工具
llm = get_shared_llm()
checkpointer = get_shared_checkpointer()
parser = get_shared_parser()
llm_agent = get_navigation_agent()
chat_agent = get_chat_agent()
valid_agent = get_validation_agent()

logger.info("使用共享agent管理器初始化完成")

# 全局代码增强智能体
_global_code_enhancement_agent = None
_global_enhancement_tools = None

async def _get_global_code_enhancement_agent():
    """获取全局的代码增强智能体（懒加载）"""
    global _global_code_enhancement_agent, _global_enhancement_tools
    
    if _global_code_enhancement_agent is None:
        logger.info("正在初始化全局代码增强智能体...")
        
        try:
            # 获取MCP工具（只获取一次）
            from src.mcp.mcp_client import get_mcp_tools
            from src.agent.code_enhance_agent import CodeAnalysisTool
            
            tools = []
            try:
                async with get_mcp_tools() as mcp_tools:
                    if mcp_tools:
                        tools.extend(mcp_tools)
                        logger.info(f"全局agent获取到 {len(mcp_tools)} 个MCP工具")
            except Exception as e:
                logger.warning(f"全局agent MCP工具获取失败: {e}")
            
            # 添加基础代码分析工具
            tools.append(CodeAnalysisTool())
            _global_enhancement_tools = tools
            
            # 使用通用的系统提示词创建agent
            system_prompt = config_manager.get_prompt("code_enhance_system_prompt")
            
            # 创建全局的ReAct智能体
            _global_code_enhancement_agent = create_react_agent(
                model=get_shared_llm(),
                tools=tools,
                prompt=system_prompt,  # 使用系统级提示词
                checkpointer=get_shared_checkpointer()
            )
            
            logger.info(f"全局代码增强智能体初始化成功，共 {len(tools)} 个工具")
            
        except Exception as e:
            logger.error(f"全局代码增强智能体初始化失败: {e}")
            # 创建最简单的fallback agent
            from src.agent.code_enhance_agent import CodeAnalysisTool
            tools = [CodeAnalysisTool()]
            _global_enhancement_tools = tools
            _global_code_enhancement_agent = create_react_agent(
                model=get_shared_llm(),
                tools=tools,
                prompt="你是一个代码增强助手。",
                checkpointer=get_shared_checkpointer()
            )
    
    return _global_code_enhancement_agent, _global_enhancement_tools

# langgraph 做法


# 统一的状态管理
class EDWState(TypedDict):
    """EDW系统统一状态管理"""
    messages: Annotated[List[AnyMessage], add]
    type: str  # 任务类型：other, model_enhance, model_add等
    user_id: str  # 用户ID，用于会话隔离

    # 模型开发相关信息
    table_name: Optional[str]  # 表名
    code_path: Optional[str]  # 代码路径
    adb_code_path: Optional[str]  # ADB中的代码路径（从code_path转换而来）
    source_code: Optional[str]  # 源代码
    enhance_code: Optional[str]  # 增强后的代码
    create_table_sql: Optional[str]  # 建表语句
    alter_table_sql: Optional[str]  # 修改表语句

    # 信息收集相关
    requirement_description: Optional[str]  # 需求描述
    logic_detail: Optional[str]  # 逻辑详情
    fields: Optional[List[dict]]  # 新增字段列表（每个字段包含physical_name, attribute_name等）
    collected_info: Optional[dict]  # 已收集的信息
    missing_info: Optional[List[str]]  # 缺失的信息列表

    # 会话状态
    session_state: Optional[str]  # 当前会话状态
    error_message: Optional[str]  # 错误信息


def navigate_node(state: EDWState):
    """导航节点：负责用户输入的初始分类"""
    print(">>> navigate Node")
    writer = get_stream_writer()
    writer({"node": ">>> navigate"})

    # 如果已经有type，直接返回
    if 'type' in state and state['type'] != 'other':
        return {"type": state['type'], "user_id": state.get("user_id", "")}

    prompt_template = config_manager.get_prompt("navigation_prompt")
    prompt = PromptTemplate.from_template(prompt_template)

    try:
        # 使用配置管理器 - 导航智能体独立memory
        config = SessionManager.get_config(state.get("user_id", ""), "navigation")

        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)

        response = llm_agent.invoke(
            {"messages": [{"role": "user", "content": prompt.format(input=content)}]},
            config
        )

        classification = response["messages"][-1].content.strip().lower()
        logger.info(f"Navigation classification: {classification}")

        if "other" in classification:
            return {"type": "other", "user_id": state.get("user_id", "")}
        else:
            return {"type": "model_enhance", "user_id": state.get("user_id", "")}
    except Exception as e:
        error_msg = f"导航节点处理失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}


def chat_node(state: EDWState):
    """聊天节点：处理普通对话"""
    print(">>> chat Node")
    try:
        writer = get_stream_writer()
        writer({"node": ">>> chat"})
    except RuntimeError:
        # 如果不在LangGraph执行上下文中，创建一个空的writer
        writer = lambda x: None
    try:
        # 使用配置管理器 - 聊天智能体独立memory
        config = SessionManager.get_config(state.get("user_id", ""), "chat")

        # 获取最后一条消息的内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)

        response = chat_agent.invoke(
            {"messages": [{"role": "user", "content": content}]},
            config
        )

        # 输出响应内容到流
        response_content = response["messages"][-1].content
        writer({"content": response_content})
        logger.info(f"Chat response: {response_content[:100]}...")

        return {"messages": response["messages"]}
    except Exception as e:
        error_msg = f"聊天节点处理失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {"messages": [HumanMessage("抱歉，我遇到了一些问题，请稍后再试。")], "error_message": error_msg}

# 主要分配模型增强等相关工作


def edw_model_node(state: EDWState):
    """模型节点：进一步分类模型相关任务"""
    print(">>> edw_model Node")
    print(f">>> {state['messages']}")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model"})

    # 如果已经识别到具体的意图类型，直接返回
    if state.get("type") in ["model_enhance", "model_add", "switch_model"]:
        logger.info(f"已识别意图类型: {state['type']}，跳过重复检测")
        return {"type": state["type"], "user_id": state.get("user_id", "")}

    prompt_template = config_manager.get_prompt("model_classification_prompt")
    prompt = PromptTemplate.from_template(prompt_template)

    try:
        # 使用配置管理器 - 模型智能体独立memory
        config = SessionManager.get_config(state.get("user_id", ""), "model")

        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)

        response = llm_agent.invoke(
            {"messages": [{"role": "user", "content": prompt.format(input=content)}]},
            config
        )

        classification = response["messages"][-1].content.strip().lower()
        logger.info(f"Model classification result: {classification}")

        if "model_enhance" in classification:
            return {"type": "model_enhance", "user_id": state.get("user_id", "")}
        elif "model_add" in classification:
            return {"type": "model_add", "user_id": state.get("user_id", "")}
        else:
            return {"type": "switch_model", "user_id": state.get("user_id", "")}
    except Exception as e:
        error_msg = f"模型节点分类失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}


def search_table_cd(table_name: str) -> str:
    """
    查询某个表的源代码
    :param table_name: 必要参数，具体表名比如dwd_fi.fi_invoice_item，
    :return：返回结果对象类型为解析之后的JSON格式对象，并用字符串形式进行表示，其中包含了源代码信息
    """
    system = FileSystemTool()
    schema = table_name.split(".")[0]
    name = table_name.split(".")[1]
    files = system.search_files_by_name("nb_" + name)
    if not files:
        return f"""{
            "status": "error",
            "message": "未找到表 {table_name} 的相关代码"
        }"""
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
            'timestamp': datetime.now().isoformat()
        }
        return str(file_info)
    return f"""
            {
        "status": "error",
            "message": "暂不支持的代码文件格式: {file.name}, 仅支持 .sql 和 .py 文件。请检查表名或代码文件格式。"
            }
        """


# 模型增强前针对数据进行校验验证
async def edw_model_enhance_data_validation_node(state: EDWState):
    """模型增强数据验证节点：验证用户输入信息的完整性"""
    print(">>> edw_model_enhance_data_validation Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_enhance_data_validation"})

    try:
        config = SessionManager.get_config(state.get("user_id", ""), "validation")

        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)

        writer({"status": "正在分析用户需求..."})

        # 使用验证代理提取关键信息
        response = valid_agent.invoke(
            {"messages": [{"role": "user", "content": content}]},
            config
        )

        # 获取LLM响应
        validation_result = response["messages"][-1].content
        logger.info(f"LLM原始响应: {validation_result}")
        writer({"llm_response": validation_result})

        # 使用LangChain输出解析器优雅地解析响应
        try:
            # 使用PydanticOutputParser解析LLM响应
            parsed_request = parser.parse(validation_result)
            writer({"parsed_data": parsed_request.dict()})

            # 验证信息完整性
            is_complete, missing_fields = parsed_request.validate_completeness()

            if not is_complete:
                # 构建完整的提示信息
                missing_info_text = "\n".join([f"- {info}" for info in missing_fields])

                # 如果是新增字段但缺少字段信息，添加额外提示
                if parsed_request.enhancement_type == "add_field" or any(keyword in parsed_request.logic_detail for keyword in ["增加字段", "新增字段", "添加字段"]):
                    if "字段定义" in str(missing_fields):
                        missing_info_text += "\n\n示例格式：\n"
                        missing_info_text += "单个字段：给dwd_fi.fi_invoice_item表增加字段invoice_doc_no（Invoice Document Number）\n"
                        missing_info_text += "多个字段：给表增加invoice_doc_no（Invoice Document Number）和customer_type（Customer Type）两个字段"

                complete_message = f"为了帮您完成模型增强，我需要以下信息：\n{missing_info_text}\n\n请补充完整信息后重新提交。"

                writer({"error": complete_message})
                writer({"missing_fields": missing_fields})
                writer({"content": complete_message})  # 确保用户能看到提示

                # 返回特殊的type标记，表示信息不完整需要直接结束
                return {
                    "type": "incomplete_info",  # 特殊标记
                    "missing_info": missing_fields,
                    "error_message": complete_message,
                    "table_name": parsed_request.table_name if "table_name" not in missing_fields else "",
                    "user_id": state.get("user_id", ""),
                    "messages": [HumanMessage(complete_message)]  # 添加消息以便用户看到
                }

            table_name = parsed_request.table_name.strip()
            logic_detail = parsed_request.logic_detail.strip()

            writer({"status": f"正在查询表 {table_name} 的源代码..."})

            # 调用search_table_cd查询表的源代码
            try:
                table_code_result = search_table_cd(table_name)
                logger.info(f"表代码查询结果: {table_code_result[:200]}...")

                # 解析表代码查询结果
                code_info = json.loads(table_code_result) if isinstance(table_code_result, str) else table_code_result

                if code_info.get("status") == "error":
                    error_msg = f"未找到表 {table_name} 的源代码: {code_info.get('message', '未知错误')}\n请确认表名是否正确。"
                    writer({"error": error_msg})
                    writer({"content": error_msg})
                    return {
                        "type": "incomplete_info",  # 标记为信息不完整
                        "error_message": error_msg,
                        "table_name": table_name,
                        "user_id": state.get("user_id", ""),
                        "messages": [HumanMessage(error_msg)]
                    }

                writer({"status": "信息收集完成，开始验证字段与底表的关联性"})
                writer({"table_found": True, "table_name": table_name})

                # 转换为ADB路径
                code_path = code_info.get("file_path", "")
                adb_path = convert_to_adb_path(code_path)

                # 提取源代码中的底表
                source_code = code_info.get("code", "")
                base_tables = extract_tables_from_code(source_code)
                logger.info(f"从源代码中提取到底表: {base_tables}")

                # 验证字段与底表的关联性
                if base_tables and parsed_request.fields:
                    writer({"status": f"正在验证 {len(parsed_request.fields)} 个新增字段与底表的关联性..."})

                    field_validation = await validate_fields_against_base_tables(
                        parsed_request.fields,
                        base_tables,
                        source_code
                    )

                    if not field_validation["valid"]:
                        # 构建字段验证错误信息
                        invalid_fields_msg = []
                        for invalid_field in field_validation["invalid_fields"]:
                            field_msg = f"- **{invalid_field}**: 在底表中未找到相似字段"

                            if invalid_field in field_validation["suggestions"]:
                                suggestions = field_validation["suggestions"][invalid_field]
                                if suggestions:
                                    suggestion_list = []
                                    for suggestion in suggestions[:3]:
                                        if "similarity" in suggestion:
                                            suggestion_list.append(f"{suggestion['field_name']} (相似度: {suggestion['similarity']:.2f})")
                                        else:
                                            suggestion_list.append(f"{suggestion['field_name']} ({suggestion.get('reason', '')})")
                                    field_msg += f"\\n  建议字段: {', '.join(suggestion_list)}"

                            invalid_fields_msg.append(field_msg)

                        # 显示底表信息
                        base_tables_info = []
                        for table_name_info, fields_list in field_validation["base_tables_info"].items():
                            if fields_list:
                                base_tables_info.append(f"- **{table_name_info}**: {', '.join(fields_list[:10])}{'...' if len(fields_list) > 10 else ''}")

                        # 添加缓存性能信息
                        cache_info = ""
                        if "cache_performance" in field_validation:
                            cache_perf = field_validation["cache_performance"]
                            cache_info = f"\\n\\n**查询性能**: 耗时{cache_perf['duration_seconds']}秒, 缓存命中率: {cache_perf['overall_hit_rate']}"

                        validation_error_msg = f"""字段验证失败，以下字段在底表中未找到相似字段：

{chr(10).join(invalid_fields_msg)}

**底表字段信息**:
{chr(10).join(base_tables_info) if base_tables_info else '无法获取底表字段信息'}{cache_info}

请确认字段名称是否正确，或参考建议字段进行修正。"""

                        writer({"error": validation_error_msg})
                        writer({"content": validation_error_msg})
                        writer({"field_validation": field_validation})

                        return {
                            "type": "incomplete_info",
                            "error_message": validation_error_msg,
                            "field_validation": field_validation,
                            "table_name": table_name,
                            "user_id": state.get("user_id", ""),
                            "messages": [HumanMessage(validation_error_msg)]
                        }
                    else:
                        writer({"status": "字段验证通过"})
                        
                        # 添加缓存性能信息到成功验证的情况
                        if "cache_performance" in field_validation:
                            cache_perf = field_validation["cache_performance"]
                            writer({"cache_performance": f"查询性能: 耗时{cache_perf['duration_seconds']}秒, 缓存命中率: {cache_perf['overall_hit_rate']}"})
                        
                        if field_validation["suggestions"]:
                            suggestions_msg = "字段建议：\\n"
                            for field_name, suggestions in field_validation["suggestions"].items():
                                suggestions_msg += f"- {field_name}: 发现相似字段 {suggestions[0]['field_name']} (相似度: {suggestions[0]['similarity']:.2f})\\n"
                            writer({"field_suggestions": suggestions_msg})
                else:
                    logger.info("未找到底表或新增字段为空，跳过字段验证")

                # 将所有信息存储到state中
                return {
                    "type": "model_enhance_node",
                    "user_id": state.get("user_id", ""),
                    # 存储解析的需求信息（直接使用Pydantic对象属性）
                    "table_name": table_name,
                    "logic_detail": logic_detail,
                    "enhancement_type": parsed_request.enhancement_type,
                    "field_info": parsed_request.field_info,
                    "business_requirement": parsed_request.business_requirement,
                    # 新增字段列表（存储为字典列表）
                    "fields": [field.dict() for field in parsed_request.fields] if parsed_request.fields else [],
                    # 存储表代码信息
                    "source_code": code_info.get("code", ""),
                    "code_path": code_path,
                    "adb_code_path": adb_path,  # 新增ADB路径
                    "collected_info": {
                        "validation_result": validation_result,
                        "parsed_requirements": parsed_request.dict(),
                        "table_code_info": code_info,
                        "adb_path": adb_path,
                        "timestamp": datetime.now().isoformat()
                    },
                    "session_state": "validation_completed"
                }

            except Exception as code_error:
                error_msg = f"查询表代码失败: {str(code_error)}"
                logger.error(error_msg)
                writer({"error": error_msg})
                return {
                    "error_message": error_msg,
                    "table_name": table_name,
                    "user_id": state.get("user_id", "")
                }

        except Exception as parse_error:
            # LangChain的parser可能抛出多种异常，统一处理
            error_msg = "信息格式解析失败。请使用更清晰的格式描述需求，确保包含：\n1. 表名（如：dwd_fi.fi_invoice_item）\n2. 具体的增强逻辑"
            logger.error(f"解析错误: {str(parse_error)}. 原始响应: {validation_result}")
            writer({"error": error_msg})
            writer({"content": error_msg})
            return {
                "type": "incomplete_info",  # 标记为信息不完整
                "error_message": error_msg,
                "user_id": state.get("user_id", ""),
                "messages": [HumanMessage(error_msg)]
            }

    except Exception as e:
        error_msg = f"数据验证失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {"error_message": error_msg, "user_id": state.get("user_id", "")}


# 同步包装器函数，处理异步数据验证节点
def edw_model_enhance_data_validation_node_sync(state: EDWState):
    """模型增强数据验证节点的同步包装器"""
    print(">>> edw_model_enhance_data_validation Node (sync wrapper)")
    
    # 在同步上下文中运行异步函数
    import asyncio
    
    try:
        # 获取或创建事件循环
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果循环已在运行，创建一个新任务
                import concurrent.futures
                import threading
                
                result = None
                exception = None
                
                def run_async():
                    nonlocal result, exception
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        result = new_loop.run_until_complete(edw_model_enhance_data_validation_node(state))
                        new_loop.close()
                    except Exception as e:
                        exception = e
                
                thread = threading.Thread(target=run_async)
                thread.start()
                thread.join()
                
                if exception:
                    raise exception
                return result
            else:
                # 循环未运行，直接使用
                return loop.run_until_complete(edw_model_enhance_data_validation_node(state))
        except RuntimeError:
            # 没有事件循环，创建新的
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(edw_model_enhance_data_validation_node(state))
            finally:
                loop.close()
                
    except Exception as e:
        logger.error(f"异步节点执行失败: {e}")
        writer = get_stream_writer()
        writer({"error": f"数据验证失败: {str(e)}"})
        return {
            "error_message": f"数据验证失败: {str(e)}",
            "user_id": state.get("user_id", "")
        }

# 新增模型前主要针对数据进行校验验证
def edw_model_add_data_validation_node(state: EDWState):
    """模型新增数据验证节点"""
    print(">>> edw_model_add_data_validation Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_add_data"})
    writer({"status": "数据验证进行中"})
    return {}


# 主要进行模型增强等相关工作
async def _run_code_enhancement(table_name: str, source_code: str, adb_code_path: str,
                                fields: list, logic_detail: str, writer, code_path: str = "") -> dict:
    """异步执行代码增强的核心函数"""
    try:
        writer({"progress": "正在获取全局代码增强智能体..."})

        # 判断代码类型
        file_path = code_path or adb_code_path or ""
        if file_path.endswith('.sql'):
            code_language = "sql"
            code_type_desc = "SQL"
        else:
            code_language = "python"
            code_type_desc = "Python"

        writer({"progress": f"检测到代码类型: {code_type_desc}"})

        # 获取全局的代码增强智能体（复用）
        enhancement_agent, tools = await _get_global_code_enhancement_agent()
        writer({"progress": f"使用全局智能体，共 {len(tools)} 个工具"})

        # 构造字段信息字符串
        fields_info = [f"{field['physical_name']} ({field['attribute_name']})" for field in fields]

        # 构造具体的任务消息（而不是修改agent的系统prompt）
        task_message = f"""请为以下数据模型进行代码增强：

**目标表**: {table_name}
**代码类型**: {code_type_desc}
**增强需求**: {logic_detail}

**新增字段**:
{chr(10).join(fields_info)}

**源代码**:
```{code_language.lower()}
{source_code}
```

请按以下步骤执行：
1. 使用sql_query工具查询目标表 {table_name} 的结构信息
2. 使用code_analysis工具分析源代码，提取底表名称
3. 对重要的底表使用sql_query工具查询结构，用于推断新字段的数据类型
4. 生成增强后的{code_type_desc}代码、新建表DDL和ALTER语句

最终请严格按照JSON格式返回：
{{
  "enhanced_code": "增强后的{code_type_desc}代码",
  "new_table_ddl": "包含新字段的完整CREATE TABLE语句",
  "alter_statements": "ALTER TABLE语句"
}}"""

        writer({"progress": "正在执行代码增强，智能体将自动分析表结构和依赖关系..."})
        writer({"status": f"智能体开始处理表 {table_name} 的增强需求..."})

        # 使用配置管理器获取配置 - 为每个用户生成独立的thread_id
        config = SessionManager.get_config("", f"enhancement_{table_name}")

        # 调用全局智能体执行增强任务
        result = enhancement_agent.invoke(
            {"messages": [HumanMessage(task_message)]},
            config
        )

        # 解析智能体的响应
        response_content = result["messages"][-1].content
        enhancement_result = _parse_agent_response(response_content)

        if enhancement_result.get("enhanced_code"):
            writer({"progress": "代码增强成功完成"})
            writer({"enhancement_details": {
                "enhanced_code_length": len(enhancement_result.get("enhanced_code", "")),
                "fields_processed": len(fields),
                "has_create_table": bool(enhancement_result.get("new_table_ddl")),
                "has_alter_table": bool(enhancement_result.get("alter_statements"))
            }})

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
            writer({"error": error_msg})
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        error_msg = f"执行代码增强时发生异常: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {
            "success": False,
            "error": error_msg
        }
    finally:
        # MCP客户端使用上下文管理器，无需手动清理
        logger.debug("代码增强任务完成")


def _parse_agent_response(content: str) -> dict:
    """解析智能体响应，提取JSON结果"""
    import json
    import re

    default_result = {
        "enhanced_code": "",
        "new_table_ddl": "",
        "alter_statements": ""
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


def edw_model_enhance_node(state: EDWState):
    """模型增强处理节点"""
    print(">>> edw_model_enhance Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_enhance"})

    try:
        # 提取状态中的信息
        table_name = state.get("table_name")
        source_code = state.get("source_code")
        adb_code_path = state.get("adb_code_path")
        code_path = state.get("adb_code_path")
        fields = state.get("fields", [])
        logic_detail = state.get("logic_detail")
        user_id = state.get("user_id", "")

        writer({"status": f"开始增强模型 {table_name}..."})
        writer({"progress": "正在初始化代码增强引擎"})

        # 验证必要信息
        if not table_name or not source_code:
            error_msg = "缺少必要信息：表名或源代码为空"
            writer({"error": error_msg})
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        if not fields:
            error_msg = "没有找到新增字段信息"
            writer({"error": error_msg})
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        writer({"progress": f"准备增强 {len(fields)} 个字段"})

        # 异步执行代码增强
        enhancement_result = asyncio.run(_run_code_enhancement(
            table_name=table_name,
            source_code=source_code,
            adb_code_path=adb_code_path,
            fields=fields,
            logic_detail=logic_detail,
            writer=writer,
            code_path=code_path
        ))

        if enhancement_result.get("success"):
            writer({"status": "模型增强完成"})
            writer({"result": "代码增强成功"})

            return {
                "user_id": user_id,
                "enhance_code": enhancement_result.get("enhanced_code"),
                "create_table_sql": enhancement_result.get("new_table_ddl"),
                "alter_table_sql": enhancement_result.get("alter_statements"),
                "field_mappings": enhancement_result.get("field_mappings"),
                "enhancement_summary": {
                    "table_name": table_name,
                    "fields_added": len(fields),
                    "base_tables_analyzed": enhancement_result.get("base_tables_analyzed", 0),
                    "timestamp": datetime.now().isoformat()
                },
                "session_state": "enhancement_completed"
            }
        else:
            error_msg = enhancement_result.get("error", "未知错误")
            writer({"error": f"代码增强失败: {error_msg}"})
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

    except Exception as e:
        error_msg = f"模型增强节点处理失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }

# 主要进行新增模型等相关工作


def edw_model_addition_node(state: EDWState):
    """模型新增处理节点"""
    print(">>> edw_model_addition Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_addition"})
    writer({"status": "模型新增处理中"})
    return {}


# 负责发送邮件
def edw_email_node(state: EDWState):
    """邮件通知节点"""
    print(">>> edw_email Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_email"})
    writer({"status": "发送邮件通知"})
    return {}

# 负责更新confluence page


def edw_confluence_node(state: EDWState):
    """Confluence文档更新节点"""
    print(">>> edw_confluence Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_confluence"})
    writer({"status": "更新Confluence文档"})
    return {}


def edw_adb_update_node(state: EDWState):
    """ADB数据库更新节点"""
    print(">>> edw_adb_update Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_adb_update"})
    writer({"status": "更新ADB数据库"})
    return {}


def model_routing_fun(state: EDWState):
    """模型开发路由函数"""
    print(">>> model_routing_fun")
    print(state)
    if state["type"] == "model_enhance":
        return "model_enhance_data_validation_node"
    elif state["type"] == "model_add":
        return "model_add_data_validation_node"
    else:
        return END


def validation_routing_fun(state: EDWState):
    """数据验证后的路由函数：决定继续处理还是直接结束"""
    print(">>> validation_routing_fun")
    print(f"State type: {state.get('type')}")

    # 如果信息不完整，直接结束
    if state.get("type") == "incomplete_info":
        logger.info("信息不完整，直接结束流程")
        return END

    # 如果是模型增强节点，继续处理
    if state.get("type") == "model_enhance_node":
        return "model_enhance_node"

    # 默认结束
    return END


model_dev_graph = (
    StateGraph(EDWState)
    .add_node("model_enhance_data_validation_node", edw_model_enhance_data_validation_node_sync)
    .add_node("model_add_data_validation_node", edw_model_add_data_validation_node)
    .add_node("model_enhance_node", edw_model_enhance_node)
    .add_node("model_addition_node", edw_model_addition_node)
    .add_node("adb_update_node", edw_adb_update_node)
    .add_node("email_node", edw_email_node)
    .add_node("confluence_node", edw_confluence_node)
    .add_conditional_edges(START, model_routing_fun, ["model_enhance_data_validation_node", "model_add_data_validation_node"])
    # 添加数据验证后的条件路由
    .add_conditional_edges("model_enhance_data_validation_node", validation_routing_fun, ["model_enhance_node", END])
    .add_edge("model_add_data_validation_node", "model_addition_node")
    .add_edge("model_enhance_node", "adb_update_node")
    .add_edge("model_addition_node", "adb_update_node")
    .add_edge("adb_update_node", "confluence_node")
    .add_edge("confluence_node", "email_node")
    .add_edge("email_node", END)
)

model_dev = model_dev_graph.compile()


def routing_fun(state: EDWState):
    """主路由函数：决定进入聊天还是模型处理"""
    if state["type"] == "model_enhance":
        return "model_node"
    return "chat_node"


# 一级导航图
guid_graph = (
    StateGraph(EDWState)
    .add_node("navigate_node", navigate_node)
    .add_node("chat_node", chat_node)
    .add_node("model_node", edw_model_node)
    .add_node("model_dev_node", model_dev)
    .add_edge(START, "navigate_node")
    .add_conditional_edges("navigate_node", routing_fun, ["chat_node", "model_node"])
    .add_edge("model_node", "model_dev_node")
    .add_edge("model_dev_node", END)
    .add_edge("chat_node", END)
)

guid = guid_graph.compile()


def create_message_from_input(user_input: str) -> HumanMessage:
    """将用户输入转换为标准消息格式"""
    return HumanMessage(content=user_input)


if __name__ == "__main__":
    print("EDW智能助手启动成功！输入'quit'退出。")
    print("管理命令：/cache help, /config help")

    # 模拟用户ID（实际应用中应该从认证系统获取）
    user_id = input("请输入用户ID（可选，直接回车使用随机ID）: ").strip()
    if not user_id:
        user_id = str(uuid.uuid4())[:8]

    print(f"当前用户ID: {user_id}")
    
    # 显示初始系统状态
    print(f"配置文件路径: {config_manager.config_dir}")
    
    # 显示MCP连接配置
    databricks_config = config_manager.get_mcp_server_config("databricks")
    if databricks_config:
        if databricks_config.type == "sse":
            print(f"MCP连接模式: SSE - {databricks_config.base_url}")
            print(f"连接参数: 超时={databricks_config.timeout}s, 重试={databricks_config.retry_count}次")
        else:
            print(f"MCP连接模式: 子进程 - {databricks_config.command}")
    else:
        print("MCP连接模式: 默认配置")
    
    # 显示缓存状态
    if cache_manager:
        stats = cache_manager.get_stats()
        print(f"缓存系统已启动 - TTL: {stats['ttl_seconds']}秒, 最大条目: {stats['max_entries']}")
    else:
        print("缓存系统已禁用")
    print(f"\n使用 '/test sse' 命令测试SSE连接")
    
    while True:
        try:
            readline = input("\n用户输入: ")
            if readline.lower() in ["quit", "exit", "退出"]:
                print("感谢使用EDW智能助手！")
                break

            if not readline.strip():
                print("请输入有效内容")
                continue

            # 处理缓存管理命令
            if readline.lower().startswith("/cache"):
                if cache_manager is None:
                    print("缓存系统已禁用")
                    continue
                    
                parts = readline.split()
                if len(parts) == 1 or parts[1] == "stats":
                    # 显示缓存统计
                    stats = cache_manager.get_stats()
                    print(f"\n=== 缓存统计信息 ===")
                    print(f"缓存条目数: {stats['cache_entries']}")
                    print(f"总请求数: {stats['total_requests']}")
                    print(f"缓存命中: {stats['cache_hits']}")
                    print(f"缓存未命中: {stats['cache_misses']}")
                    print(f"命中率: {stats['hit_rate']}")
                    print(f"TTL设置: {stats['ttl_seconds']}秒")
                    print(f"内存使用估算: {stats['memory_usage_estimate']}字节")
                    
                    # 显示已缓存的表
                    cached_tables = cache_manager.get_cached_tables()
                    if cached_tables:
                        print(f"已缓存的表 ({len(cached_tables)}个):")
                        for table in cached_tables[:10]:  # 最多显示10个
                            print(f"  - {table}")
                        if len(cached_tables) > 10:
                            print(f"  ... 还有 {len(cached_tables) - 10} 个表")
                    continue
                    
                elif parts[1] == "clear":
                    # 清除缓存
                    if len(parts) > 2:
                        # 清除特定表的缓存
                        table_pattern = parts[2]
                        cache_manager.clear_cache(table_pattern)
                        print(f"已清除匹配 '{table_pattern}' 的缓存")
                    else:
                        # 清除所有缓存
                        cache_manager.clear_cache()
                        print("已清除所有缓存")
                    continue
                    
                elif parts[1] == "help":
                    print("\n=== 缓存管理命令 ===")
                    print("/cache stats    - 显示缓存统计信息")
                    print("/cache clear    - 清除所有缓存")
                    print("/cache clear <表名模式> - 清除匹配的缓存")
                    print("/cache help     - 显示此帮助信息")
                    continue
            
            # 处理配置管理命令
            if readline.lower().startswith("/config"):
                parts = readline.split()
                if len(parts) == 1 or parts[1] == "show":
                    # 显示配置信息
                    try:
                        edw_config = config_manager.load_config()
                        print(f"\n=== 系统配置信息 ===")
                        print(f"日志级别: {edw_config.system.log_level}")
                        print(f"线程ID长度: {edw_config.system.thread_id_length}")
                        print(f"请求超时: {edw_config.system.request_timeout}秒")
                        print(f"\n=== 缓存配置 ===")
                        print(f"缓存启用: {'是' if edw_config.cache.enabled else '否'}")
                        print(f"TTL: {edw_config.cache.ttl_seconds}秒")
                        print(f"最大条目: {edw_config.cache.max_entries}")
                        print(f"\n=== 验证配置 ===")
                        print(f"相似度阈值: {edw_config.validation.similarity_threshold}")
                        print(f"最大建议数: {edw_config.validation.max_suggestions}")
                        print(f"\n=== MCP服务器 ===")
                        for name, server in edw_config.mcp_servers.items():
                            print(f"- {name}: {server.command} {' '.join(server.args)}")
                    except Exception as e:
                        print(f"获取配置信息失败: {e}")
                    continue
                    
                elif parts[1] == "reload":
                    # 重新加载配置
                    try:
                        config_manager.reload_config()
                        print("配置已重新加载")
                        
                        # 重新初始化组件
                        system_config = config_manager.get_system_config()
                        
                        # 重新初始化缓存管理器
                        cache_config = config_manager.get_cache_config()
                        if cache_config.enabled:
                            cache_manager = init_cache_manager(
                                ttl_seconds=cache_config.ttl_seconds,
                                max_entries=cache_config.max_entries
                            )
                            print(f"缓存管理器已重新初始化")
                        else:
                            cache_manager = None
                            print("缓存已禁用")
                        
                    except Exception as e:
                        print(f"重新加载配置失败: {e}")
                    continue
                    
                elif parts[1] == "path":
                    # 显示配置文件路径
                    print(f"\n=== 配置文件路径 ===")
                    print(f"配置目录: {config_manager.config_dir}")
                    print(f"主配置文件: {config_manager.config_file}")
                    print(f"提示词文件: {config_manager.prompts_file}")
                    continue
                    
                elif parts[1] == "help":
                    print("\n=== 配置管理命令 ===")
                    print("/config show     - 显示当前配置")
                    print("/config reload   - 重新加载配置文件")
                    print("/config path     - 显示配置文件路径")
                    print("/config help     - 显示此帮助信息")
                    continue

            # 使用统一配置管理器 - 主会话
            config = SessionManager.get_config(user_id, "main")

            # 创建标准消息格式
            initial_state = {
                "messages": [create_message_from_input(readline)],
                "user_id": user_id
            }

            print("\n处理中...")

            displayed_content = set()  # 避免重复显示相同内容
            
            for chunk in guid.stream(initial_state, config, stream_mode="updates"):
                if chunk:
                    # 统一处理所有节点的输出，不针对特定节点
                    for node_name, node_data in chunk.items():
                        if isinstance(node_data, dict):
                            # 优先处理包含messages的输出（最重要的AI响应）
                            if "messages" in node_data and node_data["messages"]:
                                messages = node_data["messages"]
                                last_message = messages[-1]
                                content = last_message.content if hasattr(last_message, 'content') else str(last_message)
                                content_hash = hash(content)
                                if content_hash not in displayed_content:
                                    print(f"\nAI: {content}")
                                    displayed_content.add(content_hash)
                            # 处理直接的content输出
                            elif "content" in node_data:
                                content = node_data['content']
                                content_hash = hash(content)
                                if content_hash not in displayed_content:
                                    print(f"\nAI: {content}")
                                    displayed_content.add(content_hash)
                            # 处理错误信息（高优先级）
                            elif "error" in node_data:
                                print(f"\n错误: {node_data['error']}")
                            # 处理状态信息（中优先级）
                            elif "status" in node_data:
                                print(f"状态: {node_data['status']}")
                            elif "progress" in node_data:
                                print(f"进度: {node_data['progress']}")
                            elif "warning" in node_data:
                                print(f"警告: {node_data['warning']}")

        except KeyboardInterrupt:
            print("\n用户中断操作")
            break
        except Exception as e:
            logger.error(f"主程序异常: {e}")
            print(f"发生错误: {e}")
