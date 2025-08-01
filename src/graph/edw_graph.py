from src.graph.validation_nodes import create_validation_subgraph
from src.cache import init_cache_manager
import time

from src.agent.edw_agents import (
    get_agent_manager,
    get_navigation_agent,
    get_chat_agent,
    get_validation_agent,
    get_shared_llm,
    get_shared_parser,
    get_shared_checkpointer,
    get_business_checkpointer,
    get_interaction_checkpointer
)
# 适配器已移除，直接使用子图
from src.models.edw_models import FieldDefinition, ModelEnhanceRequest
from src.models.states import EDWState
from src.cache import get_cache_manager
from src.config import get_config_manager
from langchain.prompts import PromptTemplate
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document
from langgraph.graph import StateGraph, START, END
from langgraph.config import get_stream_writer
from langgraph.types import Command
from langchain.schema.messages import AnyMessage, HumanMessage, AIMessage
from typing import List, TypedDict, Annotated, Optional
from operator import add
from dotenv import load_dotenv
from langgraph.prebuilt import create_react_agent
from src.basic.filesystem.file_operate import FileSystemTool
from src.basic.github import GitHubTool
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


# 通用总结回复提示词常量
SUMMARY_REPLY_PROMPT = """你是一个专业的对话总结助手，负责分析用户与EDW系统的交互历史，生成简洁明了的总结。

**任务要求**：
1. 仔细分析提供的对话历史和上下文信息
2. 提取关键信息：用户需求、系统回应、当前状态、遇到的问题
3. 生成结构化的markdown格式总结
4. 语言风格要友好、专业、易懂

**上下文信息**：
{context_info}

**对话历史**：
{conversation_history}

**输出要求**：
- 必须使用markdown格式
- 包含关键信息的结构化展示
- 突出当前状态和下一步行动
- 如果有错误或问题，要明确指出
- 总结长度控制在200-400字
- 使用中文回复

请生成对话总结："""


def _extract_message_content(message) -> str:
    """统一提取消息内容"""
    if isinstance(message, str):
        return message
    elif hasattr(message, 'content'):
        return message.content
    else:
        return str(message)


def _build_context_info(state: EDWState) -> str:
    """构建上下文信息字符串"""
    context_parts = []
    
    # 基础信息
    if state.get("table_name"):
        context_parts.append(f"**目标表**: {state['table_name']}")
    
    if state.get("type"):
        context_parts.append(f"**任务类型**: {state['type']}")
    
    # 状态信息
    if state.get("status"):
        context_parts.append(f"**当前状态**: {state['status']}")
    
    if state.get("status_message"):
        context_parts.append(f"**状态消息**: {state['status_message']}")
    
    if state.get("error_message"):
        context_parts.append(f"**遇到问题**: {state['error_message']}")
    
    # 业务信息
    if state.get("logic_detail"):
        context_parts.append(f"**需求描述**: {state['logic_detail']}")
    
    if state.get("fields"):
        field_count = len(state['fields'])
        context_parts.append(f"**新增字段数量**: {field_count}个")
    
    if state.get("enhancement_type"):
        context_parts.append(f"**增强类型**: {state['enhancement_type']}")
    
    # 进展信息
    if state.get("validation_status"):
        context_parts.append(f"**验证状态**: {state['validation_status']}")
    
    if state.get("current_refinement_round"):
        context_parts.append(f"**微调轮次**: 第{state['current_refinement_round']}轮")
    
    return "\n".join(context_parts) if context_parts else "无特殊上下文信息"


def _format_conversation_history(messages: List) -> str:
    """格式化对话历史为易读格式"""
    if not messages:
        return "无对话历史"
    
    formatted_messages = []
    for i, message in enumerate(messages, 1):
        content = _extract_message_content(message)
        
        # 确定消息来源
        if isinstance(message, HumanMessage):
            source = "用户"
        elif isinstance(message, AIMessage):
            source = "系统"
        else:
            source = "系统"
        
        # 限制单条消息长度
        if len(content) > 200:
            content = content[:200] + "..."
        
        formatted_messages.append(f"{i}. **{source}**: {content}")
    
    return "\n".join(formatted_messages)


def _generate_summary_with_llm(context_info: str, conversation_history: str) -> str:
    """使用LLM生成总结"""
    try:
        # 获取共享的LLM实例
        llm = get_shared_llm()
        
        # 格式化提示词
        prompt = SUMMARY_REPLY_PROMPT.format(
            context_info=context_info,
            conversation_history=conversation_history
        )
        
        # 使用LLM生成总结
        response = llm.invoke(prompt)
        return response.content if hasattr(response, 'content') else str(response)
        
    except Exception as e:
        logger.error(f"LLM总结生成失败: {e}")
        return f"## 📋 对话总结\n\n生成总结时出现错误: {str(e)}\n\n### 基本信息\n{context_info}"


def create_summary_reply(state: EDWState) -> str:
    """
    创建总结回复的独立方法
    
    Args:
        state: EDW状态对象，包含messages等信息
    
    Returns:
        markdown格式的总结回复
    """
    try:
        # 提取消息历史
        messages = state.get("messages", [])
        
        # 构建上下文信息
        context_info = _build_context_info(state)
        
        # 处理对话历史
        if len(messages) > 8:
            # 消息较多时，使用LangChain summarize处理长对话
            conversation_history = _summarize_long_conversation(messages)
        else:
            # 消息较少时，直接格式化
            conversation_history = _format_conversation_history(messages)
        
        # 使用LLM生成总结
        summary = _generate_summary_with_llm(context_info, conversation_history)
        
        logger.info(f"成功生成对话总结，消息数量: {len(messages)}")
        return summary
        
    except Exception as e:
        logger.error(f"创建总结回复失败: {e}")
        return f"## 📋 对话总结\n\n生成总结时出现错误: {str(e)}"


def _summarize_long_conversation(messages: List) -> str:
    """使用LangChain处理长对话历史"""
    try:
        # 获取共享的LLM实例
        llm = get_shared_llm()
        
        # 将消息转换为文档
        docs = []
        for i, message in enumerate(messages):
            content = _extract_message_content(message)
            source = "用户" if isinstance(message, HumanMessage) else "系统"
            doc_content = f"{source}: {content}"
            docs.append(Document(page_content=doc_content))
        
        # 使用LangChain的summarize chain
        summarize_chain = load_summarize_chain(llm, chain_type="stuff")
        summary = summarize_chain.run(docs)
        
        return f"**对话历史总结** (共{len(messages)}条消息):\n{summary}"
        
    except Exception as e:
        logger.error(f"长对话总结失败: {e}")
        # 回退到直接格式化最近的消息
        recent_messages = messages[-5:] if len(messages) > 5 else messages
        return _format_conversation_history(recent_messages)


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
        logger.info(f"调用mcp 工具 exec sql result: {result}")
        if result and "错误" not in result.lower():
            # 解析字段信息
            fields = []
            lines = result.split('\n')
            for line in lines[1:]:  # 跳过标题行
                if line.strip():
                    # 优先支持CSV格式（逗号分隔），然后是制表符，最后是空格
                    if ',' in line:
                        parts = line.split(',')
                    elif '\t' in line:
                        parts = line.split('\t')
                    else:
                        parts = line.split()

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
        logger.info(f"mcp返回信息: {table_info}")
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
        # 检查是否是因为服务问题导致的失败
        failed_tables = []
        for table_name, fields_list in validation_result["base_tables_info"].items():
            if not fields_list:  # 空列表表示查询失败
                failed_tables.append(table_name)

        if failed_tables:
            # 如果有表查询失败，返回服务错误
            error_msg = f"无法获取底表字段信息，MCP服务可能存在问题。失败的表：{', '.join(failed_tables)}\n\n请检查数据服务状态，稍后再试。"
            logger.error(f"MCP服务问题导致字段验证失败: {failed_tables}")
            return {
                "valid": False,
                "service_error": True,
                "error_message": error_msg,
                "failed_tables": failed_tables,
                "base_tables_info": validation_result["base_tables_info"],
                "cache_performance": validation_result["cache_performance"]
            }
        else:
            # 如果没有底表需要验证，返回成功
            logger.info("没有底表需要验证字段关联性")
            return validation_result

    logger.info(f"所有底表字段: {all_base_fields}")

    # 检查每个新增字段
    for field in fields:
        # 兼容字典和对象访问
        if isinstance(field, dict):
            physical_name = field.get("physical_name", "")
        else:
            physical_name = getattr(field, "physical_name", "")

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


def _validate_english_model_name(name: str) -> tuple[bool, str]:
    """验证英文模型名称格式"""
    if not name or not name.strip():
        return False, "模型名称不能为空"

    name = name.strip()

    # 检查是否包含中文字符
    if any('\u4e00' <= char <= '\u9fff' for char in name):
        return False, f"模型名称不能包含中文字符，当前值: '{name}'"

    # 检查是否符合标准格式（首字母大写，单词间空格分隔）
    words = name.split()
    if not words:
        return False, "模型名称不能为空"

    for word in words:
        if not word[0].isupper() or not word.isalpha():
            return False, f"模型名称应采用标准格式（如：Finance Invoice Header），当前值: '{name}'"

    return True, ""


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

# 代码增强智能体现在通过 EDWAgentManager 统一管理

# langgraph 做法


def navigate_node(state: EDWState):
    """导航节点：负责用户输入的初始分类"""

    # 如果已经有type，直接返回
    if 'type' in state and state['type'] != '' and state['type'] != 'other':
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
            return {"type": "model_dev", "user_id": state.get("user_id", "")}
    except Exception as e:
        error_msg = f"导航节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}


def chat_node(state: EDWState):
    """聊天节点：处理普通对话"""
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

        # 获取响应内容
        response_content = response["messages"][-1].content
        logger.info(f"Chat response: {response_content[:100]}...")

        return {"messages": response["messages"]}
    except Exception as e:
        error_msg = f"聊天节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {"messages": [AIMessage("抱歉，我遇到了一些问题，请稍后再试。")], "error_message": error_msg}

# 主要分配模型增强等相关工作


def edw_model_node(state: EDWState):
    """模型节点：进一步分类模型相关任务"""

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
        return {"type": "error", "user_id": state.get("user_id", ""), "error_message": error_msg}


def search_table_cd(table_name: str) -> dict:
    """
    查询某个表的源代码（支持GitHub和本地搜索切换）
    :param table_name: 必要参数，具体表名比如dwd_fi.fi_invoice_item
    :return: 返回结果字典，包含状态和源代码信息
             成功时: {"status": "success", "code": "...", "table_name": "...", ...}
             失败时: {"status": "error", "message": "错误信息"}
    """
    # 通过环境变量控制使用哪种搜索方式
    use_github = os.getenv("USE_GITHUB_SEARCH", "true").lower() == "true"
    
    if use_github:
        try:
            # 使用GitHub工具进行搜索
            github_tool = GitHubTool()
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
    return {"status": "error", "message": f"暂不支持的代码文件格式: {file.name}, 仅支持 .sql 和 .py 文件。请检查表名或代码文件格式。"}


# 模型增强前针对数据进行校验验证
# 注意：此函数已被重构为子图架构，见 validation_nodes.py
async def edw_model_enhance_data_validation_node_old(state: EDWState):
    """模型增强数据验证节点：验证用户输入信息的完整性"""

    try:
        config = SessionManager.get_config(state.get("user_id", ""), "validation")

        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)


        # 构建消息列表，检查是否有之前的错误信息
        if state.get("error_message") and state.get("validation_status") == "incomplete_info":
            # 有之前的错误信息，构建对话历史
            logger.info("检测到之前的验证错误，构建对话历史")
            messages = [
                AIMessage(content=state["error_message"]),  # AI的错误提示
                HumanMessage(content=content)  # 用户的新输入
            ]
        else:
            # 首次验证
            messages = [HumanMessage(content=content)]

        # 使用验证代理提取关键信息
        response = valid_agent.invoke(
            {"messages": messages},
            config
        )

        # 获取LLM响应
        validation_result = response["messages"][-1].content
        logger.info(f"LLM原始响应: {validation_result}")

        # 使用LangChain输出解析器优雅地解析响应
        try:
            # 使用PydanticOutputParser解析LLM响应
            parsed_request = parser.parse(validation_result)

            # 验证英文模型名称格式
            if parsed_request.model_attribute_name:
                is_valid_name, name_error = _validate_english_model_name(parsed_request.model_attribute_name)
                if not is_valid_name:
                    error_msg = f"模型名称格式不正确：{name_error}\n\n请使用标准的英文格式，例如：\n- Finance Invoice Header\n- Customer Order Detail\n- Inventory Management System"
                    writer({"error": error_msg})
                    writer({"content": error_msg})

                    # 不再需要调用 valid_agent.invoke()，因为错误信息已保存到状态中

                    return {
                        "validation_status": "incomplete_info",
                        "error_message": error_msg,
                        "table_name": parsed_request.table_name if parsed_request.table_name else "",
                        "user_id": state.get("user_id", ""),
                        "messages": [HumanMessage(error_msg)]
                    }

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


                # 不再需要调用 valid_agent.invoke()，因为错误信息已保存到状态中

                # 返回特殊的validation_status标记，表示信息不完整需要直接结束
                return {
                    "validation_status": "incomplete_info",  # 特殊标记
                    "missing_info": missing_fields,
                    "error_message": complete_message,
                    "table_name": parsed_request.table_name if "table_name" not in missing_fields else "",
                    "user_id": state.get("user_id", ""),
                    "messages": [HumanMessage(complete_message)]  # 添加消息以便用户看到
                }

            table_name = parsed_request.table_name.strip()
            logic_detail = parsed_request.logic_detail.strip()


            # 调用search_table_cd查询表的源代码
            try:
                code_info = search_table_cd(table_name)
                logger.info(f"表代码查询结果: {str(code_info)[:200] if code_info else 'None'}...")

                if code_info.get("status") == "error":
                    error_msg = f"未找到表 {table_name} 的源代码: {code_info.get('message', '未知错误')}\n请确认表名是否正确。"
                    writer({"error": error_msg})
                    writer({"content": error_msg})

                    # 不再需要调用 valid_agent.invoke()，因为错误信息已保存到状态中

                    return {
                        "validation_status": "incomplete_info",  # 标记为信息不完整
                        "error_message": error_msg,
                        "table_name": table_name,
                        "user_id": state.get("user_id", ""),
                        "messages": [HumanMessage(error_msg)]
                    }


                # 转换为ADB路径
                code_path = code_info.get("file_path", "")
                adb_path = convert_to_adb_path(code_path)

                # 提取源代码中的底表
                source_code = code_info.get("code", "")
                base_tables = extract_tables_from_code(source_code)
                logger.info(f"从源代码中提取到底表: {base_tables}")

                # 验证字段与底表的关联性
                if base_tables and parsed_request.fields:

                    field_validation = await validate_fields_against_base_tables(
                        parsed_request.fields,
                        base_tables,
                        source_code
                    )

                    if not field_validation["valid"]:
                        # 检查是否是MCP服务问题
                        if field_validation.get("service_error"):
                            # MCP服务问题
                            validation_error_msg = field_validation["error_message"]
                        else:
                            # 字段验证失败
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


                        # 不再需要调用 valid_agent.invoke()，因为错误信息已保存到状态中

                        return {
                            "validation_status": "incomplete_info",
                            "error_message": validation_error_msg,
                            "field_validation": field_validation,
                            "table_name": table_name,
                            "user_id": state.get("user_id", ""),
                            "messages": [HumanMessage(validation_error_msg)]
                        }
                    else:

                        # 添加缓存性能信息到成功验证的情况
                        if "cache_performance" in field_validation:
                            cache_perf = field_validation["cache_performance"]

                        if field_validation["suggestions"]:
                            suggestions_msg = "字段建议：\\n"
                            for field_name, suggestions in field_validation["suggestions"].items():
                                suggestions_msg += f"- {field_name}: 发现相似字段 {suggestions[0]['field_name']} (相似度: {suggestions[0]['similarity']:.2f})\\n"
                else:
                    logger.info("未找到底表或新增字段为空，跳过字段验证")

                # 将所有信息存储到state中
                return {
                    "type": "model_enhance",  # 保持原始类型以供路由函数识别
                    "user_id": state.get("user_id", ""),
                    "validation_status": "completed",  # 重置验证状态为完成
                    # 存储解析的需求信息（直接使用Pydantic对象属性）
                    "table_name": table_name,
                    "logic_detail": logic_detail,
                    "enhancement_type": parsed_request.enhancement_type,
                    "model_attribute_name": parsed_request.model_attribute_name,  # 用户输入的英文模型名称
                    "business_purpose": parsed_request.business_purpose,  # 业务用途描述
                    "field_info": parsed_request.field_info,
                    "business_requirement": parsed_request.business_requirement,
                    # 新增字段列表（存储为字典列表）
                    "fields": [field.model_dump() for field in parsed_request.fields] if parsed_request.fields else [],
                    # 存储表代码信息
                    "source_code": code_info.get("code", ""),
                    "code_path": code_path,
                    "adb_code_path": adb_path,  # 新增ADB路径
                    "base_tables": base_tables,  # 保存底表信息供后续使用
                    "collected_info": {
                        "validation_result": validation_result,
                        "parsed_requirements": parsed_request.model_dump(),
                        "table_code_info": code_info,
                        "adb_path": adb_path,
                        "base_tables": base_tables,  # 也在collected_info中保存一份
                        "timestamp": datetime.now().isoformat()
                    },
                    "session_state": "validation_completed"
                }

            except Exception as code_error:
                error_msg = f"查询表代码失败: {str(code_error)}"
                logger.error(error_msg)
                return {
                    "validation_status": "incomplete_info",  # 确保用户重试时能获得错误上下文
                    "error_message": error_msg,
                    "table_name": table_name,
                    "user_id": state.get("user_id", ""),
                    "messages": [HumanMessage(error_msg)]
                }

        except Exception as parse_error:
            # LangChain的parser可能抛出多种异常，统一处理
            error_msg = "信息格式解析失败。请使用更清晰的格式描述需求，确保包含：\n1. 表名（如：dwd_fi.fi_invoice_item）\n2. 具体的增强逻辑"
            logger.error(f"解析错误: {str(parse_error)}. 原始响应: {validation_result}")

            # 不再需要调用 valid_agent.invoke()，因为错误信息已保存到状态中

            return {
                "validation_status": "incomplete_info",  # 标记为信息不完整
                "error_message": error_msg,
                "user_id": state.get("user_id", ""),
                "messages": [HumanMessage(error_msg)]
            }

    except Exception as e:
        error_msg = f"数据验证失败: {str(e)}"
        logger.error(error_msg)
        return {
            "validation_status": "incomplete_info",  # 确保用户重试时能获得错误上下文
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "messages": [HumanMessage(error_msg)]
        }

# 新增模型前主要针对数据进行校验验证


def edw_model_add_data_validation_node(state: EDWState):
    """模型新增数据验证节点"""
    return {}


# 主要进行模型增强等相关工作
async def _execute_code_enhancement_task(enhancement_mode: str, **kwargs) -> dict:
    """统一的代码增强执行引擎 - 支持不同模式的提示词"""
    try:
        # 根据模式选择不同的提示词构建策略
        if enhancement_mode == "initial_enhancement":
            task_message = _build_initial_enhancement_prompt(**kwargs)
        elif enhancement_mode == "refinement":
            task_message = _build_refinement_prompt(**kwargs)
        else:
            raise ValueError(f"不支持的增强模式: {enhancement_mode}")

        # 从智能体管理器获取代码增强智能体
        from src.agent.edw_agents import get_code_enhancement_agent, get_code_enhancement_tools
        enhancement_agent = get_code_enhancement_agent()
        tools = get_code_enhancement_tools()

        # 使用配置管理器获取配置 - 为每个用户生成独立的thread_id
        table_name = kwargs.get("table_name", "unknown")
        user_id = kwargs.get("user_id", "")
        config = SessionManager.get_config(user_id, f"enhancement_{table_name}")

        # 调用全局智能体执行增强任务（异步调用以支持MCP工具）
        result = await enhancement_agent.ainvoke(
            {"messages": [HumanMessage(task_message)]},
            config
        )

        # 解析智能体的响应
        response_content = result["messages"][-1].content
        enhancement_result = _parse_agent_response(response_content)

        if enhancement_result.get("enhanced_code"):
            logger.info(f"代码增强成功 ({enhancement_mode}): {table_name}")
            return {
                "success": True,
                "enhanced_code": enhancement_result.get("enhanced_code"),
                "new_table_ddl": enhancement_result.get("new_table_ddl"),
                "alter_statements": enhancement_result.get("alter_statements"),
                "table_comment": enhancement_result.get("table_comment"),
                "optimization_summary": enhancement_result.get("optimization_summary", ""),
                "field_mappings": kwargs.get("fields", [])
            }
        else:
            error_msg = f"智能体未能生成有效的增强代码 ({enhancement_mode})"
            logger.error(error_msg)
            return {
                "success": False,
                "error": error_msg
            }

    except Exception as e:
        error_msg = f"执行代码增强时发生异常 ({enhancement_mode}): {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    finally:
        # MCP客户端使用上下文管理器，无需手动清理
        logger.debug(f"代码增强任务完成 ({enhancement_mode})")


def _build_initial_enhancement_prompt(table_name: str, source_code: str, adb_code_path: str,
                                     fields: list, logic_detail: str, code_path: str = "", **kwargs) -> str:
    """构建初始模型增强的提示词 - 完整流程"""
    
    # 判断代码类型
    file_path = code_path or adb_code_path or ""
    if file_path.endswith('.sql'):
        code_language = "sql"
        code_type_desc = "SQL"
    else:
        code_language = "python"
        code_type_desc = "Python"

    # 构造字段信息字符串
    fields_info = []
    for field in fields:
        if isinstance(field, dict):
            physical_name = field['physical_name']
            attribute_name = field['attribute_name']
        else:
            physical_name = field.physical_name
            attribute_name = field.attribute_name
        fields_info.append(f"{physical_name} ({attribute_name})")

    return f"""你是一个Databricks代码增强专家，负责为数据模型添加新字段。

**任务目标**: 为表 {table_name} 创建增强版本的{code_type_desc}代码

**增强需求**: {logic_detail}

**新增字段**:
{chr(10).join(fields_info)}

**原始源代码**:
```{code_language.lower()}
{source_code}
```

**执行步骤**:
1.  使用execute_sql工具查询目标表结构: `DESCRIBE {table_name}`
2. 分析源代码中的底表，查询底表结构来推断新字段的数据类型
3. 基于原始代码生成增强版本，确保新字段逻辑正确
4. 生成完整的CREATE TABLE和ALTER TABLE语句

**输出要求**: 严格按JSON格式返回
{{
    "enhanced_code": "完整的增强后{code_type_desc}代码",
    "new_table_ddl": "包含新字段的CREATE TABLE语句", 
    "alter_statements": "ADD COLUMN的ALTER语句"
}}"""


def _build_refinement_prompt(current_code: str, user_feedback: str, table_name: str,
                           original_context: dict, **kwargs) -> str:
    """构建代码微调的提示词 - 针对性优化"""
    
    return f"""你是一个代码优化专家，负责根据用户反馈修改AI生成的代码。
**用户反馈**: "{user_feedback}"

**优化指导原则**:
1. 重点关注用户的具体反馈，精准响应用户需求
2. 如需查询额外信息，可使用工具
3. 优化可能包括：性能改进、代码可读性、异常处理、注释补充等、属性名称修改、字段顺序修改

**注意事项**:
- 不要重新设计整体架构，只做针对性改进
- 保持与原代码的语言风格一致
- 确保修改后的代码逻辑正确且可执行
- ALTER语句如果有需要请重新生成，需满足alter table ** add column ** comment '' after '';

**输出格式**: 严格按JSON格式返回
{{
    "enhanced_code": "优化后的代码",
    "new_table_ddl": "CREATE TABLE语句（如有需要）",
    "alter_statements": "ALTER语句（如有需要）",
    "optimization_summary": "本次优化的具体改进点说明"
}}"""


def _format_fields_info(fields: list) -> str:
    """格式化字段信息为字符串"""
    if not fields:
        return "无字段信息"
    
    fields_info = []
    for field in fields:
        if isinstance(field, dict):
            name = field.get('physical_name', '')
            attr = field.get('attribute_name', '')
        else:
            name = getattr(field, 'physical_name', '')
            attr = getattr(field, 'attribute_name', '')
        
        if name and attr:
            fields_info.append(f"{name} ({attr})")
        elif name:
            fields_info.append(name)
    
    return ', '.join(fields_info) if fields_info else "无字段信息"


def _parse_agent_response(content: str) -> dict:
    """解析智能体响应，提取JSON结果"""
    import json
    import re

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


def edw_model_enhance_node(state: EDWState):
    """模型增强处理节点"""

    try:
        # 提取状态中的信息
        table_name = state.get("table_name")
        source_code = state.get("source_code")
        adb_code_path = state.get("adb_code_path")
        code_path = state.get("code_path")
        fields = state.get("fields", [])
        logic_detail = state.get("logic_detail")
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")


        # 验证必要信息
        if not table_name or not source_code:
            error_msg = "缺少必要信息：表名或源代码为空"
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        if not fields:
            error_msg = "没有找到新增字段信息"
            return {
                "error_message": error_msg,
                "user_id": user_id
            }


        # 异步执行代码增强 - 使用重构后的通用函数
        enhancement_result = asyncio.run(_execute_code_enhancement_task(
            enhancement_mode="initial_enhancement",
            table_name=table_name,
            source_code=source_code,
            adb_code_path=adb_code_path,
            fields=fields,
            logic_detail=logic_detail,
            code_path=code_path,
            user_id=user_id
        ))

        if enhancement_result.get("success"):

            # 直接使用从数据校验节点传递过来的模型名称
            model_name = state.get("model_attribute_name", "")
            logger.info(f"使用数据校验节点提取的模型名称: {model_name}")

            # 格式化增强结果为用户友好的消息
            formatted_message = f"""## 🎉 代码增强完成

**目标表**: {table_name}
**新增字段**: {len(fields)} 个
**增强类型**: {enhancement_type}
**模型名称**: {model_name or '未指定'}

### ✅ 生成的内容
- 增强代码已生成
- CREATE TABLE 语句已生成
- ALTER TABLE 语句已生成

### 📊 详细结果
```json
{json.dumps(enhancement_result, ensure_ascii=False, indent=2)}
```

### 📋 新增字段列表
"""
            # 添加字段详情
            for field in fields:
                if isinstance(field, dict):
                    physical_name = field.get('physical_name', '')
                    attribute_name = field.get('attribute_name', '')
                else:
                    physical_name = getattr(field, 'physical_name', '')
                    attribute_name = getattr(field, 'attribute_name', '')
                formatted_message += f"- {physical_name} ({attribute_name})\n"

            return {
                "messages": [AIMessage(content=formatted_message)],  # 添加 AI 消息到状态
                "user_id": user_id,
                "enhance_code": enhancement_result.get("enhanced_code"),
                "create_table_sql": enhancement_result.get("new_table_ddl"),
                "alter_table_sql": enhancement_result.get("alter_statements"),
                "model_name": model_name,  # 使用数据校验节点提取的模型名称
                "field_mappings": enhancement_result.get("field_mappings"),
                "enhancement_type": enhancement_type,  # 保留增强类型供路由使用
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
            logger.error(f"代码增强失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "enhancement_type": enhancement_type  # 保留增强类型
            }

    except Exception as e:
        error_msg = f"模型增强节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "enhancement_type": state.get("enhancement_type", "")  # 保留增强类型
        }

# 主要进行新增模型等相关工作


def edw_model_addition_node(state: EDWState):
    """模型新增处理节点"""
    return {}


# 微调相关节点
def refinement_inquiry_node(state: EDWState):
    """微调询问节点 - 展示代码并询问用户想法"""
    
    enhanced_code = state.get("enhance_code", "")
    table_name = state.get("table_name", "")
    user_id = state.get("user_id", "")
    
    # 构建友好的展示消息
    display_message = f"""🎉 **代码增强完成！**
请问您对这段代码有什么想法？您可以：
- 说"看起来不错"或"可以了"表示满意
- 提出具体的修改建议，如"能优化一下性能吗"
- 或说其他任何想法
"""
    
    from langgraph.types import interrupt
    
    # 使用interrupt等待用户输入
    user_response = interrupt({
        "prompt": display_message,
        "action_type": "refinement_conversation"
    })
    
    return {
        "user_refinement_input": user_response,
        "refinement_conversation_started": True,
        "original_enhanced_code": enhanced_code,  # 备份原始代码
        "current_refinement_round": 1,
        "user_id": user_id
    }


def refinement_intent_node(state: EDWState):
    """基于大语言模型的用户意图深度识别节点"""
    
    user_input = state.get("user_refinement_input", "")
    user_id = state.get("user_id", "")
    messages = state.get("messages", [])
    
    # 获取消息总结器和配置
    from src.graph.message_summarizer import get_message_summarizer
    from src.config import get_config_manager
    
    config_manager = get_config_manager()
    message_config = config_manager.get_message_config()
    
    # 使用消息总结器处理消息历史
    summarizer = get_message_summarizer()
    try:
        # 先进行消息总结（如果需要）
        summarized_messages = summarizer.summarize_if_needed(messages)
    except Exception as e:
        logger.warning(f"消息总结失败，使用原始消息: {e}")
    # 使用 LangChain 的 PydanticOutputParser
    from langchain.output_parsers import PydanticOutputParser
    from src.models.edw_models import RefinementIntentAnalysis
    
    parser = PydanticOutputParser(pydantic_object=RefinementIntentAnalysis)
    
    # 🎯 使用动态上下文的意图分析提示词
    intent_analysis_prompt = f"""你是一个专业的用户意图分析专家，需要结合聊天历史的上下文深度理解用户对代码增强结果的真实想法和需求。

**用户刚刚说**: "{user_input}"

**任务**: 请深度分析用户的真实意图，考虑语义、情感、上下文等多个维度。

**意图分类标准**:

1. **REFINEMENT_NEEDED** - 用户希望对代码进行调整/改进
   识别场景：
   - 明确提出修改建议（如"能不能优化一下"、"这里逻辑有问题"）
   - 表达不满意或疑虑（如"感觉性能不够好"、"这样写对吗"）
   - 提出新的要求（如"能加个异常处理吗"、"可以添加注释吗"）
   - 询问是否可以改进（如"还能更好吗"、"有没有别的写法"）

2. **SATISFIED_CONTINUE** - 用户对结果满意，希望继续后续流程
   识别场景：
   - 表达满意（如"不错"、"可以"、"很好"、"满意"）
   - 确认继续（如"继续吧"、"可以进行下一步"、"没问题"）
   - 赞同认可（如"就这样"、"挺好的"、"符合预期"）

3. **UNRELATED_TOPIC** - 用户说的内容与当前代码增强任务无关
   识别场景：
   - 日常闲聊（如"今天天气如何"、"你好"）
   - 询问其他技术问题（如"Python怎么学"）
   - 完全无关的话题

**分析要求**:
- 重点理解用户的**真实情感倾向**和**实际需求**
- 考虑**语境和上下文**，不要只看字面意思
- 对于模糊或间接的表达，要推断其深层含义
- 如果用户表达含糊，倾向于理解为需要进一步沟通

{parser.get_format_instructions()}
"""

    try:
        # 使用专门的意图分析代理（无记忆）
        from src.agent.edw_agents import create_intent_analysis_agent
        
        intent_agent = create_intent_analysis_agent()
        
        response = intent_agent.invoke(
            {"messages": summarized_messages + [HumanMessage(intent_analysis_prompt)]}
        )
        
        # 使用 LangChain parser 解析响应
        analysis_content = response["messages"][-1].content
        intent_result = parser.parse(analysis_content)
        
        logger.info(f"LLM意图分析结果: {intent_result}")
        
        result = {
            "user_intent": intent_result.intent,
            "intent_confidence": intent_result.confidence_score,
            "intent_reasoning": intent_result.reasoning,
            "refinement_requirements": intent_result.extracted_requirements,
            "user_emotion": intent_result.user_emotion,
            "suggested_response": intent_result.suggested_response,
            "user_id": user_id
        }
        
        # 准备要添加的消息列表
        messages_to_add = []
        
        # 添加用户的最新输入
        if user_input:
            messages_to_add.append(HumanMessage(content=user_input))
        
        # 将意图分析结果格式化为用户友好的消息
        intent_summary = f"📊 意图分析完成：{intent_result.intent} (置信度: {intent_result.confidence_score})"
        # 如果消息被总结了，使用总结后的消息作为基础
        if len(summarized_messages) != len(messages):
            result["messages"] = summarized_messages + messages_to_add
            logger.info(f"消息已总结：{len(messages)} -> {len(summarized_messages)} 条，添加用户输入")
        else:
            # 否则只添加用户输入
            result["messages"] = messages_to_add
        
        return result
        
    except Exception as e:
        # 解析失败时的优雅降级
        logger.error(f"意图识别解析失败: {e}")
        result = {
            "user_intent": "SATISFIED_CONTINUE",  # 默认继续
            "intent_confidence": 0.5,
            "intent_reasoning": f"解析失败，使用默认判断: {str(e)}",
            "refinement_requirements": "",
            "user_emotion": "neutral",
            "suggested_response": "",
            "user_id": user_id
        }
        
        # 即使解析失败，也要处理消息总结和用户输入
        try:
            summarized_messages = summarizer.summarize_if_needed(messages)
            
            # 准备要添加的消息列表
            messages_to_add = []
            
            # 添加用户的最新输入
            if user_input:
                messages_to_add.append(HumanMessage(content=user_input))
            
            # 如果消息被总结了，使用总结后的消息作为基础
            if len(summarized_messages) != len(messages):
                result["messages"] = summarized_messages + messages_to_add
                logger.info(f"消息已总结（异常处理）：{len(messages)} -> {len(summarized_messages)} 条，添加用户输入")
            else:
                # 否则只添加用户输入
                result["messages"] = messages_to_add
                
        except Exception as summary_error:
            logger.warning(f"异常处理中的消息总结也失败: {summary_error}")
            # 至少保存用户输入
            if user_input:
                result["messages"] = [HumanMessage(content=user_input)]
        
        return result


def code_refinement_node(state: EDWState):
    """代码微调执行节点 - 复用增强引擎"""
    
    # 获取微调需求
    refinement_requirements = state.get("refinement_requirements", "")
    current_code = state.get("enhance_code", "")
    table_name = state.get("table_name", "")
    user_id = state.get("user_id", "")
    
    # 构建原始上下文信息
    original_context = {
        "logic_detail": state.get("logic_detail", ""),
        "fields_info": _format_fields_info(state.get("fields", []))
    }
    
    try:
        # 使用微调模式的增强引擎
        refinement_result = asyncio.run(_execute_code_enhancement_task(
            enhancement_mode="refinement",
            current_code=current_code,
            user_feedback=refinement_requirements,
            table_name=table_name,
            original_context=original_context,
            user_id=user_id
        ))
        
        if refinement_result.get("success"):
            # 更新微调轮次
            current_round = state.get("current_refinement_round", 1)
            
            # 记录微调历史
            refinement_history = state.get("refinement_history", [])
            refinement_history.append({
                "round": current_round,
                "user_feedback": refinement_requirements,
                "old_code": current_code[:200] + "...",
                "optimization_summary": refinement_result.get("optimization_summary", ""),
                "timestamp": datetime.now().isoformat()
            })
            
            return {
                "enhance_code": refinement_result["enhanced_code"],  # 更新代码
                "create_table_sql": refinement_result.get("new_table_ddl", state.get("create_table_sql")),
                "alter_table_sql": refinement_result.get("alter_statements", state.get("alter_table_sql")),
                "refinement_completed": True,
                "current_refinement_round": current_round + 1,
                "refinement_history": refinement_history,
                "optimization_summary": refinement_result.get("optimization_summary", ""),
                "user_id": user_id
            }
        else:
            # 微调失败，使用原代码
            error_msg = refinement_result.get("error", "微调失败")
            logger.error(f"代码微调失败: {error_msg}")
            
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": f"代码微调失败: {error_msg}",
                "status_details": {"refinement_result": refinement_result},
                "error_message": error_msg  # 向后兼容
            }
            
    except Exception as e:
        error_msg = f"微调节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "user_id": user_id,
            "status": "error",
            "status_message": error_msg,
            "status_details": {"exception": str(e)},
            "error_message": error_msg  # 向后兼容
        }




def github_push_node(state: EDWState):
    """将AI修改的代码推送到GitHub远程仓库"""
    logger.info("模拟更新github 成功")
    return {}
    try:
        # 从状态中获取必要信息
        enhanced_code = state.get("enhance_code", "")  # 增强后的代码
        code_path = state.get("code_path", "")  # 原始代码路径
        table_name = state.get("table_name", "")
        user_id = state.get("user_id", "")
        
        # 验证必要信息
        if not enhanced_code:
            error_msg = "缺少增强后的代码，无法推送到GitHub"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        if not code_path:
            error_msg = "缺少代码文件路径，无法推送到GitHub"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        logger.info(f"准备将增强后的代码推送到GitHub: {code_path}")
        
        # 初始化GitHub工具
        try:
            github_tool = GitHubTool()
        except Exception as e:
            error_msg = f"初始化GitHub工具失败: {str(e)}"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e)},
                "error_message": error_msg  # 向后兼容
            }
        
        # 推送代码到GitHub
        try:
            # 使用固定的提交信息 "AI Code"
            commit_message = "AI Code"
            
            # 调用GitHub工具的commit_file方法
            result = github_tool.commit_file(
                file_path=code_path,
                content=enhanced_code,
                message=commit_message
            )
            
            # 检查推送结果
            if result.get("status") == "success":
                success_msg = f"成功推送代码到GitHub: {table_name}"
                logger.info(success_msg)
                
                return {
                    "user_id": user_id,
                    "status": "success",
                    "status_message": success_msg,
                    "status_details": {
                        "commit_sha": result.get("commit", {}).get("sha", ""),
                        "commit_url": result.get("commit", {}).get("url", ""),
                        "file_url": result.get("file", {}).get("url", ""),
                        "table_name": table_name,
                        "code_path": code_path
                    },
                    # 保留这些字段供后续节点使用
                    "github_commit_sha": result.get("commit", {}).get("sha", ""),
                    "github_commit_url": result.get("commit", {}).get("url", ""),
                    "github_file_url": result.get("file", {}).get("url", "")
                }
            elif result.get("status") == "no_change":
                info_msg = "代码内容未发生变化，无需推送"
                logger.info(info_msg)
                return {
                    "user_id": user_id,
                    "status": "no_change",
                    "status_message": info_msg
                }
            else:
                error_msg = result.get("message", "GitHub推送失败")
                logger.error(f"GitHub推送失败: {error_msg}")
                return {
                    "user_id": user_id,
                    "status": "error",
                    "status_message": f"推送失败: {error_msg}",
                    "status_details": {"result": result},
                    "error_message": error_msg  # 向后兼容
                }
                
        except Exception as e:
            error_msg = f"推送到GitHub时发生异常: {str(e)}"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e), "code_path": code_path},
                "error_message": error_msg  # 向后兼容
            }
            
    except Exception as e:
        error_msg = f"GitHub推送节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "user_id": state.get("user_id", ""),
            "status": "error",
            "status_message": error_msg,
            "status_details": {"exception": str(e)},
            "error_message": error_msg  # 向后兼容
        }


# EDW邮件HTML模板常量
EDW_EMAIL_HTML_TEMPLATE = """
<!DOCTYPE html>
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

            <div class="greeting">{greeting}</div>

            <div class="model-name">
                请帮忙review {model_full_name} 模型增强
            </div>

            <div class="fields-section">
                <div class="fields-title">新增字段如下：</div>
                <table class="fields-table">
                    {fields_html}
                </table>
            </div>

            <div class="review-log-title">Review log:</div>
            {review_link_html}
        </div>

        <div class="footer">
            <p style="margin: 0; color: #4a90e2; font-weight: 600;">🤖 This email was automatically generated by EDW Intelligent Assistant</p>
            <p style="margin: 5px 0 0 0; color: #4a90e2; font-size: 13px;">
                AI Generated Content | Generated at {current_time}
            </p>
        </div>
    </div>
</body>
</html>
"""

# 问候语映射常量
EDW_EMAIL_GREETING_MAP = {
    "dwd_fi": "Hello Finance Reviewers,",
    "cam_fi": "Hello Finance Reviewers,",
    "dwd_hr": "Hello HR Reviewers,",
    "cam_hr": "Hello HR Reviewers,",
    "default": "Hello SAB Reviewers,"
}


def _build_html_email_template(table_name: str, model_name: str, schema: str,
                               fields: list, confluence_page_url: str, confluence_title: str) -> str:
    """构建友好的HTML邮件模板"""

    # 确定问候语
    greeting = EDW_EMAIL_GREETING_MAP.get(schema.lower(), EDW_EMAIL_GREETING_MAP["default"])

    # 构建模型全名 - 优先使用模型名称
    if model_name:
        # 如果有模型名称，使用模型名称
        model_full_name = f"{schema}.{model_name}"
    else:
        # 如果没有模型名称，从表名提取
        table_suffix = table_name.split('.')[-1] if '.' in table_name else table_name
        # 将下划线转换为空格，并首字母大写
        formatted_name = table_suffix.replace('_', ' ').title()
        model_full_name = f"{schema}.{formatted_name}"

    # 构建字段列表HTML
    fields_html = ""
    if fields:
        for field in fields:
            # 兼容字典和对象访问
            if isinstance(field, dict):
                physical_name = field.get('physical_name', '未知字段')
                attribute_name = field.get('attribute_name', field.get('physical_name', ''))
            else:
                physical_name = getattr(field, 'physical_name', '未知字段')
                attribute_name = getattr(field, 'attribute_name', getattr(field, 'physical_name', ''))
            fields_html += f"""
                <tr>
                    <td style="padding: 8px 12px; border-left: 3px solid #0078d4; background-color: #f8f9fa;">
                        <span style="font-weight: 600; color: #323130;">{physical_name}</span>
                        <span style="color: #605e5c; margin-left: 8px;">({attribute_name})</span>
                    </td>
                </tr>"""
    else:
        fields_html = '<tr><td style="padding: 8px 12px; color: #605e5c;">暂无新增字段信息</td></tr>'

    # 构建Review链接HTML
    review_link_html = ""
    if confluence_page_url:
        review_link_html = f"""
            <div style="margin: 25px 0;">
                <a href="{confluence_page_url}"
                   style="background: linear-gradient(135deg, #0078d4, #106ebe);
                          color: white;
                          padding: 12px 24px;
                          text-decoration: none;
                          border-radius: 6px;
                          display: inline-block;
                          font-weight: 600;
                          box-shadow: 0 2px 8px rgba(0,120,212,0.3);
                          transition: all 0.3s ease;">
                    📋 Review Log
                </a>
            </div>
            <p style="color: #605e5c; font-size: 14px; margin: 10px 0;">
                Review log: <a href="{confluence_page_url}" style="color: #0078d4;">{confluence_page_url}</a>
            </p>"""
    else:
        review_link_html = '<p style="color: #d13438;">⚠️ Review链接暂不可用，请联系技术支持。</p>'

    # 使用模板常量格式化HTML
    html_content = EDW_EMAIL_HTML_TEMPLATE.format(
        greeting=greeting,
        model_full_name=model_full_name,
        fields_html=fields_html,
        review_link_html=review_link_html,
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )

    return html_content


def _send_email_via_metis(html_content: str, model_name: str, table_name: str) -> dict:
    """使用metis系统发送邮件"""
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
            "AlertName": f"🤖 Model Review Request - {model_name or table_name} [AI Generated]",
            "AlertDescription": html_content,
            "Priority": "P3",
            "Assignee": "reviewers"
        }
        logger.info(f"邮件推送html: {html_content}")
        # 创建邮件参数对象
        email_param_obj = EmailParam(email_params)

        # 创建邮件发送对象
        email_sender = Email(email_param_obj.get_param(), settings.EMAIL_TOKEN)

        # 发送邮件
        result = email_sender.send()

        return {
            "success": True,
            "result": result,
            "email_params": email_params
        }

    except ImportError as e:
        return {
            "success": False,
            "error": f"导入邮件模块失败: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"邮件发送失败: {str(e)}"
        }


# 负责发送邮件
def edw_email_node(state: EDWState):
    """优化的友好邮件发送节点"""

    try:
        # 从state中获取相关信息
        table_name = state.get("table_name", "未知表")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        confluence_page_url = state.get("confluence_page_url", "")
        confluence_title = state.get("confluence_title", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")

        # 解析schema信息
        schema = "default"
        if '.' in table_name:
            schema = table_name.split('.')[0]


        # 构建HTML邮件内容
        html_content = _build_html_email_template(
            table_name=table_name,
            model_name=model_name,
            schema=schema,
            fields=fields,
            confluence_page_url=confluence_page_url,
            confluence_title=confluence_title
        )


        # 发送邮件
        send_result = _send_email_via_metis(html_content, model_name, table_name)

        if send_result.get("success"):
            logger.info("邮件发送成功")

            return {
                "user_id": user_id,
                "email_sent": True,
                "email_format": "HTML",
                "email_subject": f"🤖 Model Review Request - {model_name or table_name} [AI Generated]",
                "confluence_link_included": bool(confluence_page_url),
                "confluence_page_url": confluence_page_url,
                "send_result": send_result.get("result", ""),
                "email_params": send_result.get("email_params", {}),
                "session_state": "email_completed"
            }
        else:
            error_msg = send_result.get("error", "未知错误")
            logger.error(f"邮件发送失败: {error_msg}")

            return {
                "error_message": f"邮件发送失败: {error_msg}",
                "user_id": user_id,
                "email_sent": False,
                "html_content": html_content,  # 保留HTML内容供调试
                "confluence_page_url": confluence_page_url
            }

    except Exception as e:
        error_msg = f"邮件节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "email_sent": False
        }

# 负责更新confluence page


async def _create_confluence_documentation(table_name: str, model_name: str,
                                           enhanced_code: str, fields: list,
                                           alter_table_sql: str, user_id: str,
                                           enhancement_type: str = "add_field", base_tables: list = None) -> dict:
    """异步创建Confluence文档的核心函数"""
    try:
        from src.basic.confluence.confluence_tools import ConfluenceWorkflowTools


        # 解析表名获取schema信息
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'default'
            table = table_name

        # 构建用于Confluence的上下文
        context = {
            "table_name": table_name,
            "enhanced_code": enhanced_code,
            "explanation": f"为表 {table_name} 增加了 {len(fields)} 个新字段",
            "improvements": [f"增加字段: {field.get('physical_name', '') if isinstance(field, dict) else getattr(field, 'physical_name', '')}" for field in fields],
            "alter_sql": alter_table_sql
        }


        # 创建Confluence工具实例
        tools = ConfluenceWorkflowTools()

        # 收集文档信息
        doc_info = await tools.collect_model_documentation_info(context)

        if "error" in doc_info:
            return {
                "success": False,
                "error": f"收集文档信息失败: {doc_info['error']}"
            }

        # 根据用户要求直接构建model_config

        # 获取相关人员信息
        stakeholders = tools._get_model_stakeholders(schema)
        current_date = datetime.now().strftime('%Y-%m-%d')

        # 根据enhancement_type确定操作类型
        operation_type = "Enhance" if enhancement_type in ["add_field", "modify_logic", "optimize_query"] else "New"

        # 根据schema确定业务域
        domain_map = {
            "dwd_fi": "Finance",
            "cam_fi": "Finance",
            "dwd_hr": "HR",
            "cam_hr": "HR"
        }
        domain = domain_map.get(schema.lower(), "Data")

        # 构建自定义的model_config（按用户要求的格式）
        final_model_name = model_name or table.replace('_', ' ').title()

        # 构建标题，避免特殊字符和长度问题
        base_title = f"{current_date}:Finance Data Model Review - {final_model_name} {operation_type}"
        ai_suffix = " [AI Generated]"

        # 确保标题不超过Confluence限制（通常是255字符，保留一些余量）
        max_length = 200
        if len(base_title) + len(ai_suffix) > max_length:
            # 截断model_name部分
            available_for_name = max_length - len(f"{current_date}:Finance Data Model Review -  {operation_type}") - len(ai_suffix)
            if available_for_name > 10:
                final_model_name = final_model_name[:available_for_name - 3] + "..."
                base_title = f"{current_date}:Finance Data Model Review - {final_model_name} {operation_type}"

        final_title = base_title + ai_suffix
        logger.info(f"创建Confluence页面标题: {final_title} (长度: {len(final_title)})")

        custom_model_config = {
            "title": final_title,
            "requirement_description": f"AI Agent 自动为 {table_name} 增强了 {len(fields)} 个新字段",
            "entity_list": f"{schema}.{final_model_name}",
            "review_requesters": stakeholders.get("requesters", ["@EDW Requester"]),
            "reviewer_mandatory": stakeholders.get("reviewers", ["@EDW Reviewer"])[0] if stakeholders.get("reviewers") else "@EDW Reviewer",
            "knowledge_link": "本文档由AI Agent自动生成，包含模型增强信息",
            "review_date": datetime.now().strftime('%Y年%m月%d日'),
            "status_tags": [
                {"title": "REQUIRE UPDATE", "color": "Yellow"}
            ],
            "dataflow": {
                "source": ", ".join(base_tables[:3]) + ("..." if len(base_tables) > 3 else "") if base_tables else "Multiple Source Tables",
                "target": table_name
            },
            "model_fields": []
        }

        # 构建model_fields - 添加新增字段信息（按用户指定格式）
        if fields:
            for field in fields:
                # 兼容字典和对象访问
                if isinstance(field, dict):
                    attribute_name = field.get('attribute_name', field.get('physical_name', ''))
                    column_name = field.get('physical_name', '')
                    column_type = field.get('data_type', 'STRING')
                else:
                    attribute_name = getattr(field, 'attribute_name', getattr(field, 'physical_name', ''))
                    column_name = getattr(field, 'physical_name', '')
                    column_type = getattr(field, 'data_type', 'STRING')

                field_info = {
                    "schema": schema,
                    "mode_name": model_name or f"{table.replace('_', ' ').title()}",
                    "table_name": table,
                    "attribute_name": attribute_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "pk": "N"  # 新增字段通常不是主键
                }
                custom_model_config["model_fields"].append(field_info)


        # 直接使用ConfluenceManager创建页面
        from src.basic.confluence.confluence_operate import ConfluenceManager

        cm = ConfluenceManager(
            tools.confluence_url,
            tools.username,
            "",
            tools.api_token
        )

        # 查找目标空间
        target_space = cm.find_space_by_name(tools.target_space_name)
        if not target_space:
            raise Exception(f"未找到空间: {tools.target_space_name}")

        space_key = target_space['key']

        # 确定页面路径
        page_path = tools._get_page_path_for_schema(schema)

        # 查找父页面（严格路径匹配）
        parent_page = cm.find_page_by_path(space_key, page_path)
        if not parent_page:
            error_msg = f"未找到父页面路径: {' -> '.join(page_path)}"
            raise Exception(error_msg)

        # 创建页面
        new_page = cm.create_data_model_page(
            space_key=space_key,
            model_config=custom_model_config,
            parent_id=parent_page['id']
        )

        if new_page:
            # 添加标签
            labels = ['EDW', 'Enhanced-Model', 'Auto-Generated', schema]
            if model_name:
                labels.append(model_name.replace(' ', '-'))
            if fields:
                labels.append('New-Fields')

            cm.add_page_labels(new_page['id'], labels)

            # 评论功能已暂时移除
            logger.info("页面创建完成，评论功能已禁用")

            page_url = f"{tools.confluence_url.rstrip('/')}/pages/viewpage.action?pageId={new_page['id']}"

            result = {
                "success": True,
                "page_id": new_page['id'],
                "page_title": new_page['title'],
                "page_url": page_url,
                "space": tools.target_space_name,
                "labels": labels,
                "creation_time": datetime.now().isoformat()
            }
        else:
            raise Exception("页面创建失败")

        return result

    except Exception as e:
        error_msg = f"创建Confluence文档时发生异常: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }


def edw_confluence_node(state: EDWState):
    """增强的Confluence文档更新节点"""

    try:
        # 提取状态中的信息
        table_name = state.get("table_name", "")
        model_name = state.get("model_name", "") or state.get("model_attribute_name", "")
        enhanced_code = state.get("enhance_code", "")
        alter_table_sql = state.get("alter_table_sql", "")
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        base_tables = state.get("base_tables", [])


        # 验证必要信息
        if not table_name:
            error_msg = "缺少表名信息，无法创建Confluence文档"
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        if not enhanced_code:
            error_msg = "缺少增强代码，无法创建完整的Confluence文档"
            logger.warning(error_msg)
            # 不阻止流程，但记录警告

        if not fields:
            error_msg = "没有新增字段信息，将创建基础文档"
            logger.warning(error_msg)


        # 异步执行Confluence文档创建
        confluence_result = asyncio.run(_create_confluence_documentation(
            table_name=table_name,
            model_name=model_name,
            enhanced_code=enhanced_code,
            fields=fields,
            alter_table_sql=alter_table_sql,
            user_id=user_id,
            enhancement_type=enhancement_type,
            base_tables=base_tables
        ))

        if confluence_result.get("success"):
            logger.info("Confluence文档创建成功")

            # 保存Confluence信息到state中，方便后续节点使用
            confluence_page_url = confluence_result.get("page_url", "")
            confluence_page_id = confluence_result.get("page_id", "")
            confluence_title = confluence_result.get("page_title", "")


            return {
                "user_id": user_id,
                # 将Confluence信息保存到state中供后续节点使用
                "confluence_page_url": confluence_page_url,  # 重要：保存页面链接到state
                "confluence_page_id": confluence_page_id,    # 保存页面ID到state
                "confluence_title": confluence_title,        # 保存页面标题到state
                # 其他详细结果
                "confluence_result": confluence_result,
                "confluence_creation_time": confluence_result.get("creation_time"),
                "session_state": "confluence_completed"
            }
        else:
            error_msg = confluence_result.get("error", "未知错误")
            logger.error(f"Confluence文档创建失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "confluence_attempted": True
            }

    except Exception as e:
        error_msg = f"Confluence节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


async def _update_adb_notebook(adb_path: str, enhanced_code: str, language: str) -> dict:
    """异步更新ADB笔记本的核心函数"""
    try:
        from src.mcp.mcp_client import get_mcp_client


        async with get_mcp_client() as client:
            if client:
                try:
                    # 获取所有MCP工具
                    tools = await client.get_tools()

                    # 查找 import_notebook 工具
                    import_tool = None
                    for tool in tools:
                        if hasattr(tool, 'name') and 'import' in tool.name.lower() and 'notebook' in tool.name.lower():
                            import_tool = tool
                            break

                    if import_tool:

                        # 调用import_notebook方法
                        result = await import_tool.ainvoke({
                            "path": adb_path,
                            "content": enhanced_code,
                            "language": language
                        })

                        return {
                            "success": True,
                            "result": str(result),
                            "adb_path": adb_path,
                            "language": language
                        }

                    else:
                        error_msg = "未找到import_notebook相关的MCP工具"
                        logger.error(error_msg)
                        return {
                            "success": False,
                            "error": error_msg
                        }

                except Exception as e:
                    error_msg = f"MCP工具调用失败: {str(e)}"
                    logger.error(error_msg)
                    return {
                        "success": False,
                        "error": error_msg
                    }
            else:
                error_msg = "MCP客户端连接失败"
                logger.error(error_msg)
                return {
                    "success": False,
                    "error": error_msg
                }

    except Exception as e:
        error_msg = f"更新ADB笔记本时发生异常: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }


def _detect_code_language(code_path: str, source_code: str = "") -> str:
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


def edw_adb_update_node(state: EDWState):
    """增强的ADB数据库更新节点 - 调用MCP服务更新笔记本"""

    try:
        # 提取状态中的信息
        adb_code_path = state.get("adb_code_path")
        enhanced_code = state.get("enhance_code")
        code_path = state.get("code_path")
        source_code = state.get("source_code", "")
        user_id = state.get("user_id", "")
        table_name = state.get("table_name")


        # 验证必要参数
        if not adb_code_path:
            error_msg = "缺少ADB代码路径"
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        if not enhanced_code:
            error_msg = "缺少增强后的代码"
            return {
                "error_message": error_msg,
                "user_id": user_id
            }

        # 检测代码语言
        language = _detect_code_language(code_path or adb_code_path, source_code)


        # 异步执行ADB更新
        import asyncio
        update_result = asyncio.run(_update_adb_notebook(
            adb_path=adb_code_path,
            enhanced_code=enhanced_code,
            language=language
        ))

        if update_result.get("success"):
            logger.info("ADB笔记本更新成功")

            return {
                "user_id": user_id,
                "adb_update_result": update_result,
                "adb_path_updated": adb_code_path,
                "code_language": language,
                "update_timestamp": datetime.now().isoformat(),
                "session_state": "adb_update_completed"
            }
        else:
            error_msg = update_result.get("error", "未知错误")
            logger.error(f"ADB更新失败: {error_msg}")
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "adb_path": adb_code_path
            }

    except Exception as e:
        error_msg = f"ADB更新节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", "")
        }


def model_routing_fun(state: EDWState):
    """模型开发路由函数"""
    if "model_enhance" in state["type"]:
        return "model_enhance_data_validation_node"
    elif "model_add" in state["type"]:
        return "model_add_data_validation_node"
    else:
        return END


def validation_check_node(state: EDWState):
    """验证检查节点：处理验证状态并实施中断"""
    from langgraph.types import interrupt, Command
    
    validation_status = state.get("validation_status")
    user_id = state.get("user_id", "")
    
    # 如果验证信息不完整，触发中断
    if validation_status == "incomplete_info":
        error_message = state.get("error_message", "需要补充信息")
        failed_node = state.get("failed_validation_node", "unknown")
        
        logger.info(f"验证失败于节点: {failed_node}, 准备中断等待用户输入")
        
        # 🔥 在节点中中断，等待用户补充信息
        user_input = interrupt({
            "prompt": error_message,
            "failed_node": failed_node,
            "validation_status": "waiting_for_input"
        })
        
        # 用户输入作为新消息添加到状态中
        return {
            "messages": [HumanMessage(content=user_input)],
            "validation_status": "retry",  # 标记需要重试
            "user_id": user_id
        }
    
    # 验证通过，可以继续
    elif validation_status == "completed":
        
        return {
            "validation_status": "proceed",  # 标记可以继续
            "user_id": user_id
        }
    
    # 其他情况
    return {"user_id": user_id}


def route_after_validation_check(state: EDWState):
    """验证检查后的路由函数"""
    validation_status = state.get("validation_status")
    
    if validation_status == "proceed":
        # 验证通过，继续到增强节点
        return "model_enhance_node"
    elif validation_status == "retry":
        # 需要重试，回到验证子图
        return "model_enhance_data_validation_node"
    else:
        # 默认结束
        return END


def enhancement_routing_fun(state: EDWState):
    """增强完成后的路由函数：决定是否需要走后续流程"""
    enhancement_type = state.get("enhancement_type", "")

    # 如果是仅修改逻辑，直接结束
    if enhancement_type == "modify_logic":
        logger.info("检测到仅修改逻辑，跳过ADB更新等后续流程")
        return END

    # 其他类型进入微调询问流程
    logger.info(f"增强类型 {enhancement_type}，进入微调询问流程")
    return "refinement_inquiry_node"


def refinement_loop_routing(state: EDWState):
    """基于LLM分析结果的智能循环路由"""
    
    user_intent = state.get("user_intent", "SATISFIED_CONTINUE")
    intent_confidence = state.get("intent_confidence", 0.5)
    
    logger.info(f"微调路由决策 - 意图: {user_intent}, 置信度: {intent_confidence}")
    
    # 高置信度的意图识别
    if intent_confidence >= 0.8:
        if user_intent == "REFINEMENT_NEEDED":
            return "code_refinement_node"
        elif user_intent in ["SATISFIED_CONTINUE", "UNRELATED_TOPIC"]:
            return "github_push_node"
    
    # 低置信度情况下的保守策略
    elif intent_confidence >= 0.6:
        if user_intent == "REFINEMENT_NEEDED":
            return "code_refinement_node"  # 倾向于响应用户需求
        else:
            return "github_push_node"
    
    # 极低置信度，默认继续流程
    else:
        logger.warning(f"意图识别置信度过低 ({intent_confidence})，默认继续流程")
        return "github_push_node"


# 创建验证子图实例
validation_subgraph = create_validation_subgraph()

model_dev_graph = (
    StateGraph(EDWState)
    .add_node("model_enhance_data_validation_node", validation_subgraph)
    .add_node("validation_check_node", validation_check_node)  # 验证检查节点
    .add_node("model_add_data_validation_node", edw_model_add_data_validation_node)
    .add_node("model_enhance_node", edw_model_enhance_node)
    .add_node("model_addition_node", edw_model_addition_node)
    # 新增微调相关节点
    .add_node("refinement_inquiry_node", refinement_inquiry_node)       # 微调询问节点
    .add_node("refinement_intent_node", refinement_intent_node)         # 意图识别节点  
    .add_node("code_refinement_node", code_refinement_node)             # 微调执行节点
    # 原有后续节点
    .add_node("github_push_node", github_push_node)
    .add_node("adb_update_node", edw_adb_update_node)
    .add_node("email_node", edw_email_node)
    .add_node("confluence_node", edw_confluence_node)
    
    # 路由配置
    .add_conditional_edges(START, model_routing_fun, ["model_enhance_data_validation_node", "model_add_data_validation_node"])
    # 验证子图完成后进入检查节点
    .add_edge("model_enhance_data_validation_node", "validation_check_node")
    # 从检查节点出来后的条件路由
    .add_conditional_edges("validation_check_node", route_after_validation_check, [
        "model_enhance_node",               # 验证通过，继续
        "model_enhance_data_validation_node",  # 需要重试
        END                                  # 其他情况结束
    ])
    .add_edge("model_add_data_validation_node", "model_addition_node")
    
    # 🎯 增强完成后进入微调流程
    .add_conditional_edges("model_enhance_node", enhancement_routing_fun, [
        "refinement_inquiry_node",          # 进入微调询问
        END                                 # 仅修改逻辑直接结束
    ])
    
    # 🔄 微调循环流程
    .add_edge("refinement_inquiry_node", "refinement_intent_node")      # 询问→意图识别
    .add_conditional_edges("refinement_intent_node", refinement_loop_routing, [
        "code_refinement_node",             # 需要微调
        "github_push_node"                  # 满意，继续后续流程
    ])
    .add_edge("code_refinement_node", "refinement_inquiry_node")        # 微调完成→再次询问（形成循环）
    
    # 原有后续流程保持不变
    .add_edge("model_addition_node", "github_push_node")  # 模型新增也要推送到GitHub
    .add_edge("github_push_node", "adb_update_node")  # GitHub推送后再更新ADB
    .add_edge("adb_update_node", "confluence_node")
    .add_edge("confluence_node", "email_node")
    .add_edge("email_node", END)
)

model_dev = model_dev_graph.compile(
    checkpointer=get_shared_checkpointer()  # 支持子图中断-恢复机制
)


def routing_fun(state: EDWState):
    """主路由函数：决定进入聊天还是模型处理"""
    if 'model' in state["type"]:
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

guid = guid_graph.compile(
    checkpointer=get_shared_checkpointer()  # 支持跨图的中断-恢复机制
)


def create_message_from_input(user_input: str) -> HumanMessage:
    """将用户输入转换为标准消息格式"""
    return HumanMessage(content=user_input)


# 状态管理现在由LangGraph的checkpointer机制处理，移除手动状态管理逻辑


if __name__ == "__main__":
    # 这个文件不应该直接运行，应通过API或其他接口调用
    pass

    # 模拟用户ID（实际应用中应该从认证系统获取）
    user_id = str(uuid.uuid4())[:8]
    logger.info(f"当前用户ID: {user_id}")

    # 记录初始系统状态
    logger.info(f"配置文件路径: {config_manager.config_dir}")

    # 记录MCP连接配置
    databricks_config = config_manager.get_mcp_server_config("databricks")
    if databricks_config:
        if databricks_config.transport == "sse":
            logger.info(f"MCP连接模式: SSE - {databricks_config.url}")
            logger.info(f"连接参数: 超时={databricks_config.timeout}s, 重试={databricks_config.retry_count}次")
        else:
            logger.info(f"MCP连接模式: {databricks_config.transport}")
            logger.info(f"连接参数: 超时={databricks_config.timeout}s, 重试={databricks_config.retry_count}次")
    else:
        logger.info("MCP连接模式: 默认配置")

    # 记录缓存状态
    if cache_manager:
        stats = cache_manager.get_stats()
        logger.info(f"缓存系统已启动 - TTL: {stats['ttl_seconds']}秒, 最大条目: {stats['max_entries']}")
    else:
        logger.info("缓存系统已禁用")
    
    # 异步初始化智能体（包括代码增强智能体）
    async def initialize_system():
        """异步初始化系统组件"""
        try:
            from src.agent.edw_agents import async_initialize_agents
            await async_initialize_agents()
            logger.info("系统异步初始化完成")
        except Exception as e:
            logger.error(f"系统异步初始化失败: {e}")
            # 即使失败也继续运行，代码增强功能可能不可用
    
    # 运行异步初始化
    try:
        asyncio.run(initialize_system())
    except Exception as e:
        logger.error(f"异步初始化运行失败: {e}")
        print("警告: 代码增强功能可能不可用")
    
    index = 0
    
    # 定义处理输出的函数，避免代码重复
    def process_output(chunk, displayed_content, final_state_holder):
        """处理流输出的辅助函数"""
        if chunk:
            for node_name, node_data in chunk.items():
                if isinstance(node_data, dict):
                    final_state_holder[0] = node_data
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

            # 处理状态重置命令
            if readline.lower() == "/reset":
                # 使用LangGraph checkpointer机制重置状态
                config = SessionManager.get_config(user_id, "main")
                try:
                    # 清除checkpointer中的会话状态
                    checkpointer = get_shared_checkpointer()
                    if hasattr(checkpointer, 'alist'):
                        # 删除该用户的所有checkpoints
                        for checkpoint_tuple in checkpointer.alist(config):
                            checkpointer.delete(config, checkpoint_tuple.checkpoint['id'])
                    print(f"用户 {user_id} 的状态已重置")
                except Exception as e:
                    print(f"状态重置失败: {e}")
                continue

            # 使用统一配置管理器 - 主会话
            config = SessionManager.get_config(user_id, "main")

            # 创建简单的初始状态（LangGraph checkpointer会自动管理历史状态）
            initial_state = {
                "messages": [create_message_from_input(readline)],
                "user_id": user_id,
            }

            displayed_content = set()  # 避免重复显示相同内容
            final_state_holder = [None]  # 使用列表来跟踪最终状态（可变对象）
            
            # 初始执行
            stream_input = initial_state
            
            # 循环处理中断，直到流程完成
            while True:
                # 执行图
                for chunk in guid.stream(stream_input, config, stream_mode="updates"):
                    process_output(chunk, displayed_content, final_state_holder)
                
                # 检查是否有中断
                current_state = guid.get_state(config)
                if current_state.next:  # 如果有待执行的节点，说明被中断了
                    # 获取中断信息
                    interrupts = current_state.tasks
                    if interrupts:
                        interrupt_found = False
                        for task in interrupts:
                            if task.interrupts:
                                interrupt_info = task.interrupts[0]
                                prompt = interrupt_info.value.get("prompt", "需要补充信息")
                                print(f"\nAI: {prompt}")
                                
                                # 等待用户输入
                                user_response = input("\n用户输入: ")
                                
                                # 准备恢复执行
                                stream_input = Command(resume=user_response)
                                interrupt_found = True
                                break
                        
                        if interrupt_found:
                            continue  # 继续循环，恢复执行
                
                # 没有中断或没有找到中断信息，结束循环
                break
            
            # 状态管理现在由LangGraph checkpointer自动处理

        except KeyboardInterrupt:
            print("\n用户中断操作")
            break
        except Exception as e:
            logger.error(f"主程序异常: {e}")
            print(f"发生错误: {e}")
