"""
模型增强验证节点模块
将原本庞大的验证函数拆分为多个独立的验证节点
使用专门的 ValidationState 进行状态管理，包含memory功能
"""

import logging
from typing import Dict, Any
from langchain.schema.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from langgraph.config import get_stream_writer
from datetime import datetime

from src.agent.edw_agents import get_validation_agent, get_shared_parser
from src.models.edw_models import ModelEnhanceRequest, FieldDefinition
from src.config import get_config_manager
from src.models.validation_state import ValidationState

logger = logging.getLogger(__name__)
config_manager = get_config_manager()
valid_agent = get_validation_agent()
parser = get_shared_parser()


def parse_user_input_node(state: ValidationState) -> dict:
    """节点1: 解析用户输入，提取关键信息 - 支持智能路由"""
    print(">>> Parse User Input Node (Smart Routing)")
    writer = get_stream_writer()
    
    # 检查是否是从中断恢复
    failed_node = state.get("failed_validation_node")
    retry_count = state.get("retry_count", 0)
    is_resume = failed_node is not None
    
    if is_resume:
        writer({
            "node": ">>> parse_user_input", 
            "status": f"从中断恢复执行 (第{retry_count + 1}次重试)",
            "previous_failed_node": failed_node
        })
    else:
        writer({"node": ">>> parse_user_input", "status": "首次解析用户需求..."})
    
    # 导入需要的依赖
    from src.graph.edw_graph import SessionManager
    
    try:
        config = SessionManager.get_config(state.get("user_id", ""), "validation")
        
        # 获取消息内容
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            content = last_message
        else:
            content = last_message.content if hasattr(last_message, 'content') else str(last_message)
        
        # 构建消息列表 - 智能上下文构建
        messages = []
        # 检查是否有之前的验证错误（无论是通过 failed_node 还是 error_message）
        previous_error = state.get("error_message", "")
        has_previous_error = is_resume or (previous_error and state.get("validation_status") == "incomplete_info")
        
        if has_previous_error:
            # 统一处理所有验证错误的情况
            if is_resume:
                logger.info(f"从中断恢复解析，失败节点: {failed_node}")
                error_prefix = "数据验证失败，"
            else:
                logger.info("检测到之前的验证错误，构建对话历史")
                error_prefix = ""
            
            messages = [
                AIMessage(content=f"{error_prefix}{previous_error}"),
                HumanMessage(content=content)
            ]
        else:
            # 首次解析
            messages = [HumanMessage(content=content)]
        
        # 使用验证代理提取关键信息
        response = valid_agent.invoke({"messages": messages}, config)
        validation_result = response["messages"][-1].content
        
        logger.info(f"LLM原始响应: {validation_result}")
        writer({"llm_response": validation_result})
        
        # 解析响应
        try:
            parsed_request = parser.parse(validation_result)
            parsed_data = parsed_request.model_dump()
            writer({"parsed_data": parsed_data})
            
            result = {
                "validation_status": "processing",
                "parsed_request": parsed_data,
                "table_name": parsed_request.table_name if parsed_request.table_name else "",
                "model_attribute_name": parsed_request.model_attribute_name,
                "enhancement_type": parsed_request.enhancement_type,
                "logic_detail": parsed_request.logic_detail,
                "business_purpose": parsed_request.business_purpose,
                "business_requirement": parsed_request.business_requirement,
                "field_info": parsed_request.field_info,
                "fields": [field.model_dump() for field in parsed_request.fields] if parsed_request.fields else []
            }
            
            # 🎯 智能路由：根据之前失败的节点决定下一步跳转
            if is_resume and failed_node:
                result["smart_route_target"] = failed_node
                result["is_resume_execution"] = True
                result["retry_count"] = retry_count + 1
                writer({"smart_routing": f"将直接跳转到失败节点: {failed_node}"})
                
                # 保留一些有用的缓存信息（如果存在）
                if state.get("source_code"):
                    result["source_code"] = state["source_code"]
                if state.get("base_tables"):
                    result["base_tables"] = state["base_tables"]
                if state.get("adb_code_path"):
                    result["adb_code_path"] = state["adb_code_path"]
                if state.get("code_path"):
                    result["code_path"] = state["code_path"]
            
            return result
            
        except Exception as parse_error:
            error_msg = "信息格式解析失败。请使用更清晰的格式描述需求。"
            logger.error(f"解析错误: {str(parse_error)}. 原始响应: {validation_result}")
            writer({"error": error_msg})
            
            return {
                "validation_status": "incomplete_info",
                "failed_validation_node": "parse_input",  # 🔥 记录失败节点
                "error_message": error_msg,
                "messages": [HumanMessage(error_msg)]
            }
            
    except Exception as e:
        error_msg = f"解析用户输入失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "parse_input",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }


def validate_model_name_node(state: ValidationState) -> dict:
    """节点2: 验证英文模型名称格式"""
    print(">>> Validate Model Name Node")
    writer = get_stream_writer()
    writer({"node": ">>> validate_model_name"})
    
    # 导入验证函数
    from src.graph.edw_graph import _validate_english_model_name
    
    model_attribute_name = state.get("model_attribute_name")
    
    # 如果没有提供模型名称，跳过验证
    if not model_attribute_name:
        return {"validation_status": "processing"}
    
    # 验证英文模型名称格式
    is_valid_name, name_error = _validate_english_model_name(model_attribute_name)
    
    if not is_valid_name:
        error_msg = f"模型名称格式不正确：{name_error}\n\n请使用标准的英文格式，例如：\n- Finance Invoice Header\n- Customer Order Detail\n- Inventory Management System"
        writer({"error": error_msg})
        writer({"content": error_msg})
        
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "validate_name",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }
    
    return {"validation_status": "processing"}


def validate_completeness_node(state: ValidationState) -> dict:
    """节点3: 验证信息完整性"""
    print(">>> Validate Completeness Node")
    writer = get_stream_writer()
    writer({"node": ">>> validate_completeness"})
    
    try:
        # 从 state 重新构建 ModelEnhanceRequest 对象进行验证
        parsed_data = state.get("parsed_request", {})
        
        # 转换 fields 为 FieldDefinition 对象
        fields = []
        if parsed_data.get("fields"):
            for field_dict in parsed_data["fields"]:
                fields.append(FieldDefinition(**field_dict))
        
        # 创建请求对象
        request = ModelEnhanceRequest(
            table_name=parsed_data.get("table_name", ""),
            enhancement_type=parsed_data.get("enhancement_type", ""),
            logic_detail=parsed_data.get("logic_detail", ""),
            field_info=parsed_data.get("field_info", ""),
            business_requirement=parsed_data.get("business_requirement", ""),
            model_attribute_name=parsed_data.get("model_attribute_name", ""),
            business_purpose=parsed_data.get("business_purpose", ""),
            fields=fields
        )
        
        # 验证完整性
        is_complete, missing_fields = request.validate_completeness()
        
        if not is_complete:
            missing_info_text = "\n".join([f"- {info}" for info in missing_fields])
            
            # 如果是新增字段但缺少字段信息，添加额外提示
            if request.enhancement_type == "add_field" or any(
                keyword in request.logic_detail 
                for keyword in ["增加字段", "新增字段", "添加字段"]
            ):
                if "字段定义" in str(missing_fields):
                    missing_info_text += "\n\n示例格式：\n"
                    missing_info_text += "单个字段：给dwd_fi.fi_invoice_item表增加字段invoice_doc_no（Invoice Document Number）\n"
                    missing_info_text += "多个字段：给表增加invoice_doc_no（Invoice Document Number）和customer_type（Customer Type）两个字段"
            
            complete_message = f"为了帮您完成模型增强，我需要以下信息：\n{missing_info_text}\n\n请补充完整信息后重新提交。"
            
            writer({"error": complete_message})
            writer({"missing_fields": missing_fields})
            writer({"content": complete_message})
            
            return {
                "validation_status": "incomplete_info",
                "failed_validation_node": "validate_completeness",  # 🔥 记录失败节点
                "missing_info": missing_fields,
                "error_message": complete_message,
                "messages": [HumanMessage(complete_message)]
            }
        
        return {"validation_status": "processing"}
        
    except Exception as e:
        error_msg = f"验证信息完整性失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "validate_completeness",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }


def search_table_code_node(state: ValidationState) -> dict:
    """节点4: 查询表的源代码"""
    print(">>> Search Table Code Node")
    writer = get_stream_writer()
    writer({"node": ">>> search_table_code"})
    
    # 导入需要的函数
    from src.graph.edw_graph import search_table_cd, convert_to_adb_path, extract_tables_from_code
    
    table_name = state.get("table_name", "").strip()
    
    if not table_name:
        error_msg = "表名为空，无法查询源代码"
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "search_code",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }
    
    writer({"status": f"正在查询表 {table_name} 的源代码..."})
    
    try:
        code_info = search_table_cd(table_name)
        logger.info(f"表代码查询结果: {str(code_info)[:200] if code_info else 'None'}...")
        
        if code_info.get("status") == "error":
            error_msg = f"未找到表 {table_name} 的源代码: {code_info.get('message', '未知错误')}\n请确认表名是否正确。"
            writer({"error": error_msg})
            writer({"content": error_msg})
            
            return {
                "validation_status": "incomplete_info",
                "failed_validation_node": "search_code",  # 🔥 记录失败节点
                "error_message": error_msg,
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
        
        return {
            "validation_status": "processing",
            "source_code": source_code,
            "code_path": code_path,
            "adb_code_path": adb_path,
            "base_tables": base_tables,
            "collected_info": {
                "table_code_info": code_info,
                "adb_path": adb_path,
                "base_tables": base_tables
            }
        }
        
    except Exception as e:
        error_msg = f"查询表代码失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "search_code",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }


async def validate_field_base_tables_node(state: ValidationState) -> dict:
    """节点5: 验证字段与底表的关联性"""
    print(">>> Validate Field Base Tables Node")
    writer = get_stream_writer()
    writer({"node": ">>> validate_field_base_tables"})
    
    # 导入需要的函数
    from src.graph.edw_graph import validate_fields_against_base_tables
    
    base_tables = state.get("base_tables", [])
    fields = state.get("fields", [])
    source_code = state.get("source_code", "")
    
    # 如果没有底表或字段，跳过验证
    if not base_tables or not fields:
        logger.info("未找到底表或新增字段为空，跳过字段验证")
        return {
            "validation_status": "completed",
            "session_state": "validation_completed"
        }
    
    writer({"status": f"正在验证 {len(fields)} 个新增字段与底表的关联性..."})
    
    try:
        # 转换字段格式
        field_objects = []
        for field_dict in fields:
            field_objects.append(FieldDefinition(**field_dict))
        
        field_validation = await validate_fields_against_base_tables(
            field_objects,
            base_tables,
            source_code
        )
        
        if not field_validation["valid"]:
            # 构建错误消息（与原代码保持一致）
            if field_validation.get("service_error"):
                validation_error_msg = field_validation["error_message"]
            else:
                # 构建详细的错误信息
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
                            field_msg += f"\n  建议字段: {', '.join(suggestion_list)}"
                    
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
                    cache_info = f"\n\n**查询性能**: 耗时{cache_perf['duration_seconds']}秒, 缓存命中率: {cache_perf['overall_hit_rate']}"
                
                validation_error_msg = f"""字段验证失败，以下字段在底表中未找到相似字段：

{chr(10).join(invalid_fields_msg)}

**底表字段信息**:
{chr(10).join(base_tables_info) if base_tables_info else '无法获取底表字段信息'}{cache_info}

请确认字段名称是否正确，或参考建议字段进行修正。"""
            
            writer({"error": validation_error_msg})
            writer({"content": validation_error_msg})
            writer({"field_validation": field_validation})
            
            return {
                "validation_status": "incomplete_info",
                "failed_validation_node": "validate_fields",  # 🔥 记录失败节点
                "error_message": validation_error_msg,
                "field_validation": field_validation,
                "messages": [HumanMessage(validation_error_msg)]
            }
        else:
            writer({"status": "字段验证通过"})
            
            # 添加缓存性能信息
            if "cache_performance" in field_validation:
                cache_perf = field_validation["cache_performance"]
                writer({"cache_performance": f"查询性能: 耗时{cache_perf['duration_seconds']}秒, 缓存命中率: {cache_perf['overall_hit_rate']}"})
            
            if field_validation["suggestions"]:
                suggestions_msg = "字段建议：\n"
                for field_name, suggestions in field_validation["suggestions"].items():
                    suggestions_msg += f"- {field_name}: 发现相似字段 {suggestions[0]['field_name']} (相似度: {suggestions[0]['similarity']:.2f})\n"
                writer({"field_suggestions": suggestions_msg})
            
            return {
                "validation_status": "completed",
                "field_validation": field_validation,
                "session_state": "validation_completed"
            }
    
    except Exception as e:
        error_msg = f"验证字段与底表关联性失败: {str(e)}"
        logger.error(error_msg)
        writer({"error": error_msg})
        
        return {
            "validation_status": "incomplete_info",
            "failed_validation_node": "validate_fields",  # 🔥 记录失败节点
            "error_message": error_msg,
            "messages": [HumanMessage(error_msg)]
        }


# 同步包装器
def validate_field_base_tables_node_sync(state: ValidationState) -> dict:
    """验证字段与底表关联性的同步包装器"""
    import asyncio
    import threading
    
    try:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果循环已在运行，创建一个新任务
                result = None
                exception = None
                
                def run_async():
                    nonlocal result, exception
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        result = new_loop.run_until_complete(validate_field_base_tables_node(state))
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
                return loop.run_until_complete(validate_field_base_tables_node(state))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(validate_field_base_tables_node(state))
            finally:
                loop.close()
    
    except Exception as e:
        logger.error(f"异步节点执行失败: {e}")
        return {
            "validation_status": "incomplete_info",
            "error_message": f"验证失败: {str(e)}"
        }


# 路由函数
def smart_route_after_parse(state: Dict[str, Any]) -> str:
    """解析后的智能路由 - 支持直接跳转到失败节点"""
    validation_status = state.get("validation_status")
    
    if validation_status == "incomplete_info":
        return END
    
    # 🎯 智能路由：如果是恢复执行，直接跳转到失败的节点
    if state.get("is_resume_execution") and state.get("smart_route_target"):
        target_node = state.get("smart_route_target")
        print(f"🎯 智能路由到失败节点: {target_node}")
        
        # 根据失败节点映射到对应的验证节点
        node_mapping = {
            "validate_name": "validate_name",
            "validate_completeness": "validate_completeness", 
            "search_code": "search_code",
            "validate_fields": "validate_fields"
        }
        
        return node_mapping.get(target_node, "validate_name")
    
    # 正常流程：从名称验证开始
    return "validate_name"

def route_after_parse(state: Dict[str, Any]) -> str:
    """解析后的路由 - 兼容旧版本"""
    if state.get("validation_status") == "incomplete_info":
        return END
    return "validate_name"


def route_after_name(state: Dict[str, Any]) -> str:
    """名称验证后的路由"""
    if state.get("validation_status") == "incomplete_info":
        return END
    return "validate_completeness"


def route_after_completeness(state: Dict[str, Any]) -> str:
    """完整性验证后的路由"""
    if state.get("validation_status") == "incomplete_info":
        return END
    return "search_code"


def route_after_code(state: Dict[str, Any]) -> str:
    """代码查询后的路由"""
    if state.get("validation_status") == "incomplete_info":
        return END
    return "validate_fields"

def route_to_validation_check(state: Dict[str, Any]) -> str:
    """统一的验证检查路由 - 将所有验证节点的结果路由到集中检查点"""
    validation_status = state.get("validation_status")
    
    if validation_status == "processing":
        # 继续到下一个验证步骤，按照当前节点决定
        current_step = state.get("current_validation_step", "")
        
        step_mapping = {
            "validate_name": "validate_completeness",
            "validate_completeness": "search_code", 
            "search_code": "validate_fields",
        }
        
        next_step = step_mapping.get(current_step)
        if next_step:
            return next_step
        
        # 如果已经是最后一步，进入验证检查
        return "validation_check"
    
    # 有问题时，直接到验证检查点统一处理
    return "validation_check"


# 创建验证子图
def create_validation_subgraph():
    """创建验证子图 - 支持智能路由和直接跳转到失败节点"""
    # 获取共享的checkpointer以支持memory功能
    from src.agent.edw_agents import get_shared_checkpointer
    
    return (
        StateGraph(ValidationState)  # 使用 ValidationState
        .add_node("parse_input", parse_user_input_node)
        .add_node("validate_name", validate_model_name_node)
        .add_node("validate_completeness", validate_completeness_node)
        .add_node("search_code", search_table_code_node)
        .add_node("validate_fields", validate_field_base_tables_node_sync)
        .add_edge(START, "parse_input")
        # 🎯 使用智能路由支持直接跳转到失败节点
        .add_conditional_edges("parse_input", smart_route_after_parse, [
            "validate_name", "validate_completeness", "search_code", "validate_fields", END
        ])
        .add_conditional_edges("validate_name", route_after_name, ["validate_completeness", END])
        .add_conditional_edges("validate_completeness", route_after_completeness, ["search_code", END])
        .add_conditional_edges("search_code", route_after_code, ["validate_fields", END])
        .add_edge("validate_fields", END)
        .compile(checkpointer=get_shared_checkpointer())  # 添加checkpointer支持memory
    )

# 子图现在在需要时动态创建和编译，无需全局变量