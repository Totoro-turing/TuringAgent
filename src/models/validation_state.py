"""
验证子图专用状态类
使用 LangGraph 内置的 memory 功能进行状态管理
每个验证节点需要的属性都在这里定义
"""

from typing import TypedDict, List, Optional, Dict, Any, Annotated
from langchain.schema.messages import AnyMessage
from operator import add


class ValidationState(TypedDict):
    """验证子图专用状态 - 包含所有验证流程需要的字段"""
    
    # ========== 核心字段（所有节点共享） ==========
    # 消息历史（LangGraph自动累积）
    messages: Annotated[List[AnyMessage], add]
    
    # 用户和会话信息
    user_id: str  # 用户ID，用于会话隔离
    type: Optional[str]  # 任务类型，从主图继承
    
    # 验证流程控制状态
    validation_status: Optional[str]  # 验证状态：processing, incomplete_info, completed, retry, proceed
    failed_validation_node: Optional[str]  # 失败的验证节点名称（用于智能路由）
    retry_count: Optional[int]  # 重试次数
    is_resume_execution: Optional[bool]  # 是否是恢复执行（用于智能路由跳转）
    smart_route_target: Optional[str]  # 智能路由目标节点
    
    # ========== parse_user_input_node 需要的字段 ==========
    # 节点功能：解析用户输入，提取关键信息
    # 输入：messages, user_id, failed_validation_node, error_message, validation_status
    # 输出：parsed_request, table_name, model_attribute_name, enhancement_type等
    error_message: Optional[str]  # 错误信息（用于构建对话历史）
    
    # ========== validate_model_name_node 需要的字段 ==========
    # 节点功能：验证英文模型名称格式
    # 输入：model_attribute_name
    # 输出：validation_status, error_message, failed_validation_node
    model_attribute_name: Optional[str]  # 模型属性名称（英文）
    
    # ========== validate_completeness_node 需要的字段 ==========
    # 节点功能：验证信息完整性
    # 输入：parsed_request（包含所有解析的信息）
    # 输出：validation_status, missing_info, error_message, failed_validation_node
    parsed_request: Optional[Dict[str, Any]]  # 解析的请求数据（包含所有字段）
    missing_info: Optional[List[str]]  # 缺失的信息列表
    
    # 从parsed_request提取的核心字段（供后续节点使用）
    table_name: Optional[str]  # 表名
    enhancement_type: Optional[str]  # 增强类型：add_field, modify_logic等
    logic_detail: Optional[str]  # 逻辑详情
    business_purpose: Optional[str]  # 业务用途描述
    business_requirement: Optional[str]  # 业务需求描述
    field_info: Optional[str]  # 字段信息描述
    fields: Optional[List[dict]]  # 新增字段列表（每个字段包含physical_name, attribute_name等）
    
    # ========== search_table_code_node 需要的字段 ==========
    # 节点功能：查询表的源代码
    # 输入：table_name
    # 输出：source_code, code_path, adb_code_path, base_tables, collected_info
    source_code: Optional[str]  # 源代码
    code_path: Optional[str]  # 本地代码路径
    adb_code_path: Optional[str]  # ADB中的代码路径
    base_tables: Optional[List[str]]  # 从代码中提取的底表列表
    
    # ========== validate_field_base_tables_node 需要的字段 ==========
    # 节点功能：验证字段与底表的关联性
    # 输入：base_tables, fields, source_code
    # 输出：validation_status, field_validation, error_message, failed_validation_node
    field_validation: Optional[dict]  # 字段验证结果（包含valid, invalid_fields, suggestions等）
    
    # ========== 收集的信息汇总 ==========
    collected_info: Optional[dict]  # 已收集的所有信息（供后续使用）
    session_state: Optional[str]  # 会话状态（validation_completed等）