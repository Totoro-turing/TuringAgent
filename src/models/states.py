"""
EDW系统统一状态管理

集中管理所有状态类型，包括主工作流状态和子图状态。
使用 LangGraph 的 TypedDict 和 Annotated 类型注解。
"""

from typing import TypedDict, List, Optional, Dict, Any, Annotated
from langchain.schema.messages import AnyMessage
from operator import add


class EDWState(TypedDict):
    """EDW系统统一状态管理"""
    messages: Annotated[List[AnyMessage], add]
    type: str  # 任务类型：other, model_enhance, model_add等
    user_id: str  # 用户ID，用于会话隔离

    # 模型开发相关信息
    table_name: Optional[str]  # 表名
    branch_name: Optional[str]  # 代码分支名称
    code_path: Optional[str]  # 代码路径
    adb_code_path: Optional[str]  # ADB中的代码路径（从code_path转换而来）
    source_code: Optional[str]  # 源代码
    enhance_code: Optional[str]  # 增强后的代码
    create_table_sql: Optional[str]  # 建表语句
    alter_table_sql: Optional[str]  # 修改表语句
    model_name: Optional[str]  # 模型名称（从表comment提取，必须为英文）
    model_attribute_name: Optional[str]  # 用户输入的模型属性名称（英文）
    business_purpose: Optional[str]  # 业务用途描述

    # 信息收集相关
    requirement_description: Optional[str]  # 需求描述
    logic_detail: Optional[str]  # 逻辑详情
    fields: Optional[List[dict]]  # 新增字段列表（每个字段包含physical_name, attribute_name等）
    collected_info: Optional[dict]  # 已收集的信息
    missing_info: Optional[List[str]]  # 缺失的信息列表

    # Confluence文档相关
    confluence_page_url: Optional[str]  # Confluence页面链接
    confluence_page_id: Optional[str]  # Confluence页面ID
    confluence_title: Optional[str]  # Confluence页面标题

    # 会话状态
    session_state: Optional[str]  # 当前会话状态
    error_message: Optional[str]  # 错误信息
    failed_validation_node: Optional[str]  # 错误节点
    # 处理状态字段
    validation_status: Optional[str]  # 验证状态：incomplete_info, completed, processing
    
    # 微调相关字段
    refinement_requested: Optional[bool]  # 用户是否请求微调
    refinement_history: Optional[List[dict]]  # 微调对话历史
    current_refinement_round: Optional[int]  # 当前微调轮次
    original_enhanced_code: Optional[str]  # 原始代码备份
    refinement_feedback: Optional[str]  # 用户最新反馈
    user_refinement_input: Optional[str]  # 用户微调输入
    refinement_conversation_started: Optional[bool]  # 微调对话是否开始
    user_intent: Optional[str]  # 用户意图识别结果
    intent_confidence: Optional[float]  # 意图识别置信度
    intent_reasoning: Optional[str]  # 意图识别推理过程
    refinement_requirements: Optional[str]  # 提取的微调需求
    user_emotion: Optional[str]  # 用户情感倾向
    suggested_response: Optional[str]  # 建议回复内容
    
    # 代码review相关字段
    review_score: Optional[float]  # 代码评分(0-100)
    review_feedback: Optional[str]  # review反馈
    review_suggestions: Optional[List[str]]  # 改进建议列表
    review_round: Optional[int]  # 当前review轮次
    review_history: Optional[List[dict]]  # review历史记录
    has_critical_issues: Optional[bool]  # 是否有严重问题
    max_review_rounds: Optional[int]  # 最大review轮次(默认3)
    
    # 属性名称review相关字段
    attribute_review_completed: Optional[bool]  # 属性review是否完成
    attribute_review_results: Optional[List[dict]]  # 各字段的review结果
    attribute_avg_score: Optional[float]  # 属性命名平均分
    attribute_improvements_applied: Optional[bool]  # 是否应用了改进建议
    attribute_suggestions: Optional[List[dict]]  # 属性名称改进建议
    
    # 统一状态和消息字段 - 避免重复定义
    status: Optional[str]  # 节点执行状态：success, error, skipped, processing等
    status_message: Optional[str]  # 状态相关消息（成功、错误、警告等）
    status_details: Optional[dict]  # 状态详细信息（可选）
    
    # 🎯 Socket通信支持 - 用于实时进度发送
    socket_queue: Optional[Any]  # Socket队列对象，用于发送实时进度到前端
    session_id: Optional[str]  # 会话ID，用于socket消息标识


class ValidationState(TypedDict):
    """验证子图专用状态 - 包含所有验证流程需要的字段"""
    
    # ========== 核心字段（所有节点共享） ==========
    # 消息历史（LangGraph自动累积）
    messages: Annotated[List[AnyMessage], add]
    
    # 用户和会话信息
    user_id: str  # 用户ID，用于会话隔离
    type: Optional[str]  # 任务类型，从主图继承
    
    # 🎯 Socket通信支持 - 用于实时进度发送
    socket_queue: Optional[Any]  # Socket队列对象，用于发送实时进度到前端
    session_id: Optional[str]  # 会话ID，用于socket消息标识
    
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
    branch_name: Optional[str]  # 代码分支名称
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


# 为了向后兼容，可以导出所有状态类型
__all__ = ['EDWState', 'ValidationState']