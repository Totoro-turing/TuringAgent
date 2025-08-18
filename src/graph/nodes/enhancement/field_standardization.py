"""
字段名称标准化节点
强制将属性名称转换为符合EDW标准的物理字段名称
"""

import logging
from typing import List, Dict, Any
from langchain.schema.messages import AIMessage
from src.models.states import EDWState
from src.graph.utils.naming import batch_standardize_field_names

logger = logging.getLogger(__name__)


async def field_standardization_node(state: EDWState) -> dict:
    """
    字段名称标准化节点 - 优化版本
    批量处理所有字段，强制使用系统标准的物理字段名称
    """
    try:
        # 获取字段列表
        fields = state.get("fields", [])
        user_id = state.get("user_id", "")
        table_name = state.get("table_name", "")
        
        if not fields:
            logger.info("没有需要标准化的字段")
            return {"user_id": user_id}
        
        logger.info(f"开始批量标准化 {len(fields)} 个字段的物理名称")
        
        # 批量标准化所有字段（一次性处理）
        standardization_results = await batch_standardize_field_names(fields)
        
        # 处理标准化结果
        standardized_fields = []
        standardization_report = []
        has_changes = False
        
        for i, (field, result) in enumerate(zip(fields, standardization_results)):
            # 更新字段信息
            if isinstance(field, dict):
                standardized_field = field.copy()
            else:
                standardized_field = field.model_dump() if hasattr(field, 'model_dump') else field.__dict__.copy()
            
            # 保留源字段名
            source_name = standardized_field.get("source_name", "")
            # 强制使用系统标准的物理名称（基于属性名称生成）
            standardized_field["source_name"] = source_name  # 保持源字段名不变
            standardized_field["physical_name"] = result["standard_physical_name"]
            standardized_field["attribute_name"] = result["attribute_name"]
            
            standardized_fields.append(standardized_field)
            
            # 生成报告条目
            report_item = {
                "index": i + 1,
                "attribute_name": result["attribute_name"],
                "standard_name": result["standard_physical_name"],
                "user_input": result.get("user_physical_name", ""),
                "is_match": False
            }
            
            # 检查是否有变化
            if "comparison" in result:
                report_item["is_match"] = result["comparison"]["is_match"]
                if not result["comparison"]["is_match"]:
                    has_changes = True
                    report_item["message"] = result["message"]
            elif not result.get("user_physical_name"):
                has_changes = True
                report_item["message"] = result["message"]
                report_item["is_generated"] = True
            
            standardization_report.append(report_item)
        
        # 生成标准化报告消息
        report_message = _generate_standardization_report(
            table_name, 
            standardization_report, 
            has_changes
        )
        
        # 构建返回结果
        result = {
            "fields": standardized_fields,  # 更新后的字段列表
            "user_id": user_id,
            "standardization_report": standardization_report,
            "standardization_completed": True
        }
        
        # 如果有变化，添加报告消息
        if has_changes:
            result["messages"] = [AIMessage(content=report_message)]
            logger.info(f"字段标准化完成，{len([r for r in standardization_report if not r.get('is_match', True)])} 个字段被标准化")
        else:
            logger.info("所有字段名称已符合标准，无需调整")
        
        return result
        
    except Exception as e:
        error_msg = f"字段标准化失败: {str(e)}"
        logger.error(error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "standardization_completed": False
        }


def _generate_standardization_report(table_name: str, 
                                    report: List[Dict[str, Any]], 
                                    has_changes: bool) -> str:
    """
    生成标准化报告
    
    Args:
        table_name: 表名
        report: 标准化报告列表
        has_changes: 是否有变化
    
    Returns:
        格式化的报告消息
    """
    if not has_changes:
        return f"[SUCCESS] 所有字段名称已符合EDW标准，无需调整"
    
    # 构建报告
    lines = [
        "## 字段名称标准化完成",
        "",
        f"**目标表**: {table_name}",
        "",
        "系统已根据EDW命名标准自动处理物理字段名称：",
        "",
        "| # | 属性名称 | 系统标准名称 | 用户输入 | 状态 |",
        "|---|----------|-------------|---------|------|"
    ]
    
    for item in report:
        index = item["index"]
        attribute_name = item["attribute_name"]
        standard_name = item["standard_name"]
        user_input = item.get("user_input", "-")
        
        if item.get("is_generated"):
            status = "[NEW] 自动生成"
        elif item.get("is_match", False):
            status = "[OK] 一致"
        else:
            status = "[UPDATE] 已标准化"
        
        lines.append(f"| {index} | {attribute_name} | {standard_name} | {user_input} | {status} |")
    
    # 添加说明
    lines.extend([
        "",
        "**说明**:",
        "- [NEW] 自动生成：系统根据属性名称自动生成物理名称",
        "- [UPDATE] 已标准化：用户输入已调整为系统标准",
        "- [OK] 一致：用户输入与系统标准一致",
        "",
        "**注意**：系统将使用标准化后的物理字段名称进行后续处理"
    ])
    
    return "\n".join(lines)


def _format_field_name_for_display(name: str, max_length: int = 30) -> str:
    """
    格式化字段名称用于显示
    
    Args:
        name: 字段名称
        max_length: 最大长度
    
    Returns:
        格式化后的名称
    """
    if not name:
        return "-"
    
    if len(name) > max_length:
        return name[:max_length-3] + "..."
    
    return name


__all__ = ['field_standardization_node']