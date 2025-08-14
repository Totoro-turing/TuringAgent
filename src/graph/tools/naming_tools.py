"""
命名工具模块

提供属性名称建议和字段名标准化的异步工具
"""

import logging
import re
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from .base import AsyncBaseTool, create_tool_result
from src.graph.utils.naming import (
    batch_standardize_field_names as _batch_standardize,
    standardize_field_name as _standardize_single,
    attribute_name_translation
)

logger = logging.getLogger(__name__)


async def search_knowledge_base(physical_name: str) -> Optional[Dict[str, Any]]:
    """
    搜索知识库获取标准命名
    
    Args:
        physical_name: 物理字段名
    
    Returns:
        匹配结果字典
    """
    try:
        # 这里可以连接到实际的知识库
        # 目前使用模拟数据
        kb_mapping = {
            "invoice_no": {"standard_name": "Invoice Number", "chinese_name": "发票号"},
            "invoice_number": {"standard_name": "Invoice Number", "chinese_name": "发票号"},
            "customer_type": {"standard_name": "Customer Type", "chinese_name": "客户类型"},
            "document_date": {"standard_name": "Document Date", "chinese_name": "文档日期"},
            "total_amount": {"standard_name": "Total Amount", "chinese_name": "总金额"},
            "payment_status": {"standard_name": "Payment Status", "chinese_name": "支付状态"},
            "order_no": {"standard_name": "Order Number", "chinese_name": "订单号"},
            "product_code": {"standard_name": "Product Code", "chinese_name": "产品代码"},
            "ship_addr": {"standard_name": "Shipping Address", "chinese_name": "送货地址"},
            "billing_date": {"standard_name": "Billing Date", "chinese_name": "账单日期"},
            "discount_amt": {"standard_name": "Discount Amount", "chinese_name": "折扣金额"}
        }
        
        # 精确匹配
        if physical_name.lower() in kb_mapping:
            return {
                "exact_match": True,
                "kb_score": 100,
                **kb_mapping[physical_name.lower()]
            }
        
        # 模糊匹配
        for key, value in kb_mapping.items():
            if key in physical_name.lower() or physical_name.lower() in key:
                return {
                    "exact_match": False,
                    "kb_score": 80,
                    **value
                }
        
        return None
        
    except Exception as e:
        logger.error(f"搜索知识库失败: {e}")
        return None


async def evaluate_with_llm(
    physical_name: str,
    current_name: str = "",
    context: str = ""
) -> Dict[str, Any]:
    """
    使用LLM评估属性名称
    
    Args:
        physical_name: 物理字段名
        current_name: 当前属性名
        context: 上下文信息
    
    Returns:
        LLM评估结果
    """
    try:
        from src.agent.edw_agents import get_shared_llm
        from langchain.schema import HumanMessage
        
        # 构建评估提示
        prompt = f"""
评估以下属性命名是否合适：
物理字段名: {physical_name}
当前属性名: {current_name or '未提供'}
上下文: {context or '无'}

请按以下标准评分（0-100）：
1. 清晰性：名称是否清楚表达含义
2. 一致性：是否符合EDW命名规范
3. 业务相关性：是否准确反映业务含义

返回JSON格式：
{{
    "score": 分数,
    "evaluation": "评价说明",
    "recommended_name": "推荐的属性名",
    "suggestions": ["建议1", "建议2"]
}}
"""
        
        llm = get_shared_llm()
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        
        # 解析响应
        import json
        try:
            content = response.content if hasattr(response, 'content') else str(response)
            result = json.loads(content)
            return result
        except:
            # 解析失败时返回默认值
            return {
                "score": 70,
                "evaluation": "LLM评估完成",
                "recommended_name": current_name or _to_pascal_case(physical_name),
                "suggestions": []
            }
            
    except Exception as e:
        logger.error(f"LLM评估失败: {e}")
        return {
            "score": 70,
            "evaluation": "评估失败，使用默认分数",
            "recommended_name": current_name or _to_pascal_case(physical_name),
            "suggestions": []
        }


def _to_pascal_case(name: str) -> str:
    """
    转换为帕斯卡命名法
    
    Args:
        name: 原始名称
    
    Returns:
        帕斯卡命名
    """
    # 处理下划线命名
    if '_' in name:
        parts = name.split('_')
        return ''.join(word.capitalize() for word in parts if word)
    # 处理连字符命名
    elif '-' in name:
        parts = name.split('-')
        return ''.join(word.capitalize() for word in parts if word)
    # 处理驼峰命名（首字母小写）
    elif name and name[0].islower():
        return name[0].upper() + name[1:]
    # 已经是帕斯卡命名或其他情况
    return name


def _check_naming_convention(name: str) -> Dict[str, Any]:
    """
    检查命名规范
    
    Args:
        name: 属性名称
    
    Returns:
        规范检查结果
    """
    score = 100
    issues = []
    
    # 检查是否为帕斯卡命名法
    if not name[0].isupper():
        score -= 10
        issues.append("首字母应大写")
    
    # 检查是否包含下划线
    if '_' in name:
        score -= 20
        issues.append("不应包含下划线")
    
    # 检查是否包含数字开头
    if name[0].isdigit():
        score -= 30
        issues.append("不应以数字开头")
    
    # 检查是否过短或过长
    if len(name) < 3:
        score -= 15
        issues.append("名称过短")
    elif len(name) > 50:
        score -= 10
        issues.append("名称过长")
    
    return {
        "score": max(0, score),
        "issues": issues,
        "is_valid": score >= 80
    }


async def suggest_attribute_names(
    physical_name: str,
    current_name: str = "",
    table_name: str = "",
    context: str = ""
) -> Dict[str, Any]:
    """
    异步生成属性名称建议
    
    Args:
        physical_name: 物理字段名
        current_name: 当前属性名
        table_name: 表名
        context: 上下文信息
    
    Returns:
        建议结果字典
    """
    try:
        logger.info(f"生成属性名称建议: {physical_name}")
        
        # 1. 搜索知识库
        kb_match = await search_knowledge_base(physical_name)
        
        # 2. 检查命名规范
        convention_score = _check_naming_convention(current_name) if current_name else {"score": 0}
        
        # 3. LLM评估
        llm_evaluation = await evaluate_with_llm(physical_name, current_name, context)
        
        # 4. 计算综合评分
        weights = {
            "kb": 0.4,
            "convention": 0.3,
            "llm": 0.3
        }
        
        kb_score = kb_match.get("kb_score", 60) if kb_match else 60
        conv_score = convention_score.get("score", 70)
        llm_score = llm_evaluation.get("score", 70)
        
        final_score = round(
            kb_score * weights["kb"] +
            conv_score * weights["convention"] +
            llm_score * weights["llm"],
            1
        )
        
        # 5. 生成建议列表
        suggestions = []
        
        # 知识库建议
        if kb_match and kb_match.get("standard_name"):
            if kb_match["standard_name"] != current_name:
                suggestions.append({
                    "type": "knowledge_base",
                    "suggested_name": kb_match["standard_name"],
                    "reason": f"EDW标准命名（{kb_match.get('chinese_name', '')}）",
                    "confidence": 0.95 if kb_match.get("exact_match") else 0.7
                })
        
        # 命名规范建议
        if convention_score["score"] < 90:
            pascal_name = _to_pascal_case(physical_name)
            if pascal_name != current_name:
                suggestions.append({
                    "type": "convention",
                    "suggested_name": pascal_name,
                    "reason": "转换为帕斯卡命名法",
                    "confidence": 0.8
                })
        
        # LLM建议
        if llm_evaluation.get("recommended_name") and llm_evaluation["recommended_name"] != current_name:
            suggestions.append({
                "type": "llm",
                "suggested_name": llm_evaluation["recommended_name"],
                "reason": llm_evaluation.get("evaluation", "AI推荐"),
                "confidence": 0.85
            })
        
        # 去重并排序
        seen_names = set()
        unique_suggestions = []
        for sugg in sorted(suggestions, key=lambda x: x["confidence"], reverse=True):
            if sugg["suggested_name"] not in seen_names:
                seen_names.add(sugg["suggested_name"])
                unique_suggestions.append(sugg)
        
        # 生成反馈信息
        if final_score >= 90:
            feedback = "属性命名优秀，完全符合EDW标准"
        elif final_score >= 80:
            feedback = "属性命名良好，略有改进空间"
        elif final_score >= 70:
            feedback = "属性命名合格，建议参考改进建议"
        else:
            feedback = "属性命名需要改进，请参考建议"
        
        return create_tool_result(
            True,
            result={
                "score": final_score,
                "feedback": feedback,
                "suggestions": unique_suggestions[:3],  # 最多返回3个建议
                "kb_match": kb_match,
                "convention_check": convention_score,
                "llm_evaluation": llm_evaluation
            },
            physical_name=physical_name,
            current_name=current_name,
            final_score=final_score
        )
        
    except Exception as e:
        error_msg = f"生成属性名称建议失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


async def batch_standardize_field_names(fields: List[Dict]) -> List[Dict[str, Any]]:
    """
    批量标准化字段名称（异步包装）
    
    Args:
        fields: 字段列表
    
    Returns:
        标准化结果列表
    """
    try:
        logger.info(f"批量标准化 {len(fields)} 个字段")
        
        # 调用已有的异步函数
        results = await _batch_standardize(fields)
        
        return results
        
    except Exception as e:
        error_msg = f"批量标准化失败: {str(e)}"
        logger.error(error_msg)
        return []


async def evaluate_attribute_name(
    name: str,
    physical_name: str = "",
    context: str = ""
) -> Dict[str, Any]:
    """
    评估属性名称质量
    
    Args:
        name: 属性名称
        physical_name: 物理字段名
        context: 上下文
    
    Returns:
        评估结果
    """
    try:
        # 命名规范检查
        convention_check = _check_naming_convention(name)
        
        # 知识库匹配
        kb_match = await search_knowledge_base(physical_name) if physical_name else None
        
        # LLM评估
        llm_eval = await evaluate_with_llm(physical_name, name, context)
        
        # 综合评分
        score = (convention_check["score"] * 0.4 +
                (kb_match.get("kb_score", 60) if kb_match else 60) * 0.3 +
                llm_eval.get("score", 70) * 0.3)
        
        return create_tool_result(
            True,
            result={
                "score": round(score, 1),
                "convention_check": convention_check,
                "kb_match": kb_match,
                "llm_evaluation": llm_eval,
                "is_valid": score >= 70
            }
        )
        
    except Exception as e:
        error_msg = f"评估属性名称失败: {str(e)}"
        logger.error(error_msg)
        return create_tool_result(False, error=error_msg)


class NamingSuggestionInput(BaseModel):
    """命名建议工具的输入参数"""
    physical_name: str = Field(description="物理字段名")
    current_name: Optional[str] = Field(default="", description="当前属性名")
    table_name: Optional[str] = Field(default="", description="表名")
    context: Optional[str] = Field(default="", description="上下文信息")


class NamingSuggestionTool(AsyncBaseTool):
    """
    属性名称建议工具
    
    提供属性命名建议和评分
    """
    name: str = "suggest_attribute_names"
    description: str = "为物理字段名提供属性名称建议"
    args_schema: type[BaseModel] = NamingSuggestionInput
    
    async def _arun(
        self,
        physical_name: str,
        current_name: str = "",
        table_name: str = "",
        context: str = "",
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行命名建议"""
        result = await suggest_attribute_names(
            physical_name=physical_name,
            current_name=current_name,
            table_name=table_name,
            context=context
        )
        
        if result["success"]:
            suggestions = result.get("result", {}).get("suggestions", [])
            score = result.get("result", {}).get("score", 0)
            
            if suggestions:
                sugg_text = "\n".join([
                    f"- {s['suggested_name']}: {s['reason']}"
                    for s in suggestions
                ])
                return f"评分: {score}\n建议:\n{sugg_text}"
            else:
                return f"评分: {score}\n当前命名良好，无需修改"
        else:
            return f"生成建议失败: {result.get('error', '未知错误')}"


class FieldStandardizationInput(BaseModel):
    """字段标准化工具的输入参数"""
    fields: List[Dict[str, str]] = Field(
        description="需要标准化的字段列表。每个字段是一个字典，必须包含attribute_name键。"
                   "格式：[{'attribute_name': 'Finance Invoice Number', 'physical_name': ''}, {'attribute_name': 'Finance Invoice Item Numbger', 'physical_name': ''}]"
    )


class FieldStandardizationTool(AsyncBaseTool):
    """
    字段名标准化工具
    
    批量将属性名转换为标准物理字段名
    """
    name: str = "standardize_field_names"
    description: str = (
        "批量标准化属性名称为物理字段名。"
        "参数fields是字典列表，每个字典必须包含attribute_name键。"
        "示例：fields=[{'attribute_name': 'Finance Invoice Number'}, {'attribute_name': 'Finance Invoice Item Numbger', 'physical_name': 'create_dt'}]"
    )
    args_schema: type[BaseModel] = FieldStandardizationInput
    
    async def _arun(
        self,
        fields: List[Dict],
        run_manager: Optional[Any] = None
    ) -> str:
        """异步执行字段标准化"""
        results = await batch_standardize_field_names(fields)
        
        if results:
            standardized_count = sum(
                1 for r in results
                if r.get("standard_physical_name") != r.get("user_physical_name")
            )
            return f"成功标准化 {len(results)} 个字段，其中 {standardized_count} 个字段被修改"
        else:
            return "字段标准化失败"