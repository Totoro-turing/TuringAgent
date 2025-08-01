"""
EDW系统数据模型定义
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field


class FieldDefinition(BaseModel):
    """字段定义"""
    physical_name: str = Field(description="字段的物理名称，如：invoice_doc_no")
    attribute_name: str = Field(description="字段的属性名称用于描述物理字段名称，如：Invoice Document Number, 如果用户没有明确指明，置空即可")


class ModelEnhanceRequest(BaseModel):
    """模型增强请求数据模型"""
    table_name: str = Field(description="需要增强的表名，格式如：dwd_fi.fi_invoice_item")
    logic_detail: str = Field(description="具体的增强逻辑描述")
    enhancement_type: Optional[str] = Field(description="增强类型：add_field(添加字段)、modify_logic(修改逻辑)、optimize_query(优化查询)等", default="add_field")
    model_attribute_name: Optional[str] = Field(description="模型属性名称（必须是英文，如：Finance Invoice Header）", default="")
    business_purpose: Optional[str] = Field(description="业务用途描述", default="")
    field_info: Optional[str] = Field(description="字段信息的文本描述（用于向后兼容）", default="")
    fields: Optional[List[FieldDefinition]] = Field(description="新增字段列表", default_factory=list)
    business_requirement: Optional[str] = Field(description="业务需求背景", default="")

    def validate_completeness(self) -> tuple[bool, list[str]]:
        """验证信息完整性，返回(是否完整, 缺失信息描述列表)"""
        missing_info = []

        # 基础字段验证
        if not self.table_name.strip() or self.table_name.strip() == "信息不完整":
            missing_info.append("表名")

        if not self.logic_detail.strip() or self.logic_detail.strip() == "信息不完整":
            missing_info.append("增强逻辑描述")

        # 如果是添加字段类型，需要额外验证
        if self.enhancement_type == "add_field" or any(keyword in self.logic_detail for keyword in ["增加字段", "新增字段", "添加字段"]):
            # 检查是否有字段定义
            if not self.fields or len(self.fields) == 0:
                missing_info.append("字段定义（至少需要提供一个字段的物理名称和属性名称）")
            else:
                # 检查每个字段的完整性
                for i, field in enumerate(self.fields):
                    if not field.physical_name.strip():
                        missing_info.append(f"第{i + 1}个字段的物理名称（请提供字段的英文名称）")
                    if not field.attribute_name.strip():
                        # 如果有物理名称，在提示中包含它
                        if field.physical_name.strip():
                            missing_info.append(f"字段 '{field.physical_name}' 的属性名称（英文描述）")
                        else:
                            missing_info.append(f"第{i + 1}个字段的属性名称")

        return len(missing_info) == 0, missing_info


class RefinementIntentAnalysis(BaseModel):
    """微调意图识别分析结果模型"""
    intent: Literal["REFINEMENT_NEEDED", "SATISFIED_CONTINUE", "UNRELATED_TOPIC"] = Field(
        description="用户意图分类"
    )
    confidence_score: float = Field(
        description="意图识别置信度 (0.0-1.0)",
        ge=0.0,
        le=1.0
    )
    reasoning: str = Field(
        description="详细说明分析推理过程"
    )
    extracted_requirements: Optional[str] = Field(
        description="如果是REFINEMENT_NEEDED，提取用户的具体需求和期望",
        default=""
    )
    user_emotion: Literal["positive", "neutral", "negative", "uncertain"] = Field(
        description="用户情感倾向",
        default="neutral"
    )
    suggested_response: Optional[str] = Field(
        description="建议对用户的回复内容",
        default=""
    )
