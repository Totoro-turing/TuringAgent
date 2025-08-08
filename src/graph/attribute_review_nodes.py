"""
属性名称Review节点模块
实现属性命名规范检查和优化建议
结合EDW知识库和大模型评估
"""

import logging
import yaml
import os
import re
from typing import Dict, Any, List, Optional, Tuple
from langchain.schema.messages import HumanMessage, AIMessage
from langgraph.types import interrupt
from langgraph.graph import StateGraph, START, END

from src.models.states import EDWState
from src.agent.edw_agents import get_shared_llm

logger = logging.getLogger(__name__)


class AttributeNameReviewer:
    """属性名称评审器"""
    
    def __init__(self):
        """初始化知识库"""
        self.knowledge_base = self._load_knowledge_base()
        self.llm = get_shared_llm()
    
    def _load_knowledge_base(self) -> dict:
        """加载EDW属性名称知识库"""
        try:
            knowledge_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'config', 'edw_attribute_knowledge.yaml'
            )
            
            with open(knowledge_path, 'r', encoding='utf-8') as f:
                knowledge = yaml.safe_load(f)
            
            logger.info(f"成功加载EDW属性名称知识库，包含 {self._count_attributes(knowledge)} 个标准属性")
            return knowledge
        except Exception as e:
            logger.error(f"加载知识库失败: {e}")
            return {
                "naming_conventions": {},
                "common_attributes": {},
                "scoring_rules": {},
                "suggestion_rules": []
            }
    
    def _count_attributes(self, knowledge: dict) -> int:
        """统计知识库中的属性数量"""
        count = 0
        for category in knowledge.get("common_attributes", {}).values():
            if isinstance(category, list):
                count += len(category)
            elif isinstance(category, dict):
                for items in category.values():
                    if isinstance(items, list):
                        count += len(items)
        return count
    
    def review_attribute_name(self, physical_name: str, attribute_name: str, 
                             context: Optional[str] = None) -> dict:
        """
        评审单个属性名称
        
        Returns:
            dict: 包含score、feedback、suggestions等
        """
        # 1. 知识库匹配
        kb_match = self._match_knowledge_base(physical_name, attribute_name)
        
        # 2. 命名规范检查
        convention_score = self._check_naming_convention(attribute_name)
        
        # 3. 使用LLM深度评估
        llm_evaluation = self._llm_evaluate(physical_name, attribute_name, context, kb_match)
        
        # 4. 综合评分
        final_score = self._calculate_final_score(kb_match, convention_score, llm_evaluation)
        
        # 5. 生成建议
        suggestions = self._generate_suggestions(
            physical_name, attribute_name, kb_match, convention_score, llm_evaluation
        )
        
        return {
            "physical_name": physical_name,
            "current_attribute_name": attribute_name,
            "score": final_score,
            "kb_match": kb_match,
            "convention_score": convention_score,
            "llm_evaluation": llm_evaluation,
            "suggestions": suggestions,
            "feedback": self._generate_feedback(final_score, kb_match, convention_score)
        }
    
    def _match_knowledge_base(self, physical_name: str, attribute_name: str) -> Optional[dict]:
        """在知识库中匹配属性"""
        physical_lower = physical_name.lower()
        
        # 遍历所有类别
        for category_name, category_data in self.knowledge_base.get("common_attributes", {}).items():
            if isinstance(category_data, dict):
                # 处理嵌套结构（如finance下的invoice、payment等）
                for subcategory, items in category_data.items():
                    if isinstance(items, list):
                        for item in items:
                            if item.get("physical", "").lower() == physical_lower:
                                return {
                                    "category": f"{category_name}/{subcategory}",
                                    "standard_name": item.get("standard"),
                                    "chinese_name": item.get("chinese"),
                                    "kb_score": item.get("score", 90),
                                    "exact_match": True
                                }
            elif isinstance(category_data, list):
                # 处理直接列表结构
                for item in category_data:
                    if item.get("physical", "").lower() == physical_lower:
                        return {
                            "category": category_name,
                            "standard_name": item.get("standard"),
                            "chinese_name": item.get("chinese"),
                            "kb_score": item.get("score", 90),
                            "exact_match": True
                        }
        
        # 模糊匹配
        return self._fuzzy_match_knowledge_base(physical_name, attribute_name)
    
    def _fuzzy_match_knowledge_base(self, physical_name: str, attribute_name: str) -> Optional[dict]:
        """模糊匹配知识库"""
        physical_parts = set(physical_name.lower().split('_'))
        best_match = None
        best_score = 0
        
        for category_name, category_data in self.knowledge_base.get("common_attributes", {}).items():
            items_to_check = []
            
            if isinstance(category_data, dict):
                for subcategory, items in category_data.items():
                    if isinstance(items, list):
                        items_to_check.extend([(f"{category_name}/{subcategory}", item) for item in items])
            elif isinstance(category_data, list):
                items_to_check.extend([(category_name, item) for item in category_data])
            
            for category_path, item in items_to_check:
                item_parts = set(item.get("physical", "").lower().split('_'))
                overlap = len(physical_parts & item_parts)
                if overlap > 0:
                    score = overlap / max(len(physical_parts), len(item_parts))
                    if score > best_score:
                        best_score = score
                        best_match = {
                            "category": category_path,
                            "standard_name": item.get("standard"),
                            "chinese_name": item.get("chinese"),
                            "kb_score": item.get("score", 90) * score,
                            "exact_match": False,
                            "similarity": score
                        }
        
        return best_match if best_score > 0.5 else None
    
    def _check_naming_convention(self, attribute_name: str) -> dict:
        """检查命名规范"""
        score = 100
        issues = []
        
        # 检查是否是帕斯卡命名法
        if re.match(r'^[A-Z][a-zA-Z0-9]*$', attribute_name):
            # 检查每个单词首字母是否大写
            if re.match(r'^([A-Z][a-z0-9]*)+$', attribute_name):
                score = 100
            else:
                score = 90
                issues.append("不完全符合帕斯卡命名法")
        # 检查是否是驼峰命名法
        elif re.match(r'^[a-z][a-zA-Z0-9]*$', attribute_name):
            score = 80
            issues.append("使用了驼峰命名法而非帕斯卡命名法")
        # 检查是否包含下划线
        elif '_' in attribute_name:
            score = 60
            issues.append("包含下划线，应使用帕斯卡命名法")
        # 检查是否包含连字符
        elif '-' in attribute_name:
            score = 50
            issues.append("包含连字符，应使用帕斯卡命名法")
        else:
            score = 70
            issues.append("命名格式不规范")
        
        # 检查长度
        if len(attribute_name) > 30:
            score -= 5
            issues.append("名称过长")
        elif len(attribute_name) < 3:
            score -= 10
            issues.append("名称过短")
        
        # 检查是否使用缩写
        common_abbreviations = ['no', 'num', 'id', 'cd', 'dt', 'amt', 'qty', 'desc']
        for abbr in common_abbreviations:
            if abbr in attribute_name.lower():
                score -= 5
                issues.append(f"可能使用了缩写: {abbr}")
                break
        
        return {
            "score": max(score, 0),
            "issues": issues
        }
    
    def _llm_evaluate(self, physical_name: str, attribute_name: str, 
                     context: Optional[str], kb_match: Optional[dict]) -> dict:
        """使用LLM评估属性名称"""
        try:
            prompt = f"""你是一个EDW（企业数据仓库）属性命名专家。请评估以下属性名称的质量。

物理字段名: {physical_name}
当前属性名: {attribute_name}
业务上下文: {context or '未提供'}
知识库匹配: {kb_match.get('standard_name') if kb_match else '无匹配'}

评估标准：
1. 清晰性（20分）：名称是否清晰表达业务含义
2. 规范性（20分）：是否符合EDW命名规范（帕斯卡命名法）
3. 一致性（20分）：是否与EDW标准保持一致
4. 准确性（20分）：是否准确反映字段用途
5. 简洁性（20分）：是否简洁而不失完整

请给出：
1. 总分（0-100）
2. 简短评价
3. 改进建议（如有）

输出JSON格式：
{{
    "score": 分数,
    "evaluation": "评价",
    "suggestions": ["建议1", "建议2"],
    "recommended_name": "推荐的属性名称"
}}"""
            
            response = self.llm.invoke(prompt)
            content = response.content if hasattr(response, 'content') else str(response)
            
            # 解析LLM响应
            import json
            try:
                result = json.loads(content)
                return result
            except:
                # 如果解析失败，尝试提取关键信息
                score_match = re.search(r'"score":\s*(\d+)', content)
                score = int(score_match.group(1)) if score_match else 70
                
                return {
                    "score": score,
                    "evaluation": "LLM评估完成",
                    "suggestions": [],
                    "recommended_name": attribute_name
                }
                
        except Exception as e:
            logger.error(f"LLM评估失败: {e}")
            return {
                "score": 70,
                "evaluation": "评估失败，使用默认分数",
                "suggestions": [],
                "recommended_name": attribute_name
            }
    
    def _calculate_final_score(self, kb_match: Optional[dict], 
                               convention_score: dict, llm_evaluation: dict) -> float:
        """计算最终评分"""
        # 权重分配
        weights = {
            "kb": 0.4,      # 知识库匹配权重
            "convention": 0.3,  # 命名规范权重
            "llm": 0.3      # LLM评估权重
        }
        
        kb_score = kb_match.get("kb_score", 60) if kb_match else 60
        conv_score = convention_score.get("score", 70)
        llm_score = llm_evaluation.get("score", 70)
        
        final_score = (
            kb_score * weights["kb"] +
            conv_score * weights["convention"] +
            llm_score * weights["llm"]
        )
        
        return round(final_score, 1)
    
    def _generate_suggestions(self, physical_name: str, attribute_name: str,
                             kb_match: Optional[dict], convention_score: dict,
                             llm_evaluation: dict) -> List[dict]:
        """生成改进建议"""
        suggestions = []
        
        # 1. 知识库建议
        if kb_match and kb_match.get("standard_name"):
            if kb_match["standard_name"] != attribute_name:
                suggestions.append({
                    "type": "knowledge_base",
                    "suggested_name": kb_match["standard_name"],
                    "reason": f"EDW标准命名（{kb_match.get('chinese_name', '')}）",
                    "confidence": 0.95 if kb_match.get("exact_match") else 0.7
                })
        
        # 2. 命名规范建议
        if convention_score["score"] < 90:
            # 转换为帕斯卡命名法
            pascal_name = self._to_pascal_case(attribute_name)
            if pascal_name != attribute_name:
                suggestions.append({
                    "type": "convention",
                    "suggested_name": pascal_name,
                    "reason": "转换为帕斯卡命名法",
                    "confidence": 0.8
                })
        
        # 3. LLM建议
        if llm_evaluation.get("recommended_name") and llm_evaluation["recommended_name"] != attribute_name:
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
        
        return unique_suggestions[:3]  # 最多返回3个建议
    
    def _to_pascal_case(self, name: str) -> str:
        """转换为帕斯卡命名法"""
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
    
    def _generate_feedback(self, score: float, kb_match: Optional[dict], 
                          convention_score: dict) -> str:
        """生成反馈信息"""
        if score >= 90:
            return "属性命名优秀，完全符合EDW标准"
        elif score >= 80:
            return "属性命名良好，略有改进空间"
        elif score >= 70:
            return "属性命名合格，建议采纳改进建议"
        elif score >= 60:
            return "属性命名需要改进，请考虑使用推荐的命名"
        else:
            return "属性命名不符合规范，强烈建议修改"


def attribute_review_node(state: EDWState) -> dict:
    """
    属性名称review节点
    评估所有字段的属性命名并给出建议
    """
    try:
        fields = state.get("fields", [])
        table_name = state.get("table_name", "")
        logic_detail = state.get("logic_detail", "")
        user_id = state.get("user_id", "")
        
        if not fields:
            logger.info("没有需要review的字段")
            return {
                "attribute_review_completed": True,
                "user_id": user_id
            }
        
        # 创建评审器
        reviewer = AttributeNameReviewer()
        
        # 评审所有字段
        review_results = []
        total_score = 0
        needs_improvement = []
        
        for field in fields:
            if isinstance(field, dict):
                physical_name = field.get('physical_name', '')
                attribute_name = field.get('attribute_name', '')
            else:
                physical_name = getattr(field, 'physical_name', '')
                attribute_name = getattr(field, 'attribute_name', '')
            
            if physical_name and attribute_name:
                # 评审单个属性
                result = reviewer.review_attribute_name(
                    physical_name=physical_name,
                    attribute_name=attribute_name,
                    context=f"表: {table_name}, 逻辑: {logic_detail}"
                )
                
                review_results.append(result)
                total_score += result["score"]
                
                # 如果评分低于80，需要改进
                if result["score"] < 80 and result["suggestions"]:
                    needs_improvement.append({
                        "field": physical_name,
                        "current": attribute_name,
                        "suggestions": result["suggestions"],
                        "score": result["score"]
                    })
                
                logger.info(f"属性review - {physical_name}: {attribute_name} -> 评分: {result['score']}")
        
        # 计算平均分
        avg_score = total_score / len(review_results) if review_results else 100
        
        # 如果有需要改进的属性，触发中断让用户确认
        if needs_improvement:
            logger.info(f"发现 {len(needs_improvement)} 个属性需要改进，触发中断询问用户")
            
            # 构建改进建议提示
            improvement_prompt = _build_improvement_prompt(needs_improvement, avg_score)
            
            # 使用中断机制让用户确认
            user_response = interrupt({
                "prompt": improvement_prompt,
                "action_type": "attribute_improvement",
                "needs_improvement": needs_improvement
            })
            
            # 处理用户响应
            return _process_user_response(state, user_response, needs_improvement, review_results)
        
        # 所有属性都合格，继续流程
        logger.info(f"所有属性命名合格，平均分: {avg_score}")
        
        return {
            "attribute_review_completed": True,
            "attribute_review_results": review_results,
            "attribute_avg_score": avg_score,
            "user_id": user_id
        }
        
    except Exception as e:
        logger.error(f"属性review节点失败: {e}")
        return {
            "attribute_review_completed": True,
            "error_message": str(e),
            "user_id": state.get("user_id", "")
        }


def _build_improvement_prompt(needs_improvement: List[dict], avg_score: float) -> str:
    """构建改进建议提示"""
    prompt = f"""## 📝 属性命名Review结果

**平均评分**: {avg_score:.1f}/100

发现以下属性命名可以改进：

"""
    
    for item in needs_improvement:
        prompt += f"\n### 字段: {item['field']}\n"
        prompt += f"- **当前名称**: {item['current']} (评分: {item['score']:.1f})\n"
        prompt += f"- **建议改进**:\n"
        
        for i, sugg in enumerate(item['suggestions'][:3], 1):
            prompt += f"  {i}. **{sugg['suggested_name']}** - {sugg['reason']}\n"
    
    prompt += """
**请选择**：
1. 输入 'accept' 或 '接受' - 采纳所有第一推荐
2. 输入 'skip' 或 '跳过' - 保持原有命名
3. 输入自定义选择，格式: field1:2,field2:1 (为每个字段选择第N个建议)
4. 直接输入新的命名，格式: field1:NewName,field2:AnotherName

您的选择："""
    
    return prompt


def _process_user_response(state: EDWState, user_response: str, 
                           needs_improvement: List[dict], 
                           review_results: List[dict]) -> dict:
    """处理用户对属性改进建议的响应"""
    
    user_id = state.get("user_id", "")
    fields = state.get("fields", [])
    
    # 解析用户响应
    response_lower = user_response.lower().strip()
    
    if response_lower in ['accept', '接受', 'yes', '是']:
        # 采纳所有第一推荐
        logger.info("用户选择采纳所有第一推荐")
        updated_fields = _apply_first_suggestions(fields, needs_improvement)
        
        return {
            "fields": updated_fields,
            "attribute_review_completed": True,
            "attribute_improvements_applied": True,
            "attribute_review_results": review_results,
            "user_id": user_id,
            "messages": [AIMessage(content="已采纳所有属性命名改进建议")]
        }
    
    elif response_lower in ['skip', '跳过', 'no', '否']:
        # 保持原有命名
        logger.info("用户选择保持原有命名")
        
        return {
            "attribute_review_completed": True,
            "attribute_improvements_applied": False,
            "attribute_review_results": review_results,
            "user_id": user_id,
            "messages": [AIMessage(content="保持原有属性命名")]
        }
    
    else:
        # 解析自定义选择
        logger.info(f"解析用户自定义选择: {user_response}")
        updated_fields = _apply_custom_selections(fields, needs_improvement, user_response)
        
        return {
            "fields": updated_fields,
            "attribute_review_completed": True,
            "attribute_improvements_applied": True,
            "attribute_review_results": review_results,
            "user_id": user_id,
            "messages": [AIMessage(content=f"已应用自定义属性命名: {user_response}")]
        }


def _apply_first_suggestions(fields: List[dict], needs_improvement: List[dict]) -> List[dict]:
    """应用第一推荐的属性名称"""
    # 创建映射表
    improvement_map = {}
    for item in needs_improvement:
        if item["suggestions"]:
            improvement_map[item["field"]] = item["suggestions"][0]["suggested_name"]
    
    # 更新字段
    updated_fields = []
    for field in fields:
        field_copy = field.copy() if isinstance(field, dict) else field.__dict__.copy()
        
        physical_name = field_copy.get('physical_name', '')
        if physical_name in improvement_map:
            field_copy['attribute_name'] = improvement_map[physical_name]
            logger.info(f"更新属性名称: {physical_name} -> {improvement_map[physical_name]}")
        
        updated_fields.append(field_copy)
    
    return updated_fields


def _apply_custom_selections(fields: List[dict], needs_improvement: List[dict], 
                            user_response: str) -> List[dict]:
    """应用用户自定义选择"""
    # 解析用户输入
    selections = {}
    for part in user_response.split(','):
        part = part.strip()
        if ':' in part:
            field_name, choice = part.split(':', 1)
            field_name = field_name.strip()
            choice = choice.strip()
            
            # 检查是否是数字（选择第N个建议）
            if choice.isdigit():
                choice_idx = int(choice) - 1
                # 查找对应的建议
                for item in needs_improvement:
                    if item["field"] == field_name and 0 <= choice_idx < len(item["suggestions"]):
                        selections[field_name] = item["suggestions"][choice_idx]["suggested_name"]
                        break
            else:
                # 直接使用用户输入的名称
                selections[field_name] = choice
    
    # 更新字段
    updated_fields = []
    for field in fields:
        field_copy = field.copy() if isinstance(field, dict) else field.__dict__.copy()
        
        physical_name = field_copy.get('physical_name', '')
        if physical_name in selections:
            field_copy['attribute_name'] = selections[physical_name]
            logger.info(f"更新属性名称: {physical_name} -> {selections[physical_name]}")
        
        updated_fields.append(field_copy)
    
    return updated_fields


def create_attribute_review_subgraph():
    """
    创建属性名称review子图
    """
    from src.agent.edw_agents import get_shared_checkpointer
    
    logger.info("创建属性名称review子图")
    
    return (
        StateGraph(EDWState)
        .add_node("attribute_review", attribute_review_node)
        .add_edge(START, "attribute_review")
        .add_edge("attribute_review", END)
        .compile(checkpointer=get_shared_checkpointer())
    )