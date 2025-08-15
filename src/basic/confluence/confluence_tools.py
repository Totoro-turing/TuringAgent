"""
Confluence集成工具

为工作流提供Confluence页面创建和信息收集功能
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime
from .confluence_operate import ConfluenceManager

logger = logging.getLogger(__name__)


class ConfluenceWorkflowTools:
    """Confluence工作流集成工具"""
    
    def __init__(self):
        """初始化Confluence工具"""
        # 从环境变量读取配置信息
        self.confluence_url = os.getenv("CONFLUENCE_URL", "https://km.xpaas.lenovo.com/")
        self.username = os.getenv("CONFLUENCE_USERNAME", "longyu3")
        self.api_token = os.getenv("CONFLUENCE_API_TOKEN")
        self.target_space_name = os.getenv("CONFLUENCE_TARGET_SPACE_NAME", "EDW Delivery Knowledge Center")
        
        # 验证必需的配置
        if not self.api_token:
            logger.error("CONFLUENCE_API_TOKEN 环境变量未设置")
            raise ValueError("CONFLUENCE_API_TOKEN 环境变量未设置")
        
        # 页面路径配置
        self.page_paths = {
            "finance": [
                "EDW Data Modeling",
                "Model Review Process & Review Log",
                "Solution Model Review Log",
                "Finance Solution Model"
            ],
            "hr": [
                "EDW Data Modeling",
                "Model Review Process & Review Log", 
                "Solution Model Review Log",
                "HR Solution Model"
            ],
            "default": [
                "EDW Data Modeling",
                "Model Review Process & Review Log",
                "Solution Model Review Log",
                "General Solution Model"
            ]
        }
        
        self.confluence_manager = None
    
    def _get_confluence_manager(self) -> ConfluenceManager:
        """获取Confluence管理器实例"""
        if not self.confluence_manager:
            self.confluence_manager = ConfluenceManager(
                self.confluence_url,
                self.username,
                "",
                self.api_token
            )
        return self.confluence_manager
    
    async def collect_model_documentation_info(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        收集模型文档信息
        
        Args:
            context: 工作流上下文，包含table_name, enhanced_code等
            
        Returns:
            收集到的文档信息
        """
        try:
            table_name = context.get("table_name", "")
            enhanced_code = context.get("enhanced_code", "")
            explanation = context.get("explanation", "")
            improvements = context.get("improvements", [])
            alter_sql = context.get("alter_sql", "")
            
            # 验证table_name
            if not table_name:
                logger.error("❌ table_name为空，无法收集文档信息")
                return {"error": "table_name不能为空", "template": "basic_template"}
            
            logger.info(f"🔍 开始收集模型文档信息: {table_name}")
            
            # 解析表名获取schema信息
            schema_info = self._parse_table_name(table_name)
            
            # 优先使用上游传递的字段信息，不再进行二次提取
            fields_from_context = context.get("fields", [])
            if fields_from_context:
                # 使用上游传递的详细字段信息
                field_info = {
                    "existing_fields": [],
                    "new_fields": fields_from_context,
                    "modified_fields": [],
                    "field_summary": {
                        "new_fields_count": len(fields_from_context),
                        "total_estimated": len(fields_from_context),
                        "code_lines": len(enhanced_code.split('\n')) if enhanced_code else 0
                    }
                }
                logger.info(f"✅ 使用上游传递的 {len(fields_from_context)} 个字段信息")
            else:
                # 如果没有上游字段信息，才回退到老的解析方式
                logger.warning("⚠️ 上游没有传递字段信息，使用遗留解析方式")
                field_info = await self._collect_field_information(table_name, enhanced_code, alter_sql)
            
            # 获取模型属性名称
            model_name = context.get("model_name", "")
            
            # 生成文档内容
            doc_info = {
                "title": self._generate_page_title(table_name, explanation, model_name),
                "template": "enhanced_model_template",
                "schema_info": schema_info,
                "field_info": field_info,
                "model_name": model_name,  # 添加模型名称
                "enhancement_details": {
                    "explanation": explanation,
                    "improvements": improvements,
                    "has_new_fields": bool(alter_sql and alter_sql.strip()),
                    "alter_sql": alter_sql
                },
                "metadata": {
                    "created_date": datetime.now().strftime('%Y年%m月%d日'),
                    "model_type": "enhanced",
                    "source_table": table_name,
                    "enhancement_timestamp": datetime.now().isoformat()
                },
                "stakeholders": self._get_model_stakeholders(schema_info["schema"]),
                "review_info": self._generate_review_info(table_name, schema_info["schema"], model_name)
            }
            
            logger.info(f"✅ 模型文档信息收集完成: {table_name}")
            return doc_info
            
        except Exception as e:
            logger.error(f"❌ 收集模型文档信息失败: {e}")
            return {"error": str(e), "template": "basic_template"}
    
    async def create_confluence_page(self, doc_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        创建Confluence页面
        
        Args:
            doc_info: 文档信息
            
        Returns:
            创建结果
        """
        try:
            # 安全获取schema_info
            schema_info = doc_info.get("schema_info")
            if not schema_info:
                # 如果没有schema_info，尝试从model_config获取
                model_config = doc_info.get("model_config", {})
                table_name = model_config.get("table_name", "unknown_table")
                if '.' in table_name:
                    schema = table_name.split('.')[0]
                else:
                    schema = "default"
            else:
                table_name = schema_info.get("table_name", schema_info.get("full_name", "unknown_table"))
                schema = schema_info.get("schema", "default")
            
            logger.info(f"📄 开始创建Confluence页面: {table_name}")
            
            cm = self._get_confluence_manager()
            
            # 1. 查找目标空间
            target_space = cm.find_space_by_name(self.target_space_name)
            if not target_space:
                raise Exception(f"未找到空间: {self.target_space_name}")
            
            space_key = target_space['key']
            
            # 2. 确定页面路径
            page_path = self._get_page_path_for_schema(schema)
            
            # 3. 查找父页面
            parent_page = cm.find_page_by_path(space_key, page_path)
            if not parent_page:
                raise Exception(f"未找到父页面路径: {' -> '.join(page_path)}")
            
            # 4. 生成页面内容
            page_content = self._generate_page_content(doc_info)
            
            # 5. 创建页面
            new_page = cm.create_page(
                space_key=space_key,
                title=doc_info["title"],
                content=page_content,
                parent_id=parent_page['id']
            )
            
            if new_page:
                # 6. 添加标签
                labels = self._generate_page_labels(doc_info)
                cm.add_page_labels(new_page['id'], labels)
                
                # 7. 评论功能已禁用
                logger.info("页面评论功能已暂时禁用")
                
                page_url = f"{self.confluence_url.rstrip('/')}/pages/viewpage.action?pageId={new_page['id']}"
                
                result = {
                    "success": True,
                    "page_id": new_page['id'],
                    "page_title": new_page['title'],
                    "page_url": page_url,
                    "parent_page": parent_page['title'],
                    "space": self.target_space_name,
                    "labels": labels,
                    "creation_time": datetime.now().isoformat()
                }
                
                logger.info(f"✅ Confluence页面创建成功: {table_name} - {new_page['id']}")
                return result
            else:
                raise Exception("页面创建失败")
                
        except Exception as e:
            logger.error(f"❌ 创建Confluence页面失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "table_name": doc_info.get("schema_info", {}).get("table_name", "unknown")
            }
    
    def _parse_table_name(self, table_name: str) -> Dict[str, str]:
        """解析表名获取schema信息"""
        if '.' in table_name:
            schema, table = table_name.split('.', 1)
        else:
            schema = 'default'
            table = table_name
            
        return {
            "table_name": table_name,  # 添加table_name字段
            "full_name": table_name,
            "schema": schema,
            "table": table,
            "display_name": table.replace('_', ' ').title()
        }
    
    async def _collect_field_information(self, table_name: str, enhanced_code: str, alter_sql: str) -> Dict[str, Any]:
        """收集字段信息 - 遗留解析方式（只在上游没有字段信息时使用）"""
        try:
            logger.warning("⚠️ 使用遗留解析方式收集字段信息，可能不准确")
            
            field_info = {
                "existing_fields": [],
                "new_fields": [],
                "modified_fields": [],
                "field_summary": {}
            }
            
            # 从增强代码中提取字段信息（简化处理）
            if enhanced_code:
                # 这里应该解析SQL代码提取字段信息
                # 暂时提供基本信息
                field_info["field_summary"]["total_estimated"] = "待分析"
                field_info["field_summary"]["code_lines"] = len(enhanced_code.split('\n'))
            
            # 从ALTER SQL中提取新增字段
            if alter_sql and alter_sql.strip():
                new_fields = self._parse_alter_sql_fields(alter_sql)
                field_info["new_fields"] = new_fields
                field_info["field_summary"]["new_fields_count"] = len(new_fields)
            
            return field_info
            
        except Exception as e:
            logger.error(f"❌ 收集字段信息失败: {e}")
            return {"error": str(e)}
    
    def _parse_alter_sql_fields(self, alter_sql: str) -> List[Dict[str, str]]:
        """解析ALTER SQL中的新增字段"""
        try:
            new_fields = []
            lines = alter_sql.split('\n')
            
            for line in lines:
                line = line.strip().upper()
                if 'ADD COLUMN' in line or 'ADD' in line:
                    # 简化的字段解析
                    parts = line.split()
                    if len(parts) >= 3:
                        field_name = parts[2].strip('(),;')
                        field_type = parts[3].strip('(),;') if len(parts) > 3 else 'STRING'
                        
                        new_fields.append({
                            "name": field_name,
                            "type": field_type,
                            "source": "ALTER SQL"
                        })
            
            return new_fields
            
        except Exception as e:
            logger.error(f"❌ 解析ALTER SQL字段失败: {e}")
            return []
    
    def _get_page_path_for_schema(self, schema: str) -> List[str]:
        """根据schema获取页面路径"""
        schema_lower = schema.lower()
        
        if 'fi' in schema_lower or 'finance' in schema_lower:
            return self.page_paths["finance"]
        elif 'hr' in schema_lower:
            return self.page_paths["hr"]
        else:
            return self.page_paths["default"]
    
    def _get_model_stakeholders(self, schema: str) -> Dict[str, List[str]]:
        """获取模型相关人员"""
        stakeholder_mapping = {
            "dwd_fi": {
                "reviewers": ["@Tommy ZC1 Tong"],
                "requesters": ["@Daisy Shi", "@Serena XQ7 Sun", "@Xianmei XM2 Chang"],
                "business_owner": "Finance Team",
                "data_owner": "EDW Team"
            },
            "dwd_hr": {
                "reviewers": ["@HR Reviewer"],
                "requesters": ["@HR Requester"],
                "business_owner": "HR Team", 
                "data_owner": "EDW Team"
            },
            "default": {
                "reviewers": ["@EDW Reviewer"],
                "requesters": ["@EDW Requester"],
                "business_owner": "Business Team",
                "data_owner": "EDW Team"
            }
        }
        
        return stakeholder_mapping.get(schema, stakeholder_mapping["default"])
    
    def _generate_page_title(self, table_name: str, explanation: str, model_name: str = "") -> str:
        """生成页面标题 - 固定格式: 2025-08-14: Finance Data Model Review - 模型属性名称"""
        date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 解析schema信息决定业务域
        if '.' in table_name:
            schema = table_name.split('.')[0].lower()
        else:
            schema = 'default'
            
        # 根据schema确定业务域
        if 'fi' in schema:
            domain = "Finance"
        elif 'hr' in schema:
            domain = "HR"
        elif 'sc' in schema:
            domain = "Supply Chain"
        elif 'mk' in schema:
            domain = "Marketing"
        else:
            domain = "Data"
        
        # 优先使用模型属性名称，如果没有则使用表名
        display_name = model_name if model_name else (
            table_name.split('.', 1)[1] if '.' in table_name else table_name
        )
            
        return f"{date_str}: {domain} Data Model Review - {display_name}"
    
    def _generate_review_info(self, table_name: str, schema: str, model_name: str = "") -> Dict[str, Any]:
        """生成审核信息"""
        stakeholders = self._get_model_stakeholders(schema)
        
        # 生成Entity List：schema+模型属性名称
        if model_name:
            # 优先使用模型属性名称
            entity_list = f"{schema.lower()}.{model_name}"
        elif '.' in table_name:
            schema_part, model_part = table_name.split('.', 1)
            # 使用小写schema + 模型名称
            entity_list = f"{schema_part.lower()}.{model_part}"
        else:
            entity_list = table_name
        
        return {
            "requirement_description": f"对 {table_name} 进行模型增强和优化",
            "entity_list": entity_list,
            "review_requesters": stakeholders["requesters"],
            "reviewer_mandatory": stakeholders["reviewers"][0] if stakeholders["reviewers"] else "@EDW Reviewer",
            "review_date": datetime.now().strftime('%Y年%m月%d日'),
            "business_owner": stakeholders["business_owner"],
            "data_owner": stakeholders["data_owner"]
        }
    
    def _generate_page_content(self, doc_info: Dict[str, Any]) -> str:
        """生成页面内容"""
        try:
            cm = self._get_confluence_manager()
            
            # 生成状态标签
            status_tags = [
                {"title": "ENHANCED", "color": "Green"},
                {"title": "PENDING REVIEW", "color": "Yellow"}
            ]
            
            if doc_info["enhancement_details"]["has_new_fields"]:
                status_tags.append({"title": "NEW FIELDS", "color": "Blue"})
            
            # 构建配置用于页面生成
            model_config = {
                "title": doc_info["title"],
                "requirement_description": doc_info["review_info"]["requirement_description"],
                "entity_list": doc_info["review_info"]["entity_list"],
                "review_requesters": doc_info["review_info"]["review_requesters"],
                "reviewer_mandatory": doc_info["review_info"]["reviewer_mandatory"],
                "review_date": doc_info["review_info"]["review_date"],
                "status_tags": status_tags,
                "dataflow": {
                    "source": f"Original {doc_info['schema_info'].get('table_name', doc_info['schema_info'].get('full_name', 'unknown'))}",
                    "target": f"Enhanced {doc_info['schema_info'].get('table_name', doc_info['schema_info'].get('full_name', 'unknown'))}"
                },
                "model_fields": self._format_fields_for_confluence(doc_info["field_info"], doc_info["schema_info"]["schema"], doc_info.get("model_name", ""), doc_info["schema_info"]["table_name"]),
                "enhancement_summary": doc_info["enhancement_details"]["explanation"],
                "improvements": doc_info["enhancement_details"]["improvements"],
                "new_fields_info": doc_info["field_info"].get("new_fields", [])
            }
            
            # 使用现有的方法生成内容
            content = cm._build_data_model_content(model_config)
            
            # 添加增强特定的部分
            enhancement_section = self._build_enhancement_section(doc_info)
            content += enhancement_section
            
            return content
            
        except Exception as e:
            logger.error(f"❌ 生成页面内容失败: {e}")
            return f"<p>生成页面内容失败: {str(e)}</p>"
    
    def _format_fields_for_confluence(self, field_info: Dict[str, Any], schema: str = "default", model_name: str = "", table_name: str = "") -> List[Dict[str, str]]:
        """格式化字段信息用于Confluence表格"""
        formatted_fields = []
        
        # 确定模型名称，如果没有则使用默认值
        display_model_name = model_name if model_name else "Enhanced Model"
        
        # 新增字段
        for field in field_info.get("new_fields", []):
            # 获取字段信息 - 从state中fields正确获取
            physical_name = field.get("physical_name", field.get("name", ""))  # 字段物理名（保持原样）
            attribute_name = field.get("attribute_name", "")  # 字段属性名称（英文）
            data_type = field.get("data_type", field.get("type", "string"))  # 数据类型
            
            # 如果没有attribute_name，使用physical_name作为备选
            if not attribute_name:
                attribute_name = physical_name
            
            formatted_fields.append({
                "schema": schema,  # 使用实际的数据库schema
                "mode_name": display_model_name,  # 使用模型名称
                "table_name": table_name.lower() if table_name else "unknown",  # 使用实际表名（小写）
                "attribute_name": attribute_name,  # 字段的属性名称（英文）
                "column_name": physical_name,  # 字段的物理名称
                "column_type": data_type,  # 数据类型
                "pk": "N"
            })
        
        return formatted_fields
    
    def _build_enhancement_section(self, doc_info: Dict[str, Any]) -> str:
        """构建增强特定的页面部分"""
        try:
            cm = self._get_confluence_manager()
            enhancement_details = doc_info["enhancement_details"]
            
            sections = []
            
            # 增强说明部分
            sections.append("<h2>增强说明</h2>")
            sections.append(f"<p>{enhancement_details['explanation']}</p>")
            
            # 改进点
            if enhancement_details["improvements"]:
                sections.append("<h3>主要改进点</h3>")
                sections.append("<ul>")
                for improvement in enhancement_details["improvements"]:
                    sections.append(f"<li>{improvement}</li>")
                sections.append("</ul>")
            
            # 新增字段信息
            if enhancement_details["has_new_fields"]:
                sections.append("<h3>新增字段</h3>")
                alter_sql = enhancement_details.get("alter_sql", "")
                if alter_sql:
                    sections.append("<h4>DDL语句</h4>")
                    sections.append(f"<pre><code>{alter_sql}</code></pre>")
                
                new_fields = doc_info["field_info"].get("new_fields", [])
                if new_fields:
                    sections.append("<h4>新增字段列表</h4>")
                    headers = ["物理字段名", "属性名称", "数据类型", "说明"]
                    rows = []
                    for field in new_fields:
                        rows.append([
                            field.get("physical_name", field.get("name", "")),
                            field.get("attribute_name", ""),
                            field.get("data_type", field.get("type", "")),
                            field.get("comment", "新增字段")
                        ])
                    
                    field_table = cm.create_table_from_data(headers, rows)
                    sections.append(field_table)
            
            # 技术信息
            sections.append("<h2>技术信息</h2>")
            metadata = doc_info["metadata"]
            tech_info = cm.create_info_macro(
                f"增强时间: {metadata['enhancement_timestamp']}<br/>"
                f"模型类型: {metadata['model_type']}<br/>"
                f"源表: {metadata['source_table']}",
                "info"
            )
            sections.append(tech_info)
            
            return "\n".join(sections)
            
        except Exception as e:
            logger.error(f"❌ 构建增强部分失败: {e}")
            return f"<p>构建增强部分失败: {str(e)}</p>"
    
    def _generate_page_labels(self, doc_info: Dict[str, Any]) -> List[str]:
        """生成页面标签"""
        labels = ['EDW', 'Enhanced-Model', 'Auto-Generated']
        
        schema = doc_info["schema_info"]["schema"]
        labels.append(schema)
        
        if doc_info["enhancement_details"]["has_new_fields"]:
            labels.append('New-Fields')
        
        if 'fi' in schema.lower():
            labels.extend(['Finance', 'Financial-Model'])
        elif 'hr' in schema.lower():
            labels.extend(['HR', 'Human-Resources'])
        
        return labels
    
    def _generate_page_comment(self, doc_info: Dict[str, Any]) -> str:
        """生成页面评论"""
        stakeholders = doc_info["stakeholders"]
        reviewers = " ".join(stakeholders["reviewers"])
        requesters = " ".join(stakeholders["requesters"])
        
        comment = (
            f"模型增强文档已自动创建完成。"
            f"请相关审核人员({reviewers})和申请人员({requesters})进行审核确认。\n\n"
            f"增强内容: {doc_info['enhancement_details']['explanation'][:100]}...\n"
            f"创建时间: {doc_info['metadata']['created_date']}"
        )
        
        return comment


# 工具函数
async def create_confluence_documentation(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    创建Confluence文档的工具函数
    
    Args:
        context: 工作流上下文
        
    Returns:
        创建结果
    """
    try:
        tools = ConfluenceWorkflowTools()
        
        # 1. 收集文档信息
        doc_info = await tools.collect_model_documentation_info(context)
        
        if "error" in doc_info:
            return {"success": False, "error": doc_info["error"]}
        
        # 2. 创建页面
        result = await tools.create_confluence_page(doc_info)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 创建Confluence文档失败: {e}")
        return {"success": False, "error": str(e)}