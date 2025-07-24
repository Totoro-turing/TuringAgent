"""
Confluence API 操作完整指南 - EDW专用版
使用 atlassian-python-api 库与 Confluence 进行交互
专门用于在指定页面层次结构下创建数据模型页面
"""

from atlassian import Confluence
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple


class ConfluenceManager:
    def __init__(self, url, username, password, token=None):
        """
        初始化 Confluence 连接

        Args:
            url: Confluence 实例 URL (如 https://your-domain.atlassian.net)
            username: 用户名或邮箱
            password: 密码
            token: API token (推荐使用，比密码更安全)
        """
        if token:
            # 使用 API token 认证 (推荐)
            self.confluence = Confluence(
                url=url,
                username=username,
                token=token,
                cloud=True  # 如果是 Confluence Cloud
            )
        else:
            # 使用密码认证
            self.confluence = Confluence(
                url=url,
                username=username,
                password=password,
                cloud=True
            )

    def get_spaces(self):
        """获取所有空间"""
        try:
            spaces = self.confluence.get_all_spaces(start=0, limit=50)
            print("可用空间：")
            for space in spaces['results']:
                print(f"- {space['name']} (Key: {space['key']})")
            return spaces
        except Exception as e:
            print(f"获取空间失败: {e}")
            return None

    def find_space_by_name(self, space_name: str) -> Optional[Dict]:
        """
        根据空间名称查找空间

        Args:
            space_name: 空间名称

        Returns:
            空间信息字典或None
        """
        try:
            spaces = self.confluence.get_all_spaces(start=0, limit=100)

            for space in spaces['results']:
                if space['name'] == space_name:
                    print(f"找到空间: {space['name']} (Key: {space['key']})")
                    return space

            print(f"未找到名称为 '{space_name}' 的空间")
            print("可用空间列表：")
            for space in spaces['results']:
                print(f"  - {space['name']}")
            return None

        except Exception as e:
            print(f"查找空间失败: {e}")
            return None

    def get_pages_in_space(self, space_key, limit=50):
        """获取空间中的所有页面"""
        try:
            pages = self.confluence.get_all_pages_from_space(
                space=space_key,
                start=0,
                limit=limit,
                expand='version,body.storage'
            )
            print(f"空间 {space_key} 中的页面：")
            for page in pages:
                print(f"- {page['title']} (ID: {page['id']})")
            return pages
        except Exception as e:
            print(f"获取页面失败: {e}")
            return None

    def get_page_by_title(self, space_key, title):
        """根据标题获取页面"""
        try:
            page = self.confluence.get_page_by_title(
                space=space_key,
                title=title,
                expand='body.storage,version'
            )
            if page:
                print(f"找到页面: {page['title']} (ID: {page['id']})")
                return page
            else:
                print(f"未找到标题为 '{title}' 的页面")
                return None
        except Exception as e:
            print(f"获取页面失败: {e}")
            return None

    def find_page_by_path(self, space_key: str, page_path: List[str]) -> Optional[Dict]:
        """
        根据页面路径查找页面

        Args:
            space_key: 空间键
            page_path: 页面路径列表，从根页面到目标页面

        Returns:
            页面信息字典或None

        Example:
            page_path = ["EDW Data Modeling", "Model Review Process & Review Log",
                        "Solution Model Review Log", "Finance Solution Model"]
        """
        try:
            current_parent_id = None
            current_page = None

            print(f"开始查找页面路径: {' -> '.join(page_path)}")

            for i, page_title in enumerate(page_path):
                print(f"  查找第 {i+1} 级页面: {page_title}")

                if current_parent_id is None:
                    # 查找根页面
                    current_page = self.get_page_by_title(space_key, page_title)
                else:
                    # 查找子页面
                    current_page = self.find_child_page_by_title(current_parent_id, page_title)

                if not current_page:
                    print(f"    ✗ 未找到页面: {page_title}")
                    print(f"    路径断开位置: 第{i+1}级页面 '{page_title}'")
                    
                    # 提供诊断信息
                    if current_parent_id:
                        print(f"    父页面ID: {current_parent_id}")
                        print(f"    建议: 请检查页面 '{page_title}' 是否存在于父页面下")
                    else:
                        print(f"    建议: 请检查根页面 '{page_title}' 是否存在于空间中")
                    
                    print(f"    完整预期路径: {' -> '.join(page_path)}")
                    print(f"    已成功路径: {' -> '.join(page_path[:i])}")
                    return None

                print(f"    找到页面: {current_page['title']} (ID: {current_page['id']})")
                current_parent_id = current_page['id']

            print(f"成功找到目标页面: {current_page['title']}")
            return current_page

        except Exception as e:
            error_msg = f"查找页面路径失败: {str(e)}"
            print(error_msg)
            print(f"失败时的状态:")
            print(f"  - 当前路径进度: {i+1}/{len(page_path)} (正在查找: '{page_title}')")
            print(f"  - 当前父页面ID: {current_parent_id}")
            print(f"  - 完整路径: {' -> '.join(page_path)}")
            return None

    def find_child_page_by_title(self, parent_id: str, title: str) -> Optional[Dict]:
        """
        在指定父页面下查找子页面

        Args:
            parent_id: 父页面ID
            title: 子页面标题

        Returns:
            页面信息字典或None
        """
        try:
            print(f"    正在查找父页面({parent_id})下的子页面: '{title}'")
            
            # 获取子页面列表
            children = self.confluence.get_page_child_by_type(
                parent_id,
                type='page',
                start=0,
                limit=50,
                expand='version,body.storage'
            )

            print(f"    父页面下共有 {len(children)} 个子页面:")
            for i, child in enumerate(children):
                child_title = child.get('title', '未知标题')
                print(f"      {i+1}. '{child_title}' (ID: {child.get('id', '未知ID')})")
                
                # 精确匹配
                if child_title == title:
                    print(f"    ✓ 找到匹配页面: '{child_title}'")
                    return child

            print(f"    ✗ 未找到匹配的子页面: '{title}'")
            return None

        except Exception as e:
            error_msg = f"查找子页面失败: {str(e)}"
            print(error_msg)
            print(f"    查找参数: parent_id={parent_id}, title='{title}'")
            return None

    def get_page_children(self, page_id: str) -> List[Dict]:
        """
        获取页面的所有子页面

        Args:
            page_id: 页面ID

        Returns:
            子页面列表
        """
        try:
            children = self.confluence.get_page_child_by_type(
                page_id,
                type='page',
                start=0,
                limit=50,
                expand='version,body.storage'
            )

            print(f"页面子页面列表:")
            for child in children:
                print(f"  - {child['title']} (ID: {child['id']})")

            return children

        except Exception as e:
            print(f"获取子页面失败: {e}")
            return []

    def _validate_title(self, title: str) -> tuple[bool, str]:
        """
        验证页面标题基本要求（系统生成的标题一般都是可靠的）
        
        Returns:
            (是否有效, 错误信息或建议)
        """
        if not title or not title.strip():
            return False, "标题不能为空"
        
        # 只检查长度限制作为安全措施
        if len(title) > 255:
            return False, f"标题过长 ({len(title)} 字符)，Confluence限制为255字符"
        
        return True, ""

    def create_page(self, space_key, title, content, parent_id=None):
        """
        创建新页面 - 修复版本

        Args:
            space_key: 空间键
            title: 页面标题
            content: 页面内容 (HTML 格式)
            parent_id: 父页面 ID (可选)
        """
        try:
            # 验证标题
            is_valid, validation_error = self._validate_title(title)
            if not is_valid:
                print(f"标题验证失败: {validation_error}")
                return None
            
            # 检查页面是否已存在
            if parent_id:
                # 在父页面下检查
                existing_page = self.find_child_page_by_title(parent_id, title)
                if existing_page:
                    print(f"页面 '{title}' 已存在于父页面下")
                    return existing_page
            else:
                # 在空间根部检查
                existing_page = self.confluence.get_page_by_title(space_key, title)
                if existing_page:
                    print(f"页面 '{title}' 已存在")
                    return existing_page

            # 使用正确的 API 方法创建页面
            if parent_id:
                # 创建子页面 - 使用 create_page 方法的正确参数
                result = self.confluence.create_page(
                    space=space_key,
                    title=title,
                    body=content,
                    parent_id=parent_id,
                    type='page',
                    representation='storage'
                )
            else:
                # 创建根页面
                result = self.confluence.create_page(
                    space=space_key,
                    title=title,
                    body=content,
                    type='page',
                    representation='storage'
                )

            print(f"页面创建成功: {result['title']} (ID: {result['id']})")
            return result

        except Exception as e:
            error_details = f"创建页面失败: {str(e)}"
            print(error_details)
            print(f"页面信息 - 标题: '{title}' (长度: {len(title)}), 空间: {space_key}, 父页面: {parent_id}")
            print(f"尝试使用备用方法创建页面...")

            # 备用方法：直接使用 REST API
            try:
                return self._create_page_with_rest_api(space_key, title, content, parent_id)
            except Exception as e2:
                backup_error = f"备用方法也失败: {str(e2)}"
                print(backup_error)
                print(f"完整错误信息 - 主要错误: {error_details}, 备用错误: {backup_error}")
                return None

    def _create_page_with_rest_api(self, space_key, title, content, parent_id=None):
        """
        使用 REST API 直接创建页面的备用方法
        """
        try:
            # 构建页面数据
            page_data = {
                'type': 'page',
                'title': title,
                'space': {'key': space_key},
                'body': {
                    'storage': {
                        'value': content,
                        'representation': 'storage'
                    }
                }
            }

            # 如果指定了父页面，添加 ancestors
            if parent_id:
                page_data['ancestors'] = [{'id': parent_id}]

            # 使用内部的 REST 客户端
            url = f"{self.confluence.url}/rest/api/content"

            # 发送 POST 请求
            response = self.confluence.post(url, data=page_data)

            if response and 'id' in response:
                print(f"使用备用方法创建页面成功: {response['title']} (ID: {response['id']})")
                return response
            else:
                print(f"备用方法创建页面失败 - 响应: {response}")
                if response and 'message' in response:
                    print(f"Confluence API错误信息: {response['message']}")
                return None

        except Exception as e:
            print(f"备用方法执行失败: {str(e)}")
            print(f"请求数据: space={space_key}, title='{title}' (长度: {len(title)}), parent_id={parent_id}")
            return None

    def update_page(self, page_id, title, content, version_number=None):
        """
        更新页面

        Args:
            page_id: 页面 ID
            title: 新标题
            content: 新内容 (HTML 格式)
            version_number: 版本号 (如果不提供会自动获取)
        """
        try:
            # 获取当前页面信息
            current_page = self.confluence.get_page_by_id(
                page_id,
                expand='version,body.storage'
            )

            if not current_page:
                print(f"未找到 ID 为 {page_id} 的页面")
                return None

            # 获取当前版本号
            current_version = current_page['version']['number']
            new_version = version_number if version_number else current_version + 1

            # 更新页面
            result = self.confluence.update_page(
                page_id=page_id,
                title=title,
                body=content,
                version=new_version
            )

            print(f"页面更新成功: {result['title']} (版本: {result['version']['number']})")
            return result

        except Exception as e:
            print(f"更新页面失败: {e}")
            return None

    def delete_page(self, page_id):
        """删除页面"""
        try:
            # 获取页面信息用于确认
            page = self.confluence.get_page_by_id(page_id)
            if page:
                print(f"准备删除页面: {page['title']}")
                result = self.confluence.remove_page(page_id)
                print(f"页面删除成功")
                return result
            else:
                print(f"未找到 ID 为 {page_id} 的页面")
                return None

        except Exception as e:
            print(f"删除页面失败: {e}")
            return None

    def search_content(self, query, limit=25):
        """搜索内容"""
        try:
            results = self.confluence.cql(
                cql=f'text ~ "{query}"',
                limit=limit,
                expand='content.space,content.version'
            )

            print(f"搜索 '{query}' 的结果：")
            for result in results['results']:
                content = result['content']
                print(f"- {content['title']} (空间: {content['space']['name']})")

            return results

        except Exception as e:
            print(f"搜索失败: {e}")
            return None

    def add_attachment(self, page_id, file_path, comment=""):
        """为页面添加附件"""
        try:
            result = self.confluence.attach_file(
                filename=file_path,
                page_id=page_id,
                comment=comment
            )
            print(f"附件上传成功: {file_path}")
            return result

        except Exception as e:
            print(f"上传附件失败: {e}")
            return None

    def get_page_attachments(self, page_id):
        """获取页面的所有附件"""
        try:
            attachments = self.confluence.get_attachments_from_content(page_id)
            print(f"页面附件列表：")
            for attachment in attachments['results']:
                print(f"- {attachment['title']} (大小: {attachment['extensions']['fileSize']} bytes)")
            return attachments

        except Exception as e:
            print(f"获取附件失败: {e}")
            return None

    def export_page_as_pdf(self, page_id, output_path):
        """将页面导出为 PDF"""
        try:
            pdf_content = self.confluence.export_page(page_id)
            with open(output_path, 'wb') as f:
                f.write(pdf_content)
            print(f"页面已导出为 PDF: {output_path}")
            return True

        except Exception as e:
            print(f"导出 PDF 失败: {e}")
            return False

    # ===== 数据模型文档创建功能 =====

    def create_status_macro(self, title: str, color: str = "Green") -> str:
        """
        创建状态宏标签

        Args:
            title: 状态标题 (如 APPROVED, REQUIRE UPDATE)
            color: 颜色 (Green, Red, Yellow, Blue, Grey)

        Returns:
            状态宏的 HTML 代码
        """
        return f"""<ac:structured-macro ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">{color}</ac:parameter><ac:parameter ac:name="title">{title}</ac:parameter></ac:structured-macro>"""

    def create_info_macro(self, content: str, macro_type: str = "info") -> str:
        """
        创建信息宏 (info, warning, note, tip)

        Args:
            content: 宏内容
            macro_type: 宏类型 (info, warning, note, tip)

        Returns:
            信息宏的 HTML 代码
        """
        return f"""<ac:structured-macro ac:name="{macro_type}" ac:schema-version="1"><ac:rich-text-body><p>{content}</p></ac:rich-text-body></ac:structured-macro>"""

    def create_table_from_data(self, headers: List[str], rows: List[List[str]],
                               table_class: str = "default") -> str:
        """
        从数据创建表格

        Args:
            headers: 表头列表
            rows: 数据行列表
            table_class: 表格样式类 (default, confluenceTable)

        Returns:
            表格的 HTML 代码
        """
        # 创建表头
        header_html = "<thead><tr>"
        for header in headers:
            header_html += f"<th><p><strong>{header}</strong></p></th>"
        header_html += "</tr></thead>"

        # 创建数据行
        body_html = "<tbody>"
        for row in rows:
            body_html += "<tr>"
            for cell in row:
                body_html += f"<td><p>{cell}</p></td>"
            body_html += "</tr>"
        body_html += "</tbody>"

        return f"""<table data-layout="{table_class}">{header_html}{body_html}</table>"""

    def create_data_model_page(self, space_key: str, model_config: Dict, parent_id: Optional[str] = None):
        """
        创建数据模型文档页面

        Args:
            space_key: 空间键
            model_config: 模型配置字典
            parent_id: 父页面ID (可选)
        """
        try:
            # 构建页面内容
            content = self._build_data_model_content(model_config)

            # 创建页面
            page = self.create_page(
                space_key=space_key,
                title=model_config.get("title", "数据模型文档"),
                content=content,
                parent_id=parent_id
            )

            return page

        except Exception as e:
            print(f"创建数据模型页面失败: {e}")
            return None

    def _build_data_model_content(self, config: Dict) -> str:
        """构建数据模型页面内容"""

        # 构建状态标签
        status_html = ""
        for status in config.get("status_tags", []):
            status_html += self.create_status_macro(status["title"], status["color"])
            status_html += " "  # 添加间距

        # 构建需求信息表格
        requirement_rows = [
            ["Requirement Description", config.get("requirement_description", "")],
            ["Entity List", config.get("entity_list", "")],
            ["Review Requester", " ".join(config.get("review_requesters", []))],
            ["Reviewer (Mandatory)", config.get("reviewer_mandatory", "")],
            ["Model Knowledge Collection Link", config.get("knowledge_link", "待添加")],
            ["Review Date", config.get("review_date", datetime.now().strftime('%Y年%m月%d日'))],
            ["Status", status_html]
        ]

        requirement_table = self.create_table_from_data(
            headers=["项目", "内容"],
            rows=requirement_rows
        )

        # 构建 DataFlow 信息
        dataflow = config.get("dataflow", {})
        dataflow_html = f"""<h2>DataFlow</h2><p><strong>{dataflow.get("source", "源数据集")}</strong> → <strong>{dataflow.get("target", "目标数据集")}</strong></p>"""

        # 构建模型字段表格
        model_fields = config.get("model_fields", [])
        if model_fields:
            field_rows = []
            for field in model_fields:
                field_rows.append([
                    field.get("schema", ""),
                    field.get("mode_name", ""),
                    field.get("table_name", ""),
                    field.get("attribute_name", ""),
                    field.get("column_name", ""),
                    field.get("column_type", ""),
                    field.get("pk", "")
                ])

            model_table = self.create_table_from_data(
                headers=["Schema", "Mode Name", "Table Name", "Attribute Name",
                         "Column Name", "Column Type", "PK"],
                rows=field_rows
            )
        else:
            model_table = "<p><em>暂无模型字段信息</em></p>"

        # 组装完整页面内容
        content = f"""<h1>数据模型文档</h1><h2>需求信息</h2>{requirement_table}{dataflow_html}<h2>Model Screenshot</h2><p><strong>模型字段:</strong></p>{model_table}<h2>备注</h2>{self.create_info_macro("此页面通过 Python API 自动生成，包含完整的数据模型文档信息。", "info")}"""

        return content

    def add_page_labels(self, page_id: str, labels: List[str]) -> bool:
        """
        为页面添加标签

        Args:
            page_id: 页面ID
            labels: 标签列表

        Returns:
            是否添加成功
        """
        try:
            for label in labels:
                self.confluence.set_page_label(page_id, label)
            print(f"标签添加成功: {', '.join(labels)}")
            return True
        except Exception as e:
            print(f"添加标签失败: {e}")
            return False

    def create_page_comment(self, page_id: str, comment: str) -> Optional[Dict]:
        """
        为页面添加评论 - 已禁用

        Args:
            page_id: 页面ID
            comment: 评论内容

        Returns:
            None (功能已禁用)
        """
        print(f"页面评论功能已暂时禁用 - 页面ID: {page_id}")
        return None


def create_finance_model_pages():
    """在指定页面层次结构下创建财务模型页面"""

    # 配置连接信息
    CONFLUENCE_URL = "https://km.xpaas.lenovo.com/"
    USERNAME = "longyu3"
    API_TOKEN = "ODAwMTgyNDE4MjkzOkf49kKmllqMHutw8/Z5Qeq2Zntn"

    # 目标空间和页面路径
    TARGET_SPACE_NAME = "EDW Delivery Knowledge Center"
    PAGE_PATH = [
        "EDW Data Modeling",
        "Model Review Process & Review Log",
        "Solution Model Review Log",
        "Finance Solution Model"
    ]

    print("=" * 80)
    print("EDW Finance Solution Model 页面创建工具")
    print("=" * 80)

    # 初始化管理器
    cm = ConfluenceManager(CONFLUENCE_URL, USERNAME, "", API_TOKEN)

    # 1. 查找目标空间
    print(f"\n步骤 1: 查找空间 '{TARGET_SPACE_NAME}'")
    target_space = cm.find_space_by_name(TARGET_SPACE_NAME)
    if not target_space:
        print(f"错误: 无法找到空间 '{TARGET_SPACE_NAME}'")
        return None

    space_key = target_space['key']
    print(f"✓ 找到空间: {target_space['name']} (Key: {space_key})")

    # 2. 查找目标父页面
    print(f"\n步骤 2: 查找页面路径")
    print(f"路径: {' -> '.join(PAGE_PATH)}")

    parent_page = cm.find_page_by_path(space_key, PAGE_PATH)
    if not parent_page:
        print(f"错误: 无法找到目标父页面")
        print(f"请确认以下页面路径是否存在:")
        for i, page_name in enumerate(PAGE_PATH):
            print(f"  {i+1}. {page_name}")
        return None

    print(f"✓ 找到目标父页面: {parent_page['title']} (ID: {parent_page['id']})")

    # 3. 查看父页面的现有子页面
    print(f"\n步骤 3: 查看父页面现有子页面")
    existing_children = cm.get_page_children(parent_page['id'])

    # 4. 创建数据模型页面
    print(f"\n步骤 4: 创建数据模型页面")

    # 配置数据模型信息（基于原图）
    model_config = {
        "title": "2025-05-29: Finance Data Model Review - PCSD Fact Finace Actual PNL Audit Trail Enhance",
        "requirement_description": "segment phase 2 月结之后则需要存储数据后对比表",
        "entity_list": "cam_fi.PCSD Fact Finace Actual PNL Audit Trail",
        "review_requesters": ["@Daisy Shi", "@Serena XQ7 Sun", "@Xianmei XM2 Chang"],
        "reviewer_mandatory": "@Tommy ZC1 Tong",
        "knowledge_link": "待添加知识库链接",
        "review_date": "2025年5月13日",
        "status_tags": [
            {"title": "APPROVED", "color": "Green"},
            {"title": "REQUIRE UPDATE", "color": "Yellow"},
            {"title": "APPROVED", "color": "Green"}
        ],
        "dataflow": {
            "source": "cam_fi.CAM Fact Finace Actual PNL Dataset",
            "target": "cam_fi.PCSD Fact Finace Actual PNL Audit Trail"
        },
        "model_fields": [
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Fiscal Year Period", "column_name": "fy_period", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Company Code", "column_name": "company_cd", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Sales Document Number", "column_name": "sales_doc_no", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Source ID", "column_name": "source_id", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Data Source Category Code", "column_name": "data_source_catg_cd", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Billing Document Number", "column_name": "billing_doc_no", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Line Item of Billing Doc Number", "column_name": "line_item_of_billing_doc_no", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Billing category Code", "column_name": "billing_catg_cd", "column_type": "string", "pk": "Y"},
            {"schema": "cam_fi", "mode_name": "Fact Finace Actual PNL Audit Trail", "table_name": "fact_fi_actl_pnl_audit_trail", "attribute_name": "Geo Code", "column_name": "geo_cd", "column_type": "string", "pk": "N"}
        ]
    }

    # 创建页面
    new_page = cm.create_data_model_page(
        space_key=space_key,
        model_config=model_config,
        parent_id=parent_page['id']
    )

    if new_page:
        print(f"✓ 数据模型页面创建成功!")
        print(f"  页面ID: {new_page['id']}")
        print(f"  页面标题: {new_page['title']}")
        print(f"  父页面: {parent_page['title']}")

        # 5. 添加标签（评论功能已禁用）
        print(f"\n步骤 5: 添加标签")
        cm.add_page_labels(new_page['id'], ['财务模型', 'cam_fi', 'PNL', 'Audit Trail', 'EDW'])
        print("页面评论功能已暂时禁用")

        print(f"\n" + "=" * 80)
        print("页面创建完成!")
        print(f"主页面: {new_page['title']}")
        print(f"位置: {TARGET_SPACE_NAME} -> {' -> '.join(PAGE_PATH)} -> {new_page['title']}")
        print(f"页面URL: {CONFLUENCE_URL.rstrip('/')}/pages/viewpage.action?pageId={new_page['id']}")
        print("=" * 80)

        return new_page

    else:
        print("✗ 页面创建失败")
        return None


def main():
    """主函数"""
    try:
        # 创建财务模型页面
        result = create_finance_model_pages()

        if result:
            print("\n🎉 所有任务执行成功!")
        else:
            print("\n❌ 任务执行失败，请检查配置和权限")

    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")


if __name__ == "__main__":
    print("EDW Confluence 页面创建工具")
    print("专用于在指定页面层次结构下创建财务模型文档")

    # 运行主程序
    main()