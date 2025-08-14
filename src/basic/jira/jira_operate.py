"""
JIRA API 操作完整指南 - EDW专用版
使用 atlassian-python-api 库与 JIRA 进行交互
专门用于更新JIRA状态和添加评论功能
"""

from atlassian import Jira
import json
import os
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging

logger = logging.getLogger(__name__)


class JiraManager:
    """JIRA管理器类"""
    
    def __init__(self, url: str = None, username: str = None, token: str = None):
        """
        初始化 JIRA 连接

        Args:
            url: JIRA 实例 URL (如 https://your-domain.atlassian.net 或 https://jira.company.com)
            username: 用户名或邮箱
            token: API token
        """
        # 从环境变量获取配置（如果参数未提供）
        self.jira_url = url or os.getenv("JIRA_URL")
        self.username = username or os.getenv("JIRA_USERNAME", "longyu3")
        self.token = token or os.getenv("JIRA_TOKEN")
        
        if not self.jira_url:
            raise ValueError("JIRA_URL must be provided or set in environment variables")
        if not self.token:
            raise ValueError("JIRA_TOKEN must be provided or set in environment variables")
        
        # 初始化JIRA连接
        try:
            self.jira = Jira(
                url=self.jira_url,
                username=self.username,
                token=self.token,  # 使用token认证
                cloud=False  # 企业版JIRA Server
            )
            logger.info(f"JIRA连接初始化成功: {self.jira_url}")
        except Exception as e:
            logger.error(f"JIRA连接初始化失败: {e}")
            raise

    def get_issue(self, issue_key: str) -> Optional[Dict[str, Any]]:
        """
        获取JIRA问题详情

        Args:
            issue_key: 问题键值 (如 EDW-123)

        Returns:
            问题详情字典或None
        """
        try:
            issue = self.jira.issue(issue_key, expand='transitions,comments')
            if issue:
                logger.info(f"成功获取问题: {issue_key}")
                return issue
            else:
                logger.warning(f"未找到问题: {issue_key}")
                return None
        except Exception as e:
            logger.error(f"获取问题失败 {issue_key}: {e}")
            return None

    def get_issue_transitions(self, issue_key: str) -> List[Dict[str, Any]]:
        """
        获取问题的可用状态转换

        Args:
            issue_key: 问题键值

        Returns:
            可用转换列表
        """
        try:
            transitions = self.jira.get_issue_transitions(issue_key)
            logger.info(f"问题 {issue_key} 可用转换: {len(transitions)} 个")
            for transition in transitions:
                logger.info(f"  - {transition['name']} (ID: {transition['id']})")
            return transitions
        except Exception as e:
            logger.error(f"获取问题转换失败 {issue_key}: {e}")
            return []

    def update_issue_status(self, issue_key: str, status_name: str, comment: str = "") -> bool:
        """
        更新JIRA问题状态

        Args:
            issue_key: 问题键值 (如 EDW-123)
            status_name: 目标状态名称 (如 "In Progress", "Done", "To Do")
            comment: 状态变更时的评论 (可选)

        Returns:
            是否更新成功
        """
        try:
            logger.info(f"准备更新问题 {issue_key} 状态为: {status_name}")
            
            # 1. 获取当前问题信息
            issue = self.get_issue(issue_key)
            if not issue:
                logger.error(f"无法获取问题信息: {issue_key}")
                return False
            
            current_status = issue['fields']['status']['name']
            logger.info(f"当前状态: {current_status}")
            
            # 如果已经是目标状态，无需更新
            if current_status == status_name:
                logger.info(f"问题 {issue_key} 已经是 {status_name} 状态")
                return True
            
            # 2. 获取可用的状态转换
            transitions = self.get_issue_transitions(issue_key)
            if not transitions:
                logger.error(f"无法获取问题 {issue_key} 的状态转换")
                return False
            
            # 3. 查找目标状态对应的转换ID
            target_transition = None
            for transition in transitions:
                if transition['name'].lower() == status_name.lower():
                    target_transition = transition
                    break
                # 也检查转换的目标状态
                if 'to' in transition and transition['to']['name'].lower() == status_name.lower():
                    target_transition = transition
                    break
            
            if not target_transition:
                logger.error(f"未找到到状态 '{status_name}' 的转换")
                logger.info(f"可用转换: {[t['name'] for t in transitions]}")
                return False
            
            # 4. 执行状态转换
            transition_id = target_transition['id']
            logger.info(f"执行转换: {target_transition['name']} (ID: {transition_id})")
            
            # 构建转换数据
            transition_data = {
                "transition": {"id": transition_id}
            }
            
            # 如果有评论，添加到转换中
            if comment.strip():
                transition_data["update"] = {
                    "comment": [
                        {
                            "add": {
                                "body": comment
                            }
                        }
                    ]
                }
            
            # 执行转换
            result = self.jira.issue_transition(issue_key, transition_data)
            
            # 验证状态更新
            updated_issue = self.get_issue(issue_key)
            if updated_issue:
                new_status = updated_issue['fields']['status']['name']
                if new_status == status_name:
                    logger.info(f"状态更新成功: {issue_key} {current_status} -> {new_status}")
                    return True
                else:
                    logger.warning(f"状态更新异常: 期望 {status_name}, 实际 {new_status}")
                    return False
            else:
                logger.error("无法验证状态更新结果")
                return False
                
        except Exception as e:
            logger.error(f"更新问题状态失败 {issue_key}: {e}")
            return False

    def add_comment(self, issue_key: str, comment: str, visibility: Dict = None) -> bool:
        """
        为JIRA问题添加评论

        Args:
            issue_key: 问题键值 (如 EDW-123)
            comment: 评论内容
            visibility: 评论可见性设置 (可选)
                例如: {"type": "group", "value": "developers"}

        Returns:
            是否添加成功
        """
        try:
            logger.info(f"准备为问题 {issue_key} 添加评论")
            
            # 验证问题是否存在
            issue = self.get_issue(issue_key)
            if not issue:
                logger.error(f"无法获取问题信息: {issue_key}")
                return False
            
            # 构建评论数据
            comment_data = {
                "body": comment
            }
            
            # 添加可见性设置
            if visibility:
                comment_data["visibility"] = visibility
            
            # 添加评论
            result = self.jira.issue_add_comment(issue_key, comment_data)
            
            if result:
                logger.info(f"评论添加成功: {issue_key}")
                logger.info(f"评论内容: {comment[:100]}...")
                return True
            else:
                logger.error(f"评论添加失败: {issue_key}")
                return False
                
        except Exception as e:
            logger.error(f"添加评论失败 {issue_key}: {e}")
            return False

    def get_issue_comments(self, issue_key: str) -> List[Dict[str, Any]]:
        """
        获取问题的所有评论

        Args:
            issue_key: 问题键值

        Returns:
            评论列表
        """
        try:
            issue = self.get_issue(issue_key)
            if not issue:
                return []
            
            comments = issue['fields']['comments']['comments']
            logger.info(f"问题 {issue_key} 共有 {len(comments)} 条评论")
            
            return comments
            
        except Exception as e:
            logger.error(f"获取评论失败 {issue_key}: {e}")
            return []

    def update_issue_field(self, issue_key: str, field_name: str, field_value: Any) -> bool:
        """
        更新JIRA问题的特定字段

        Args:
            issue_key: 问题键值
            field_name: 字段名称
            field_value: 字段值

        Returns:
            是否更新成功
        """
        try:
            logger.info(f"准备更新问题 {issue_key} 字段 {field_name}")
            
            # 构建更新数据
            update_data = {
                "fields": {
                    field_name: field_value
                }
            }
            
            # 执行更新
            result = self.jira.update_issue_field(issue_key, update_data)
            
            logger.info(f"字段更新成功: {issue_key}.{field_name}")
            return True
            
        except Exception as e:
            logger.error(f"更新字段失败 {issue_key}.{field_name}: {e}")
            return False

    def search_issues(self, jql: str, max_results: int = 50) -> List[Dict[str, Any]]:
        """
        使用JQL搜索问题

        Args:
            jql: JQL查询语句
            max_results: 最大结果数

        Returns:
            问题列表
        """
        try:
            logger.info(f"执行JQL搜索: {jql}")
            
            issues = self.jira.jql(jql, limit=max_results)
            
            if 'issues' in issues:
                logger.info(f"搜索到 {len(issues['issues'])} 个问题")
                return issues['issues']
            else:
                logger.warning("搜索结果为空")
                return []
                
        except Exception as e:
            logger.error(f"JQL搜索失败: {e}")
            return []

    def get_project_info(self, project_key: str) -> Optional[Dict[str, Any]]:
        """
        获取项目信息

        Args:
            project_key: 项目键值

        Returns:
            项目信息字典或None
        """
        try:
            project = self.jira.project(project_key)
            if project:
                logger.info(f"获取项目信息成功: {project['name']}")
                return project
            else:
                logger.warning(f"未找到项目: {project_key}")
                return None
                
        except Exception as e:
            logger.error(f"获取项目信息失败: {e}")
            return None

    def create_edw_comment(self, issue_key: str, table_name: str, action: str, 
                          details: str = "", success: bool = True) -> bool:
        """
        创建EDW专用的评论格式

        Args:
            issue_key: 问题键值
            table_name: 表名
            action: 执行的操作 (如 "模型增强", "字段添加", "代码生成")
            details: 详细信息
            success: 是否成功

        Returns:
            是否添加成功
        """
        try:
            # 构建EDW格式的评论
            status_emoji = "✅" if success else "❌"
            status_text = "成功" if success else "失败"
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            comment_body = f"""
{status_emoji} **EDW自动化操作 - {status_text}**

**表名:** {table_name}
**操作:** {action}
**时间:** {timestamp}

**详细信息:**
{details}

---
*此评论由EDW自动化系统生成*
"""
            
            return self.add_comment(issue_key, comment_body)
            
        except Exception as e:
            logger.error(f"创建EDW评论失败: {e}")
            return False


def test_jira_operations():
    """测试JIRA操作功能"""
    try:
        print("=" * 60)
        print("JIRA操作功能测试")
        print("=" * 60)
        
        # 初始化JIRA管理器
        jm = JiraManager()
        
        # 测试问题键值（需要替换为实际存在的问题）
        test_issue_key = "EDW-1"  # 请替换为实际的问题键值
        
        print(f"\n1. 测试获取问题信息: {test_issue_key}")
        issue = jm.get_issue(test_issue_key)
        if issue:
            print(f"   问题标题: {issue['fields']['summary']}")
            print(f"   当前状态: {issue['fields']['status']['name']}")
        
        print(f"\n2. 测试获取可用状态转换")
        transitions = jm.get_issue_transitions(test_issue_key)
        
        print(f"\n3. 测试添加评论")
        comment_success = jm.add_comment(
            test_issue_key, 
            "这是一条测试评论，由JIRA API自动添加。"
        )
        print(f"   评论添加: {'成功' if comment_success else '失败'}")
        
        print(f"\n4. 测试EDW格式评论")
        edw_comment_success = jm.create_edw_comment(
            test_issue_key,
            "dwd_fi.test_table",
            "模型增强测试",
            "添加了3个新字段，优化了查询性能",
            True
        )
        print(f"   EDW评论添加: {'成功' if edw_comment_success else '失败'}")
        
        print("\n" + "=" * 60)
        print("测试完成")
        print("=" * 60)
        
    except Exception as e:
        print(f"测试失败: {e}")


if __name__ == "__main__":
    # 运行测试（仅在开发环境使用）
    print("JIRA操作类 - 开发测试")
    print("注意: 请确保在开发环境中运行，并替换为实际的问题键值")
    # test_jira_operations()  # 取消注释以运行测试