"""
GitHub工具类：用于在企业GitHub仓库中搜索和读取代码

主要功能：
1. 搜索和读取代码文件
2. 提交和更新文件
3. 代码版本回退功能
   - get_commit_history: 获取文件或仓库的提交历史
   - get_file_at_commit: 获取文件在指定提交时的内容
   - rollback_file: 将文件回滚到指定版本
   - revert_last_commit: 撤销最近的n个提交
   - compare_versions: 比较两个版本之间的差异

使用示例：
    # 初始化工具
    github_tool = GitHubTool()
    
    # 获取文件提交历史
    history = github_tool.get_commit_history("src/model.py", max_count=10)
    
    # 回滚到特定版本
    result = github_tool.rollback_file("src/model.py", "abc123def456")
    
    # 撤销最近的提交
    result = github_tool.revert_last_commit("src/model.py")
    
    # 比较版本差异
    diff = github_tool.compare_versions("src/model.py", "abc123", "def456")
"""
import os
import base64
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
from github import Github, GithubException
from github.ContentFile import ContentFile
from github.Repository import Repository

logger = logging.getLogger(__name__)


class GitHubTool:
    """GitHub工具类，用于搜索和读取仓库中的代码"""
    
    def __init__(self, token: Optional[str] = None, repo_name: Optional[str] = None, 
                 branch: Optional[str] = None, base_url: Optional[str] = None):
        """
        初始化GitHub工具
        
        Args:
            token: GitHub访问令牌，如果不提供则从环境变量读取
            repo_name: 仓库名称（格式：owner/repo），如果不提供则从环境变量读取
            branch: 默认分支名称，如果不提供则从环境变量读取或使用main
            base_url: GitHub企业版的API地址，如果不提供则从环境变量读取
        """
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.repo_name = repo_name or os.getenv("GITHUB_REPO")
        self.branch = branch or os.getenv("GITHUB_BRANCH", "main")
        self.base_url = base_url or os.getenv("GITHUB_BASE_URL")
        
        if not self.token:
            raise ValueError("GitHub token未配置，请设置GITHUB_TOKEN环境变量")
        if not self.repo_name:
            raise ValueError("GitHub仓库未配置，请设置GITHUB_REPO环境变量")
        
        # 处理GITHUB_REPO格式（移除可能的URL前缀）
        if self.repo_name.startswith("http"):
            # 从URL中提取owner/repo部分
            import re
            match = re.search(r'[^/]+/[^/]+$', self.repo_name.rstrip('/'))
            if match:
                self.repo_name = match.group()
                logger.info(f"从URL中提取仓库名: {self.repo_name}")
            else:
                raise ValueError(f"无法从URL中提取仓库名: {self.repo_name}")
        
        # 初始化GitHub客户端
        if self.base_url:
            # 企业版GitHub
            self.github = Github(self.token, base_url=self.base_url)
            logger.info(f"使用企业GitHub: {self.base_url}")
        else:
            # 公共GitHub
            self.github = Github(self.token)
        
        # 获取仓库对象
        try:
            self.repo: Repository = self.github.get_repo(self.repo_name)
            logger.info(f"成功连接到GitHub仓库: {self.repo_name}")
        except GithubException as e:
            raise ValueError(f"无法访问仓库 {self.repo_name}: {e}")
    
    def search_files_by_name(self, pattern: str, path_prefix: str = "") -> List[Dict[str, Any]]:
        """
        按文件名搜索文件
        
        Args:
            pattern: 文件名模式（支持通配符）
            path_prefix: 路径前缀，用于限制搜索范围
        
        Returns:
            匹配的文件列表，每个元素包含path、name、type等信息
        """
        try:
            matched_files = []
            
            # 使用GitHub搜索API
            # 注意：GitHub搜索API有限制，可能需要分页处理
            query = f"repo:{self.repo_name} filename:{pattern}"
            if path_prefix:
                query += f" path:{path_prefix}"
            
            # 执行搜索
            search_results = self.github.search_code(query=query)
            
            for result in search_results:
                file_info = {
                    "path": result.path,
                    "name": result.name,
                    "sha": result.sha,
                    "url": result.html_url,
                    "download_url": result.download_url
                }
                matched_files.append(file_info)
            
            logger.info(f"GitHub API返回 {len(matched_files)} 个匹配文件")
            return matched_files
            
        except GithubException as e:
            logger.error(f"搜索文件失败: {e}")
            return []
    
    def read_file(self, file_path: str, ref: Optional[str] = None) -> Optional[str]:
        """
        读取文件内容
        
        Args:
            file_path: 文件路径（相对于仓库根目录）
            ref: 分支、标签或commit SHA，默认使用初始化时的分支
        
        Returns:
            文件内容字符串，如果失败返回None
        """
        try:
            ref = ref or self.branch
            content_file = self.repo.get_contents(file_path, ref=ref)
            
            if isinstance(content_file, list):
                # 如果是目录，返回None
                logger.warning(f"{file_path} 是一个目录，不是文件")
                return None
            
            # 解码文件内容
            content = base64.b64decode(content_file.content).decode('utf-8')
            return content
            
        except GithubException as e:
            logger.error(f"读取文件失败 {file_path}: {e}")
            return None
    
    def search_table_code(self, table_name: str) -> Dict[str, Any]:
        """
        查询某个表的源代码（兼容原有search_table_cd函数的接口）
        
        Args:
            table_name: 表名，格式如 dwd_fi.fi_invoice_item
        
        Returns:
            返回结果字典，与原search_table_cd函数格式一致
        """
        try:
            # 解析表名
            parts = table_name.split(".")
            if len(parts) != 2:
                return {"status": "error", "message": f"表名格式不正确: {table_name}"}
            
            schema, name = parts
            file_pattern = f"nb_{name}"
            
            logger.info(f"正在GitHub仓库中查找表: {table_name} 的代码")
            
            # 搜索文件
            matched_files = self.search_files_by_name(file_pattern)
            
            if not matched_files:
                return {"status": "error", "message": f"未找到表 {table_name} 的相关代码"}
            
            # 精确匹配文件名（去除GitHub搜索的部分匹配结果）
            exact_pattern = f"nb_{name}"
            exact_matched_files = []
            for file_info in matched_files:
                file_name_without_ext = os.path.splitext(file_info['name'])[0]
                # 精确匹配：文件名（不含扩展名）必须完全等于pattern
                if file_name_without_ext == exact_pattern:
                    exact_matched_files.append(file_info)
                    logger.debug(f"精确匹配: {file_info['name']}")
                else:
                    logger.debug(f"排除部分匹配: {file_info['name']} (不等于 {exact_pattern})")
            
            if not exact_matched_files:
                # 如果没有精确匹配，回退到原始匹配结果
                logger.warning(f"没有精确匹配的文件，使用部分匹配结果")
                exact_matched_files = matched_files
            
            # 过滤包含schema的文件
            target_file = None
            for file_info in exact_matched_files:
                if schema in file_info['path']:
                    target_file = file_info
                    break
            
            if not target_file:
                # 如果没有找到包含schema的文件，使用第一个匹配的文件
                target_file = exact_matched_files[0]
            
            file_path = target_file['path']
            file_name = target_file['name']
            
            # 检查文件扩展名
            if not file_name.endswith(('.sql', '.py')):
                return {
                    "status": "error", 
                    "message": f"暂不支持的代码文件格式: {file_name}, 仅支持 .sql 和 .py 文件。"
                }
            
            # 读取文件内容
            content = self.read_file(file_path)
            if content is None:
                return {"status": "error", "message": f"无法读取文件内容: {file_path}"}
            
            # 获取文件元信息
            try:
                content_file = self.repo.get_contents(file_path, ref=self.branch)
                if isinstance(content_file, list):
                    return {"status": "error", "message": f"路径是目录而非文件: {file_path}"}
                
                size = content_file.size
                last_modified = content_file.last_modified  # 这可能需要额外的API调用
            except:
                size = len(content.encode('utf-8'))
                last_modified = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            language = 'sql' if file_name.endswith('.sql') else 'python'
            
            # 构建返回结果（与原search_table_cd格式一致）
            file_info = {
                'status': 'success',
                'table_name': table_name,
                'description': f"{table_name}表的数据加工代码",
                'code': content,
                'language': language,
                'file_name': file_name,
                'file_path': file_path,  # GitHub上的路径
                'file_size': size,
                'file_info': {
                    'name': file_name,
                    'language': language,
                    'size': size,
                    'last_modified': last_modified,
                    'github_url': target_file['url'],
                    'download_url': target_file['download_url']
                },
                'timestamp': datetime.now().isoformat(),
                'source': 'github'  # 标记数据来源
            }
            
            return file_info
            
        except Exception as e:
            logger.error(f"搜索表代码时发生异常: {e}")
            return {"status": "error", "message": f"搜索失败: {str(e)}"}
    
    def update_file_on_branch(self, file_path: str, content: str, message: str,
                             branch: Optional[str] = None, create_branch: bool = False,
                             base_branch: Optional[str] = None) -> Dict[str, Any]:
        """
        更新文件并推送到指定分支
        
        Args:
            file_path: 文件路径（相对于仓库根目录）
            content: 新的文件内容
            message: 提交消息
            branch: 目标分支名，默认使用初始化时的分支
            create_branch: 如果分支不存在是否创建新分支
            base_branch: 创建新分支时的基础分支，默认使用主分支
        
        Returns:
            操作结果字典，包含status、commit信息等
        """
        try:
            target_branch = branch or self.branch
            base_branch = base_branch or self.branch
            
            # 检查目标分支是否存在
            branch_exists = True
            try:
                self.repo.get_branch(target_branch)
                logger.info(f"目标分支 {target_branch} 已存在")
            except GithubException:
                branch_exists = False
                logger.info(f"目标分支 {target_branch} 不存在")
            
            # 如果分支不存在且需要创建
            if not branch_exists:
                if create_branch:
                    try:
                        # 获取基础分支的最新commit
                        base_ref = self.repo.get_branch(base_branch)
                        base_sha = base_ref.commit.sha
                        
                        # 创建新分支
                        ref = f"refs/heads/{target_branch}"
                        self.repo.create_git_ref(ref=ref, sha=base_sha)
                        logger.info(f"成功创建新分支 {target_branch}，基于 {base_branch}")
                    except GithubException as e:
                        return {
                            "status": "error",
                            "message": f"创建分支失败: {str(e)}"
                        }
                else:
                    return {
                        "status": "error",
                        "message": f"分支 {target_branch} 不存在，请设置 create_branch=True 来创建新分支"
                    }
            
            # 检查文件是否存在并获取当前内容
            file_exists = True
            current_sha = None
            try:
                existing_file = self.repo.get_contents(file_path, ref=target_branch)
                current_sha = existing_file.sha
                current_content = base64.b64decode(existing_file.content).decode('utf-8')
                
                # 检查内容是否有变化
                if current_content == content:
                    return {
                        "status": "no_change",
                        "message": "文件内容未发生变化",
                        "file": {
                            "path": file_path,
                            "branch": target_branch,
                            "url": existing_file.html_url
                        }
                    }
            except GithubException:
                file_exists = False
                logger.info(f"文件 {file_path} 在分支 {target_branch} 中不存在，将创建新文件")
            
            # 更新或创建文件
            if file_exists:
                # 更新现有文件
                result = self.repo.update_file(
                    path=file_path,
                    message=message,
                    content=content,
                    sha=current_sha,
                    branch=target_branch
                )
                operation = "updated"
                logger.info(f"成功更新文件 {file_path}")
            else:
                # 创建新文件
                result = self.repo.create_file(
                    path=file_path,
                    message=message,
                    content=content,
                    branch=target_branch
                )
                operation = "created"
                logger.info(f"成功创建文件 {file_path}")
            
            # 构建返回结果
            return {
                "status": "success",
                "operation": operation,
                "branch": target_branch,
                "commit": {
                    "sha": result['commit'].sha,
                    "message": message,
                    "author": result['commit'].commit.author.name,
                    "date": result['commit'].commit.author.date.isoformat(),
                    "url": result['commit'].html_url
                },
                "file": {
                    "path": file_path,
                    "size": len(content.encode('utf-8')),
                    "sha": result['content'].sha,
                    "url": result['content'].html_url
                }
            }
            
        except GithubException as e:
            error_msg = f"更新文件失败: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg,
                "details": e.data if hasattr(e, 'data') else None
            }
        except Exception as e:
            error_msg = f"意外错误: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
    def commit_file(self, file_path: str, content: str, message: str, 
                   branch: Optional[str] = None) -> Dict[str, Any]:
        """
        提交文件到GitHub仓库（简化版本，向后兼容）
        
        Args:
            file_path: 文件路径
            content: 文件内容
            message: 提交消息
            branch: 目标分支，默认使用初始化时的分支
        
        Returns:
            提交结果信息
        """
        return self.update_file_on_branch(
            file_path=file_path,
            content=content,
            message=message,
            branch=branch,
            create_branch=False
        )
    
    def list_files(self, path: str = "", recursive: bool = False) -> List[Dict[str, Any]]:
        """
        列出仓库中的文件
        
        Args:
            path: 目录路径，默认为根目录
            recursive: 是否递归列出子目录
        
        Returns:
            文件列表
        """
        try:
            contents = self.repo.get_contents(path, ref=self.branch)
            if not isinstance(contents, list):
                contents = [contents]
            
            files = []
            for content in contents:
                file_info = {
                    "name": content.name,
                    "path": content.path,
                    "type": content.type,
                    "size": content.size if content.type == "file" else 0,
                    "sha": content.sha,
                    "url": content.html_url
                }
                files.append(file_info)
                
                # 递归处理子目录
                if recursive and content.type == "dir":
                    subfiles = self.list_files(content.path, recursive=True)
                    files.extend(subfiles)
            
            return files
            
        except GithubException as e:
            logger.error(f"列出文件失败: {e}")
            return []
    
    def get_commit_history(self, file_path: Optional[str] = None, 
                          max_count: int = 20, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取文件或仓库的提交历史
        
        Args:
            file_path: 特定文件路径，如果为None则获取整个仓库的提交历史
            max_count: 返回的最大提交数量
            branch: 分支名称，默认使用初始化时的分支
            
        Returns:
            提交历史列表，包含sha、message、author、date等信息
        """
        try:
            target_branch = branch or self.branch
            commits = []
            
            if file_path:
                # 获取特定文件的提交历史
                commit_list = self.repo.get_commits(path=file_path, sha=target_branch)
            else:
                # 获取整个仓库的提交历史
                commit_list = self.repo.get_commits(sha=target_branch)
            
            # 限制返回数量
            count = 0
            for commit in commit_list:
                if count >= max_count:
                    break
                    
                commit_info = {
                    "sha": commit.sha,
                    "message": commit.commit.message,
                    "author": {
                        "name": commit.commit.author.name,
                        "email": commit.commit.author.email,
                        "date": commit.commit.author.date.isoformat()
                    },
                    "committer": {
                        "name": commit.commit.committer.name,
                        "email": commit.commit.committer.email,
                        "date": commit.commit.committer.date.isoformat()
                    },
                    "url": commit.html_url,
                    "parents": [p.sha for p in commit.parents]
                }
                commits.append(commit_info)
                count += 1
            
            logger.info(f"获取了 {len(commits)} 个提交记录")
            return commits
            
        except GithubException as e:
            logger.error(f"获取提交历史失败: {e}")
            return []
        except Exception as e:
            logger.error(f"获取提交历史时发生异常: {e}")
            return []
    
    def get_file_at_commit(self, file_path: str, commit_sha: str) -> Optional[str]:
        """
        获取文件在指定提交时的内容
        
        Args:
            file_path: 文件路径
            commit_sha: 提交SHA
            
        Returns:
            文件内容，如果失败返回None
        """
        try:
            # 获取指定提交
            commit = self.repo.get_commit(commit_sha)
            
            # 获取该提交时的文件内容
            try:
                content_file = self.repo.get_contents(file_path, ref=commit.sha)
                
                if isinstance(content_file, list):
                    logger.error(f"{file_path} 是一个目录，不是文件")
                    return None
                
                # 解码文件内容
                content = base64.b64decode(content_file.content).decode('utf-8')
                logger.info(f"成功获取文件 {file_path} 在提交 {commit_sha[:7]} 时的内容")
                return content
                
            except GithubException as e:
                if e.status == 404:
                    logger.error(f"文件 {file_path} 在提交 {commit_sha[:7]} 时不存在")
                else:
                    logger.error(f"获取文件内容失败: {e}")
                return None
                
        except GithubException as e:
            if e.status == 404:
                logger.error(f"未找到提交: {commit_sha}")
            else:
                logger.error(f"获取提交失败: {e}")
            return None
        except Exception as e:
            logger.error(f"获取文件历史版本时发生异常: {e}")
            return None
    
    def rollback_file(self, file_path: str, target_commit_sha: str, 
                     message: Optional[str] = None, branch: Optional[str] = None) -> Dict[str, Any]:
        """
        将文件回滚到指定提交时的版本
        
        Args:
            file_path: 文件路径
            target_commit_sha: 目标提交SHA
            message: 提交消息，默认自动生成
            branch: 目标分支
            
        Returns:
            操作结果字典
        """
        try:
            target_branch = branch or self.branch
            
            # 获取当前文件内容作为备份
            current_content = None
            try:
                current_file = self.repo.get_contents(file_path, ref=target_branch)
                if not isinstance(current_file, list):
                    current_content = base64.b64decode(current_file.content).decode('utf-8')
                    current_sha = current_file.sha
                else:
                    return {
                        "status": "error",
                        "message": f"{file_path} 是一个目录，不是文件"
                    }
            except GithubException:
                # 文件当前不存在，可能是要恢复已删除的文件
                current_content = None
                current_sha = None
            
            # 获取目标版本的内容
            target_content = self.get_file_at_commit(file_path, target_commit_sha)
            if target_content is None:
                return {
                    "status": "error",
                    "message": f"无法获取文件 {file_path} 在提交 {target_commit_sha[:7]} 时的内容"
                }
            
            # 检查内容是否有变化
            if current_content == target_content:
                return {
                    "status": "no_change",
                    "message": "文件内容与目标版本相同，无需回滚",
                    "file_path": file_path,
                    "target_commit": target_commit_sha
                }
            
            # 构建提交消息
            if message is None:
                message = f"Rollback {file_path} to commit {target_commit_sha[:7]}"
            
            # 执行文件更新
            result = self.update_file_on_branch(
                file_path=file_path,
                content=target_content,
                message=message,
                branch=target_branch
            )
            
            if result.get("status") == "success":
                logger.info(f"成功回滚文件 {file_path} 到提交 {target_commit_sha[:7]}")
                
                # 添加回滚相关信息
                result.update({
                    "rollback_info": {
                        "target_commit": target_commit_sha,
                        "previous_content_available": current_content is not None,
                        "file_restored": current_content is None  # 如果之前文件不存在，说明是恢复操作
                    }
                })
            
            return result
            
        except Exception as e:
            error_msg = f"回滚文件时发生异常: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
    def revert_last_commit(self, file_path: str, n: int = 1, 
                          branch: Optional[str] = None) -> Dict[str, Any]:
        """
        撤销文件最近的n个提交
        
        Args:
            file_path: 文件路径
            n: 要撤销的提交数量，默认为1
            branch: 目标分支
            
        Returns:
            操作结果字典
        """
        try:
            if n < 1:
                return {
                    "status": "error",
                    "message": "撤销的提交数量必须大于0"
                }
            
            # 获取文件的提交历史
            commits = self.get_commit_history(file_path=file_path, max_count=n+1, branch=branch)
            
            if not commits:
                return {
                    "status": "error",
                    "message": f"未找到文件 {file_path} 的提交历史"
                }
            
            if len(commits) <= n:
                return {
                    "status": "error",
                    "message": f"文件 {file_path} 的提交历史不足 {n+1} 个，无法撤销 {n} 个提交"
                }
            
            # 获取目标提交（n个提交之前的版本）
            target_commit = commits[n]
            target_sha = target_commit["sha"]
            
            # 构建提交消息，包含被撤销的提交信息
            reverted_commits = commits[:n]
            commit_messages = [f"- {c['sha'][:7]}: {c['message'].split('\n')[0]}" for c in reverted_commits]
            
            if n == 1:
                message = f"Revert last commit for {file_path}\n\nReverted commit:\n" + commit_messages[0]
            else:
                message = f"Revert last {n} commits for {file_path}\n\nReverted commits:\n" + "\n".join(commit_messages)
            
            # 使用 rollback_file 执行回滚
            result = self.rollback_file(
                file_path=file_path,
                target_commit_sha=target_sha,
                message=message,
                branch=branch
            )
            
            if result.get("status") == "success":
                # 添加撤销信息
                result["revert_info"] = {
                    "reverted_count": n,
                    "reverted_commits": reverted_commits,
                    "target_commit": target_commit
                }
                logger.info(f"成功撤销文件 {file_path} 的最近 {n} 个提交")
            
            return result
            
        except Exception as e:
            error_msg = f"撤销提交时发生异常: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }
    
    def compare_versions(self, file_path: str, sha1: str, sha2: str = "HEAD") -> Dict[str, Any]:
        """
        比较文件在两个版本之间的差异
        
        Args:
            file_path: 文件路径
            sha1: 第一个版本的SHA
            sha2: 第二个版本的SHA，默认为HEAD
            
        Returns:
            包含差异信息的字典
        """
        try:
            # 获取两个版本的内容
            content1 = self.get_file_at_commit(file_path, sha1)
            if content1 is None:
                return {
                    "status": "error",
                    "message": f"无法获取文件 {file_path} 在提交 {sha1[:7]} 时的内容"
                }
            
            # 处理 sha2 为 "HEAD" 的情况
            if sha2 == "HEAD":
                try:
                    current_file = self.repo.get_contents(file_path, ref=self.branch)
                    if isinstance(current_file, list):
                        return {
                            "status": "error",
                            "message": f"{file_path} 是一个目录，不是文件"
                        }
                    content2 = base64.b64decode(current_file.content).decode('utf-8')
                    sha2_actual = self.repo.get_branch(self.branch).commit.sha
                except GithubException:
                    content2 = None
                    sha2_actual = "HEAD"
            else:
                content2 = self.get_file_at_commit(file_path, sha2)
                sha2_actual = sha2
                if content2 is None:
                    return {
                        "status": "error",
                        "message": f"无法获取文件 {file_path} 在提交 {sha2[:7]} 时的内容"
                    }
            
            # 比较内容
            if content1 == content2:
                return {
                    "status": "no_change",
                    "message": "两个版本的内容相同",
                    "file_path": file_path,
                    "sha1": sha1,
                    "sha2": sha2_actual,
                    "identical": True
                }
            
            # 计算差异统计
            lines1 = content1.split('\n') if content1 else []
            lines2 = content2.split('\n') if content2 else []
            
            # 使用 difflib 计算详细差异
            import difflib
            differ = difflib.unified_diff(
                lines1, 
                lines2, 
                fromfile=f"{file_path}@{sha1[:7]}", 
                tofile=f"{file_path}@{sha2_actual[:7] if sha2_actual != 'HEAD' else 'HEAD'}",
                lineterm=''
            )
            
            diff_lines = list(differ)
            
            # 统计变化
            added_lines = sum(1 for line in diff_lines if line.startswith('+') and not line.startswith('+++'))
            deleted_lines = sum(1 for line in diff_lines if line.startswith('-') and not line.startswith('---'))
            
            # 获取提交信息
            try:
                commit1 = self.repo.get_commit(sha1)
                commit1_info = {
                    "sha": commit1.sha,
                    "message": commit1.commit.message,
                    "author": commit1.commit.author.name,
                    "date": commit1.commit.author.date.isoformat()
                }
            except:
                commit1_info = {"sha": sha1}
            
            if sha2_actual != "HEAD":
                try:
                    commit2 = self.repo.get_commit(sha2_actual)
                    commit2_info = {
                        "sha": commit2.sha,
                        "message": commit2.commit.message,
                        "author": commit2.commit.author.name,
                        "date": commit2.commit.author.date.isoformat()
                    }
                except:
                    commit2_info = {"sha": sha2_actual}
            else:
                commit2_info = {"sha": "HEAD", "message": "当前工作区版本"}
            
            return {
                "status": "success",
                "file_path": file_path,
                "commits": {
                    "from": commit1_info,
                    "to": commit2_info
                },
                "statistics": {
                    "added_lines": added_lines,
                    "deleted_lines": deleted_lines,
                    "total_changes": added_lines + deleted_lines
                },
                "diff": "\n".join(diff_lines),
                "has_changes": True
            }
            
        except Exception as e:
            error_msg = f"比较版本时发生异常: {str(e)}"
            logger.error(error_msg)
            return {
                "status": "error",
                "message": error_msg
            }