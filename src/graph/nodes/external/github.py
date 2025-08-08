"""
GitHub集成节点
负责将代码推送到GitHub仓库
"""

import logging
from src.models.states import EDWState
from src.basic.github import GitHubTool

logger = logging.getLogger(__name__)


def github_push_node(state: EDWState):
    """将AI修改的代码推送到GitHub远程仓库"""
    logger.info("模拟更新github 成功")
    return {}
    
    # 实际实现代码（暂时禁用）
    try:
        # 从状态中获取必要信息
        enhanced_code = state.get("enhance_code", "")  # 增强后的代码
        code_path = state.get("code_path", "")  # 原始代码路径
        table_name = state.get("table_name", "")
        user_id = state.get("user_id", "")
        
        # 验证必要信息
        if not enhanced_code:
            error_msg = "缺少增强后的代码，无法推送到GitHub"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        if not code_path:
            error_msg = "缺少代码文件路径，无法推送到GitHub"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        logger.info(f"准备将增强后的代码推送到GitHub: {code_path}")
        
        # 初始化GitHub工具
        try:
            github_tool = GitHubTool()
        except Exception as e:
            error_msg = f"初始化GitHub工具失败: {str(e)}"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e)},
                "error_message": error_msg  # 向后兼容
            }
        
        # 推送代码到GitHub
        try:
            # 使用固定的提交信息 "AI Code"
            commit_message = "AI Code"
            
            # 调用GitHub工具的commit_file方法
            result = github_tool.commit_file(
                file_path=code_path,
                content=enhanced_code,
                message=commit_message
            )
            
            # 检查推送结果
            if result.get("status") == "success":
                success_msg = f"成功推送代码到GitHub: {table_name}"
                logger.info(success_msg)
                
                return {
                    "user_id": user_id,
                    "status": "success",
                    "status_message": success_msg,
                    "status_details": {
                        "commit_sha": result.get("commit", {}).get("sha", ""),
                        "commit_url": result.get("commit", {}).get("url", ""),
                        "file_url": result.get("file", {}).get("url", ""),
                        "table_name": table_name,
                        "code_path": code_path
                    },
                    # 保留这些字段供后续节点使用
                    "github_commit_sha": result.get("commit", {}).get("sha", ""),
                    "github_commit_url": result.get("commit", {}).get("url", ""),
                    "github_file_url": result.get("file", {}).get("url", "")
                }
            elif result.get("status") == "no_change":
                info_msg = "代码内容未发生变化，无需推送"
                logger.info(info_msg)
                return {
                    "user_id": user_id,
                    "status": "no_change",
                    "status_message": info_msg
                }
            else:
                error_msg = result.get("message", "GitHub推送失败")
                logger.error(f"GitHub推送失败: {error_msg}")
                return {
                    "user_id": user_id,
                    "status": "error",
                    "status_message": f"推送失败: {error_msg}",
                    "status_details": {"result": result},
                    "error_message": error_msg  # 向后兼容
                }
                
        except Exception as e:
            error_msg = f"推送到GitHub时发生异常: {str(e)}"
            logger.error(error_msg)
            return {
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e), "code_path": code_path},
                "error_message": error_msg  # 向后兼容
            }
            
    except Exception as e:
        error_msg = f"GitHub推送节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "user_id": state.get("user_id", ""),
            "status": "error",
            "status_message": error_msg,
            "status_details": {"exception": str(e)},
            "error_message": error_msg  # 向后兼容
        }