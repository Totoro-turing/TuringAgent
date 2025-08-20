"""
GitHub集成节点
负责将代码推送到GitHub仓库
"""

import logging
from langchain.schema.messages import AIMessage
from src.models.states import EDWState
from src.basic.github import GitHubTool
from src.graph.utils.progress import send_node_start, send_node_processing, send_node_completed, send_node_failed, send_node_skipped

logger = logging.getLogger(__name__)


def github_push_node(state: EDWState):
    """将AI修改的代码推送到GitHub远程仓库"""
    # 🎯 发送节点开始进度
    send_node_start(state, "github_push", "开始推送代码到GitHub...")
    
    logger.info("模拟更新github 成功")
    
    # 🎯 发送模拟成功进度
    send_node_completed(state, "github_push", "模拟推送成功（实际功能已禁用）", extra_data={"simulated": True})
    
    # 返回模拟成功的消息
    return {
        "messages": [AIMessage(content="已成功推送代码到GitHub（模拟模式）")],
        "user_id": state.get("user_id", ""),
        "status": "simulated"
    }
    
    # 实际实现代码（暂时禁用）
    try:
        # 从状态中获取必要信息
        enhanced_code = state.get("enhance_code", "")  # 增强后的代码
        code_path = state.get("code_path", "")  # 原始代码路径
        table_name = state.get("table_name", "")
        user_id = state.get("user_id", "")
        
        # 🎯 发送验证进度
        send_node_processing(state, "github_push", "验证推送参数...", 0.1)
        
        # 验证必要信息
        if not enhanced_code:
            error_msg = "缺少增强后的代码，无法推送到GitHub"
            logger.error(error_msg)
            send_node_skipped(state, "github_push", "缺少增强代码")
            return {
                "messages": [AIMessage(content=f"GitHub推送跳过: {error_msg}")],
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        if not code_path:
            error_msg = "缺少代码文件路径，无法推送到GitHub"
            logger.error(error_msg)
            send_node_skipped(state, "github_push", "缺少代码路径")
            return {
                "messages": [AIMessage(content=f"GitHub推送跳过: {error_msg}")],
                "user_id": user_id,
                "status": "skipped",
                "status_message": error_msg,
                "error_message": error_msg  # 向后兼容
            }
        
        logger.info(f"准备将增强后的代码推送到GitHub: {code_path}")
        
        # 🎯 发送初始化进度
        send_node_processing(state, "github_push", "初始化GitHub工具...", 0.3)
        
        # 初始化GitHub工具
        try:
            github_tool = GitHubTool()
        except Exception as e:
            error_msg = f"初始化GitHub工具失败: {str(e)}"
            logger.error(error_msg)
            send_node_failed(state, "github_push", error_msg)
            return {
                "messages": [AIMessage(content=f"GitHub工具初始化失败: {error_msg}")],
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e)},
                "error_message": error_msg  # 向后兼容
            }
        
        # 🎯 发送推送进度
        send_node_processing(state, "github_push", f"正在推送代码到GitHub: {table_name}", 0.7)
        
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
                
                # 🎯 发送成功进度
                send_node_completed(
                    state, 
                    "github_push", 
                    success_msg,
                    extra_data={
                        "commit_sha": result.get("commit", {}).get("sha", ""),
                        "table_name": table_name
                    }
                )
                
                # 构建成功消息
                commit_sha = result.get("commit", {}).get("sha", "")[:8] if result.get("commit", {}).get("sha") else "N/A"
                commit_url = result.get("commit", {}).get("url", "")
                
                message_content = f"已成功将代码推送到GitHub\n\n"
                message_content += f"表名: {table_name}\n"
                message_content += f"文件路径: {code_path}\n"
                message_content += f"Commit SHA: {commit_sha}\n"
                if commit_url:
                    message_content += f"Commit URL: {commit_url}"
                
                return {
                    "messages": [AIMessage(content=message_content)],
                    "user_id": user_id,
                    "status": "success",
                    "status_message": success_msg,
                    "status_details": {
                        "commit_sha": result.get("commit", {}).get("sha", ""),
                        "commit_url": commit_url,
                        "file_url": result.get("file", {}).get("url", ""),
                        "table_name": table_name,
                        "code_path": code_path
                    },
                    # 保留这些字段供后续节点使用
                    "github_commit_sha": result.get("commit", {}).get("sha", ""),
                    "github_commit_url": commit_url,
                    "github_file_url": result.get("file", {}).get("url", "")
                }
            elif result.get("status") == "no_change":
                info_msg = "代码内容未发生变化，无需推送"
                logger.info(info_msg)
                # 🎯 发送跳过进度
                send_node_skipped(state, "github_push", "代码无变化")
                return {
                    "messages": [AIMessage(content="代码内容未发生变化，无需推送到GitHub")],
                    "user_id": user_id,
                    "status": "no_change",
                    "status_message": info_msg
                }
            else:
                error_msg = result.get("message", "GitHub推送失败")
                logger.error(f"GitHub推送失败: {error_msg}")
                # 🎯 发送失败进度
                send_node_failed(state, "github_push", error_msg)
                return {
                    "messages": [AIMessage(content=f"GitHub推送失败: {error_msg}")],
                    "user_id": user_id,
                    "status": "error",
                    "status_message": f"推送失败: {error_msg}",
                    "status_details": {"result": result},
                    "error_message": error_msg  # 向后兼容
                }
                
        except Exception as e:
            error_msg = f"推送到GitHub时发生异常: {str(e)}"
            logger.error(error_msg)
            # 🎯 发送异常失败进度
            send_node_failed(state, "github_push", error_msg)
            return {
                "messages": [AIMessage(content=f"GitHub推送异常: {str(e)}")],
                "user_id": user_id,
                "status": "error",
                "status_message": error_msg,
                "status_details": {"exception": str(e), "code_path": code_path},
                "error_message": error_msg  # 向后兼容
            }
            
    except Exception as e:
        error_msg = f"GitHub推送节点处理失败: {str(e)}"
        logger.error(error_msg)
        # 🎯 发送全局异常失败进度
        send_node_failed(state, "github_push", error_msg)
        return {
            "messages": [AIMessage(content=f"GitHub节点处理失败: {str(e)}")],
            "user_id": state.get("user_id", ""),
            "status": "error",
            "status_message": error_msg,
            "status_details": {"exception": str(e)},
            "error_message": error_msg  # 向后兼容
        }