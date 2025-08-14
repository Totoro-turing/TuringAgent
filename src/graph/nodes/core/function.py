"""
EDW功能性节点
处理各种工具性任务，如名称转换、查询操作等
"""

import logging
import json
from typing import Dict, Any
from langchain.schema.messages import HumanMessage, AIMessage

from src.graph.utils.progress import send_node_start, send_node_processing, send_node_completed, send_node_failed
from src.models.states import EDWState
from src.agent.edw_agents import get_agent_manager
from src.graph.utils.session import SessionManager

logger = logging.getLogger(__name__)


async def edw_function_handler_node(state: EDWState) -> Dict[str, Any]:
    """
    EDW功能处理节点的异步实现

    处理各种工具性任务，如名称转换、查询操作等
    """
    # 发送开始进度
    send_node_start(state, "function_handler", "开始处理功能性任务...")

    try:
        logger.info("进入EDW功能性节点（异步版本）")

        # 获取Agent管理器
        agent_manager = get_agent_manager()

        # 获取功能Agent（应该在项目启动时已初始化）
        function_agent = agent_manager.agents.get('function')
        if not function_agent:
            error_msg = "功能Agent未正确初始化，请检查项目启动时的初始化流程"
            logger.error(error_msg)
            send_node_failed(state, "function_handler", error_msg)
            return {
                "messages": [AIMessage(content=f"系统错误：{error_msg}")],
                "error_message": error_msg,
                "status": "error"
            }

        # 获取用户消息
        last_message = state["messages"][-1]
        if isinstance(last_message, str):
            user_input = last_message
        else:
            user_input = last_message.content if hasattr(last_message, 'content') else str(last_message)

        logger.info(f"用户请求: {user_input[:100]}...")

        # 发送解析请求进度
        send_node_processing(state, "function_handler", f"正在解析用户需求：**{user_input[:30]}...**", 0.3)

        # 获取带监控的会话配置
        config = SessionManager.get_config_with_monitor(
            user_id=state.get("user_id", ""),
            agent_type="function",
            state=state,
            node_name="function_handler",
            enhanced_monitoring=True
        )
        # 执行Agent处理（异步调用）
        try:
            response = await function_agent.ainvoke(
                {"messages": [HumanMessage(content=user_input)]},
                config
            )

            # 发送处理完成进度
            send_node_processing(state, "function_handler", "正在整理执行结果...", 0.8)

            # 获取响应内容
            if isinstance(response, dict) and "messages" in response:
                response_messages = response["messages"]
                # 提取最后一条消息作为结果
                if response_messages:
                    final_message = response_messages[-1]
                    if hasattr(final_message, 'content'):
                        result_content = final_message.content
                    else:
                        result_content = str(final_message)
                else:
                    result_content = "任务已完成，但没有返回具体结果。"
            else:
                result_content = str(response)

            logger.info(f"功能执行成功: {result_content[:200]}...")

            # 发送完成进度
            send_node_completed(state, "function_handler", "✅ 功能任务执行完成！", {
                "result_type": "function_execution",
                "result_length": len(result_content)
            })

            # 构建响应消息
            response_message = AIMessage(content=result_content)

            return {
                "messages": [response_message],
                "function_result": result_content,
                "status": "completed"
            }

        except Exception as e:
            error_msg = f"执行功能任务失败: {str(e)}"
            logger.error(error_msg)
            send_node_failed(state, "function_handler", f"任务执行失败: {error_msg}")
            return {
                "messages": [AIMessage(content=f"执行失败：{error_msg}")],
                "error_message": error_msg,
                "status": "failed"
            }

    except Exception as e:
        error_msg = f"功能节点处理失败: {str(e)}"
        logger.error(error_msg)
        return {
            "messages": [AIMessage(content=f"系统错误：{error_msg}")],
            "error_message": error_msg,
            "status": "error"
        }
