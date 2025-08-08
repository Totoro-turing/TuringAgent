"""
EDW图流式服务适配器 - 桥接Flask和LangGraph

支持功能：
1. 流式执行EDW工作流
2. 处理LangGraph的interrupt机制
3. 双通道通信（SSE文本流 + SocketIO状态）
4. 会话状态管理
"""

import asyncio
import json
import uuid
from typing import AsyncGenerator, Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
from src.graph.edw_graph import guid, EDWState, SessionManager
from src.agent.edw_agents import get_agent_manager
from langchain.schema.messages import HumanMessage, AIMessage, SystemMessage
import logging

logger = logging.getLogger(__name__)


@dataclass
class EDWStreamConfig:
    """EDW流式服务配置"""
    session_id: str
    user_id: str
    socket_queue: Optional[Any] = None  # SocketIOAgentMessageQueue（可选）


class EDWStreamService:
    """EDW图流式执行服务 - 处理与LangGraph的交互"""

    def __init__(self, config: EDWStreamConfig):
        self.config = config
        self.current_thread_id = None
        self.is_interrupted = False
        self.interrupt_data = None
        self.current_state = None
        self.workflow_active = False

    async def stream_workflow(self, user_message: str) -> AsyncGenerator[Dict, None]:
        """
        流式执行EDW工作流，生成SSE格式的数据

        Args:
            user_message: 用户输入消息

        Yields:
            Dict: SSE格式的数据块，包含type、content、session_id等字段
        """

        try:
            # 1. 创建初始状态
            initial_state = {
                "messages": [HumanMessage(content=user_message)],
                "user_id": self.config.user_id,
                "type": None  # 由导航节点决定任务类型
            }

            # 2. 获取LangGraph配置
            graph_config = SessionManager.get_config(self.config.user_id, "main")
            self.current_thread_id = graph_config["configurable"]["thread_id"]

            # 3. 通过SocketIO推送工作流开始事件
            if self.config.socket_queue:
                self.config.socket_queue.send_message(
                    self.config.session_id,
                    "workflow_start",
                    {
                        "message": "EDW工作流开始执行",
                        "thread_id": self.current_thread_id,
                        "timestamp": datetime.now().isoformat()
                    }
                )

            self.workflow_active = True

            # 4. 流式执行图
            async for chunk in guid.astream(initial_state, graph_config, stream_mode="updates"):
                # 处理每个节点的输出
                for node_name, node_output in chunk.items():

                    # 保存当前状态
                    self.current_state = node_output

                    # 通过SocketIO推送节点状态
                    if self.config.socket_queue:
                        await self._push_node_update(node_name, node_output)

                    # 根据节点类型处理流式输出
                    async for output_chunk in self._process_node_output(node_name, node_output):
                        yield output_chunk

                    # 检查是否有中断
                    if self._check_interrupt(node_output):
                        self.is_interrupted = True
                        self.interrupt_data = node_output

                        # 返回中断提示
                        yield {
                            "type": "interrupt",
                            "prompt": self._extract_interrupt_prompt(node_output),
                            "node": node_name,
                            "session_id": self.config.session_id
                        }

                        # 中断后暂停执行
                        logger.info(f"工作流在节点 {node_name} 中断，等待用户输入")
                        return

            # 工作流完成
            self.workflow_active = False

            # 推送完成事件
            if self.config.socket_queue:
                self.config.socket_queue.send_message(
                    self.config.session_id,
                    "workflow_complete",
                    {
                        "message": "工作流执行完成",
                        "timestamp": datetime.now().isoformat()
                    }
                )

            yield {
                "type": "done",
                "session_id": self.config.session_id,
                "message": "工作流执行完成"
            }

        except Exception as e:
            logger.error(f"工作流执行错误: {e}", exc_info=True)
            self.workflow_active = False

            yield {
                "type": "error",
                "error": str(e),
                "session_id": self.config.session_id
            }

    async def resume_from_interrupt(self, user_input: str) -> AsyncGenerator[Dict, None]:
        """
        从中断点恢复流式执行

        Args:
            user_input: 用户对中断的响应

        Yields:
            Dict: SSE格式的数据块
        """

        if not self.is_interrupted:
            yield {
                "type": "error",
                "error": "当前没有待处理的中断",
                "session_id": self.config.session_id
            }
            return

        try:
            # 获取配置
            graph_config = SessionManager.get_config(self.config.user_id, "main")

            # 构建恢复状态 - 注意这里的状态更新
            resume_state = {
                "user_refinement_input": user_input,  # 微调输入
                "messages": [HumanMessage(content=user_input)]  # 添加用户消息
            }

            # 重置中断状态
            self.is_interrupted = False
            self.interrupt_data = None

            # 推送恢复事件
            if self.config.socket_queue:
                self.config.socket_queue.send_message(
                    self.config.session_id,
                    "workflow_resume",
                    {
                        "message": "工作流恢复执行",
                        "user_input": user_input,
                        "timestamp": datetime.now().isoformat()
                    }
                )

            # 继续流式执行
            async for chunk in guid.astream(resume_state, graph_config, stream_mode="updates"):
                for node_name, node_output in chunk.items():

                    # 保存当前状态
                    self.current_state = node_output

                    # 推送节点更新
                    if self.config.socket_queue:
                        await self._push_node_update(node_name, node_output)

                    # 处理节点输出
                    async for output_chunk in self._process_node_output(node_name, node_output):
                        yield output_chunk

                    # 再次检查中断
                    if self._check_interrupt(node_output):
                        self.is_interrupted = True
                        self.interrupt_data = node_output

                        yield {
                            "type": "interrupt",
                            "prompt": self._extract_interrupt_prompt(node_output),
                            "node": node_name,
                            "session_id": self.config.session_id
                        }
                        return

            # 完成
            yield {
                "type": "done",
                "session_id": self.config.session_id,
                "message": "微调完成"
            }

        except Exception as e:
            logger.error(f"恢复执行错误: {e}", exc_info=True)
            yield {
                "type": "error",
                "error": str(e),
                "session_id": self.config.session_id
            }

    async def _process_node_output(self, node_name: str, node_output: Dict) -> AsyncGenerator[Dict, None]:
        """
        处理不同节点的输出，生成相应的流式数据

        Args:
            node_name: 节点名称
            node_output: 节点输出数据

        Yields:
            Dict: 处理后的输出数据
        """

        # 导航节点 - 返回任务分类结果
        if node_name == "navigate_node":
            task_type = node_output.get("type", "unknown")
            # 只记录日志，不输出到流（让后续节点处理实际输出）
            logger.info(f"导航节点识别任务类型: {task_type}")
            # 如果是other类型，表示将进入chat_node
            # 如果是model_dev类型，表示将进入model_node

        # 聊天节点 - 流式返回AI响应（普通聊天）
        elif node_name == "chat_node":
            async for text_chunk in self._stream_chat_content(node_output):
                yield text_chunk

        # 验证子图 - 返回验证进度
        elif node_name == "validation_subgraph":
            validation_status = node_output.get("validation_status", "processing")
            yield {
                "type": "validation_progress",
                "status": validation_status,
                "message": node_output.get("status_message", "正在验证信息..."),
                "session_id": self.config.session_id
            }

        # 代码增强节点 - 流式返回增强过程和结果
        elif node_name == "model_enhance_node":
            async for progress_chunk in self._stream_enhancement_progress(node_output):
                yield progress_chunk

        # 属性名称review节点 - 返回属性review结果
        elif node_name == "attribute_review_subgraph" or node_name == "attribute_review":
            avg_score = node_output.get("attribute_avg_score", 0)
            review_results = node_output.get("attribute_review_results", [])
            improvements_applied = node_output.get("attribute_improvements_applied", False)
            
            yield {
                "type": "attribute_review",
                "avg_score": avg_score,
                "review_results": review_results,
                "improvements_applied": improvements_applied,
                "message": "属性命名review完成" if improvements_applied else "保持原有属性命名",
                "session_id": self.config.session_id
            }
        
        # 代码review节点 - 返回review结果
        elif node_name == "code_review_subgraph" or node_name == "review":
            review_score = node_output.get("review_score", 0)
            review_feedback = node_output.get("review_feedback", "")
            review_suggestions = node_output.get("review_suggestions", [])
            review_round = node_output.get("review_round", 1)
            
            yield {
                "type": "code_review",
                "score": review_score,
                "feedback": review_feedback,
                "suggestions": review_suggestions,
                "round": review_round,
                "session_id": self.config.session_id
            }
        
        # 代码重新生成节点 - 返回改进进度
        elif node_name == "regenerate":
            yield {
                "type": "code_regeneration",
                "status": node_output.get("status", "processing"),
                "message": node_output.get("status_message", "正在根据review建议重新生成代码..."),
                "session_id": self.config.session_id
            }
        
        # 微调节点 - 返回微调后的代码
        elif node_name == "code_refinement_node":
            refined_code = node_output.get("enhance_code", "")
            if refined_code:
                yield {
                    "type": "refined_code",
                    "content": refined_code,
                    "round": node_output.get("current_refinement_round", 1),
                    "session_id": self.config.session_id
                }

        # GitHub推送节点
        elif node_name == "github_push_node":
            yield {
                "type": "github_push",
                "status": node_output.get("status", "processing"),
                "message": node_output.get("status_message", "正在推送到GitHub..."),
                "pr_url": node_output.get("pr_url", ""),
                "session_id": self.config.session_id
            }

        # ADB更新节点
        elif node_name == "adb_update_node":
            yield {
                "type": "adb_update",
                "status": node_output.get("status", "processing"),
                "message": node_output.get("status_message", "正在更新ADB..."),
                "session_id": self.config.session_id
            }

        # Confluence节点
        elif node_name == "confluence_node":
            yield {
                "type": "confluence_update",
                "status": node_output.get("status", "processing"),
                "page_url": node_output.get("confluence_page_url", ""),
                "session_id": self.config.session_id
            }

        # 默认节点输出
        else:
            yield {
                "type": "node_update",
                "node": node_name,
                "status": node_output.get("status", "processing"),
                "message": node_output.get("status_message", ""),
                "session_id": self.config.session_id
            }

    async def _stream_chat_content(self, node_output: Dict) -> AsyncGenerator[Dict, None]:
        """流式输出聊天内容"""
        messages = node_output.get("messages", [])

        for msg in messages:
            if isinstance(msg, AIMessage):
                content = msg.content
                # 按字符分块，模拟打字效果
                chunk_size = 10  # 每次输出10个字符
                for i in range(0, len(content), chunk_size):
                    chunk = content[i:i + chunk_size]
                    yield {
                        "type": "content",
                        "content": chunk,
                        "session_id": self.config.session_id
                    }
                    await asyncio.sleep(0.02)  # 20ms延迟，模拟打字

    async def _stream_enhancement_progress(self, node_output: Dict) -> AsyncGenerator[Dict, None]:
        """流式输出增强进度"""

        # 模拟进度步骤
        steps = [
            {"step": "分析表结构", "progress": 20},
            {"step": "识别字段关系", "progress": 40},
            {"step": "生成字段定义", "progress": 60},
            {"step": "优化代码逻辑", "progress": 80},
            {"step": "添加注释文档", "progress": 90},
            {"step": "完成代码增强", "progress": 100}
        ]

        for step_info in steps:
            yield {
                "type": "progress",
                "step": step_info["step"],
                "progress": step_info["progress"],
                "session_id": self.config.session_id
            }
            await asyncio.sleep(0.2)  # 模拟处理时间

        # 最后输出增强后的代码
        enhanced_code = node_output.get("enhance_code", "")
        if enhanced_code:
            # 分块输出代码
            yield {
                "type": "enhanced_code",
                "content": enhanced_code,
                "table_name": node_output.get("table_name", ""),
                "session_id": self.config.session_id
            }

    async def _push_node_update(self, node_name: str, node_output: Dict):
        """通过SocketIO推送节点更新"""
        if not self.config.socket_queue:
            return

        # 节点元数据映射
        node_meta = self._get_node_metadata(node_name)

        # 构建推送数据
        push_data = {
            "node": node_name,
            "meta": node_meta,
            "status": node_output.get("status", "processing"),
            "message": node_output.get("status_message", ""),
            "timestamp": datetime.now().isoformat()
        }

        # 添加特定节点的额外信息
        if node_name == "validation_subgraph":
            push_data["validation_status"] = node_output.get("validation_status", "")
            push_data["missing_info"] = node_output.get("missing_info", [])

        elif node_name == "model_enhance_node":
            push_data["table_name"] = node_output.get("table_name", "")
            push_data["fields_count"] = len(node_output.get("fields", []))

        self.config.socket_queue.send_message(
            self.config.session_id,
            "node_progress",
            push_data
        )

    def _get_node_metadata(self, node_name: str) -> Dict:
        """获取节点元数据"""
        metadata_map = {
            "navigate_node": {"icon": "🧭", "label": "任务分类", "color": "#4CAF50"},
            "chat_node": {"icon": "💬", "label": "智能对话", "color": "#2196F3"},
            "validation_subgraph": {"icon": "✅", "label": "信息验证", "color": "#FF9800"},
            "attribute_review_subgraph": {"icon": "📝", "label": "属性命名Review", "color": "#00BCD4"},
            "attribute_review": {"icon": "✏️", "label": "属性评估", "color": "#00ACC1"},
            "model_enhance_node": {"icon": "🚀", "label": "代码增强", "color": "#9C27B0"},
            "code_review_subgraph": {"icon": "🔍", "label": "代码Review", "color": "#FF5722"},
            "review": {"icon": "📊", "label": "质量评估", "color": "#FF5722"},
            "regenerate": {"icon": "🔧", "label": "代码改进", "color": "#FF6F00"},
            "code_refinement_node": {"icon": "✨", "label": "代码微调", "color": "#00BCD4"},
            "refinement_inquiry_node": {"icon": "💭", "label": "微调询问", "color": "#FFC107"},
            "refinement_intent_node": {"icon": "🎯", "label": "意图识别", "color": "#795548"},
            "github_push_node": {"icon": "📤", "label": "推送GitHub", "color": "#607D8B"},
            "adb_update_node": {"icon": "🔄", "label": "更新ADB", "color": "#E91E63"},
            "confluence_node": {"icon": "📝", "label": "生成文档", "color": "#3F51B5"},
            "email_node": {"icon": "📧", "label": "发送邮件", "color": "#009688"}
        }
        return metadata_map.get(node_name, {
            "icon": "⚙️",
            "label": node_name.replace("_", " ").title(),
            "color": "#757575"
        })

    def _get_task_type_label(self, task_type: str) -> str:
        """获取任务类型的中文标签"""
        type_labels = {
            "model_enhance": "模型增强",
            "model_add": "新增模型",
            "chat": "智能对话",
            "other": "其他任务"
        }
        return type_labels.get(task_type, task_type)

    def _check_interrupt(self, node_output: Dict) -> bool:
        """检查节点输出是否包含中断信号"""
        # 检查是否有中断标志
        if node_output.get("interrupt", False):
            return True

        # 检查特定的中断节点
        if "refinement_inquiry_node" in str(node_output):
            return True

        # 检查是否有用户输入请求
        if node_output.get("action_type") == "refinement_conversation":
            return True

        return False

    def _extract_interrupt_prompt(self, node_output: Dict) -> str:
        """从节点输出中提取中断提示"""
        # 优先使用明确的prompt字段
        if "prompt" in node_output:
            return node_output["prompt"]

        # 从消息中提取
        messages = node_output.get("messages", [])
        for msg in reversed(messages):
            if isinstance(msg, (AIMessage, SystemMessage)):
                return msg.content

        # 默认提示
        return "请提供您的反馈或输入"

    def get_status(self) -> Dict:
        """获取服务当前状态"""
        return {
            "session_id": self.config.session_id,
            "user_id": self.config.user_id,
            "thread_id": self.current_thread_id,
            "workflow_active": self.workflow_active,
            "is_interrupted": self.is_interrupted,
            "has_interrupt_data": self.interrupt_data is not None
        }

    def cleanup(self):
        """清理服务资源"""
        self.current_state = None
        self.interrupt_data = None
        self.workflow_active = False
        logger.info(f"EDW服务清理完成: {self.config.session_id}")
