"""
工具调用监控模块

为LangGraph Agent提供工具调用的实时监控和进度反馈
"""

import logging
import time
import json
import ast
from typing import Dict, List, Any, Optional
from langchain.callbacks.base import BaseCallbackHandler
from langchain.schema import AgentAction, AgentFinish, LLMResult
from langchain.schema.messages import BaseMessage

from src.models.states import EDWState
from src.graph.utils.progress import send_progress

logger = logging.getLogger(__name__)


class ToolCallMonitor(BaseCallbackHandler):
    """
    基础工具调用监控器
    
    监控Agent的工具调用过程，实时发送进度更新
    """
    
    def __init__(self, state: EDWState, node_name: str = "unknown"):
        """
        初始化监控器
        
        Args:
            state: 工作流状态对象
            node_name: 节点名称，用于标识消息来源
        """
        super().__init__()
        self.state = state
        self.node_name = node_name
        self.current_tools = []
        self.tool_call_count = 0
        self.tool_start_times = {}
        
    def on_agent_action(self, action: AgentAction, **kwargs: Any) -> Any:
        """当Agent决定使用工具时触发"""
        try:
            self.tool_call_count += 1
            tool_name = action.tool
            tool_input = action.tool_input
            
            # 记录工具调用
            self.current_tools.append({
                "name": tool_name,
                "input": tool_input,
                "status": "calling"
            })
            
            # 发送工具调用决策消息（简化版，详细参数在on_tool_start中显示）
            send_progress(
                self.state,
                self.node_name,
                "processing",
                f"🔧 Agent决定使用工具: **{tool_name}**",
                0.0,
                {
                    "action": "tool_decision",
                    "tool_name": tool_name,
                    "tool_input_summary": self._get_input_summary(tool_input)
                }
            )
            
            logger.info(f"Agent决定调用工具: {tool_name}")
            
        except Exception as e:
            logger.error(f"监控工具调用开始失败: {e}")
    
    def _get_input_summary(self, tool_input: Any) -> str:
        """获取工具输入的简要摘要"""
        try:
            if isinstance(tool_input, dict):
                # 返回主要参数的键名
                keys = list(tool_input.keys())[:3]
                return f"参数: {', '.join(keys)}" + (f" 等{len(tool_input)}个" if len(tool_input) > 3 else "")
            elif isinstance(tool_input, str):
                return f"文本参数 ({len(tool_input)}字符)"
            else:
                return "其他参数"
        except Exception:
            return "参数摘要不可用"
    
    def on_tool_start(
        self, 
        serialized: Dict[str, Any], 
        input_str: str, 
        **kwargs: Any
    ) -> Any:
        """工具开始执行时触发"""
        try:
            tool_name = serialized.get("name", "unknown_tool")
            run_id = kwargs.get("run_id", "")
            
            # 记录开始时间
            self.tool_start_times[run_id] = time.time()
            
            # 简化参数处理 - 只尝试解析为字典，不做复杂判断
            tool_input = None
            try:
                if input_str and input_str.strip():
                    # 尝试使用ast.literal_eval（处理Python dict格式）
                    if input_str.strip().startswith('{') or input_str.strip().startswith('['):
                        try:
                            tool_input = ast.literal_eval(input_str)
                        except:
                            # 失败则尝试JSON
                            try:
                                tool_input = json.loads(input_str)
                            except:
                                # 都失败就用原始字符串
                                tool_input = {"input": input_str}
                    else:
                        tool_input = {"input": input_str}
            except Exception as e:
                logger.debug(f"解析工具输入失败: {e}")
                tool_input = {"input": input_str[:500] if input_str else ""}
            
            # 简单的消息 - 只显示工具名称，参数通过socket发送给前端
            message = f"⚙️ 执行工具: **{tool_name}**"
            
            send_progress(
                self.state,
                self.node_name,
                "processing",
                message,
                0.0,
                {
                    "action": "tool_executing",
                    "tool_name": tool_name,
                    "tool_input": tool_input,  # 完整参数通过socket发送，前端决定是否显示
                    "show_on_hover": True  # 标记为悬停显示
                }
            )
            
        except Exception as e:
            logger.error(f"监控工具执行开始失败: {e}")
    
    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        """工具执行完成时触发"""
        try:
            run_id = kwargs.get("run_id", "")
            
            # 计算执行时间
            duration = 0
            if run_id in self.tool_start_times:
                duration = time.time() - self.tool_start_times[run_id]
                del self.tool_start_times[run_id]
            
            # 更新最后一个工具的状态
            if self.current_tools:
                self.current_tools[-1]["status"] = "completed"
                self.current_tools[-1]["output"] = self._sanitize_output(output)
                
                tool_name = self.current_tools[-1]["name"]
                
                send_progress(
                    self.state,
                    self.node_name,
                    "processing",
                    f"✅ 工具 **{tool_name}** 执行完成 ({duration:.1f}秒)",
                    0.0,
                    {
                        "action": "tool_complete",
                        "tool_name": tool_name,
                        "duration": duration,
                        "output_preview": self._get_output_preview(output)
                    }
                )
                
        except Exception as e:
            logger.error(f"监控工具执行完成失败: {e}")
    
    def on_tool_error(self, error: Exception, **kwargs: Any) -> Any:
        """工具执行出错时触发"""
        try:
            # 更新最后一个工具的状态
            if self.current_tools:
                self.current_tools[-1]["status"] = "error"
                self.current_tools[-1]["error"] = str(error)
                
                tool_name = self.current_tools[-1]["name"]
                
                send_progress(
                    self.state,
                    self.node_name,
                    "failed",
                    f"❌ 工具 **{tool_name}** 执行失败: {str(error)}",
                    0.0,
                    {
                        "action": "tool_error",
                        "tool_name": tool_name,
                        "error": str(error)
                    }
                )
                
        except Exception as e:
            logger.error(f"监控工具错误失败: {e}")
    
    def on_agent_finish(self, finish: AgentFinish, **kwargs: Any) -> Any:
        """Agent完成时触发"""
        try:
            send_progress(
                self.state,
                self.node_name,
                "completed",
                f"🤖 Agent完成任务，共使用 {len(self.current_tools)} 个工具",
                1.0,
                {
                    "action": "agent_finish",
                    "tools_used": len(self.current_tools)
                }
            )
            
        except Exception as e:
            logger.error(f"监控Agent完成失败: {e}")
    
    def _sanitize_input(self, tool_input: Any) -> Dict[str, Any]:
        """清理工具输入以便安全传输 - 简化版本"""
        try:
            if isinstance(tool_input, dict):
                # 简单截断过长的值
                sanitized = {}
                for key, value in tool_input.items():
                    if isinstance(value, str) and len(value) > 500:
                        sanitized[key] = value[:500] + f"... (共{len(value)}字符)"
                    elif isinstance(value, (list, dict)):
                        json_str = json.dumps(value, ensure_ascii=False)
                        if len(json_str) > 500:
                            sanitized[key] = json_str[:500] + "..."
                        else:
                            sanitized[key] = value
                    else:
                        sanitized[key] = value
                return sanitized
            else:
                # 非字典类型直接返回
                return {"input": str(tool_input)[:500] if len(str(tool_input)) > 500 else str(tool_input)}
        except Exception:
            return {"input": "无法解析"}
    
    def _sanitize_output(self, output: str) -> str:
        """清理工具输出"""
        try:
            return output[:500] + "..." if len(output) > 500 else output
        except Exception:
            return "无法解析输出"
    
    def _get_output_preview(self, output: str) -> str:
        """获取输出预览"""
        try:
            if len(output) <= 100:
                return output
            return output[:100] + "..."
        except Exception:
            return "无法预览"
    


class EnhancedToolMonitor(ToolCallMonitor):
    """
    增强型工具调用监控器
    
    提供更详细的监控信息和统计数据
    """
    
    def __init__(self, state: EDWState, node_name: str = "unknown", enable_detailed_logging: bool = True):
        super().__init__(state, node_name)
        self.enable_detailed_logging = enable_detailed_logging
        self.execution_stats = {
            "total_tools": 0,
            "successful_tools": 0,
            "failed_tools": 0,
            "start_time": None,
            "end_time": None
        }
        
    def on_llm_start(
        self, 
        serialized: Dict[str, Any], 
        prompts: List[str], 
        **kwargs: Any
    ) -> Any:
        """LLM开始时触发 - 已禁用监控以减少消息量"""
        # 注释掉LLM监控以减少消息过多的问题
        pass
    
    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> Any:
        """LLM完成时触发 - 已禁用监控以减少消息量"""
        # 注释掉LLM监控以减少消息过多的问题
        pass
    
    def on_agent_action(self, action: AgentAction, **kwargs: Any) -> Any:
        """增强的Agent动作监控"""
        try:
            # 调用父类方法
            super().on_agent_action(action, **kwargs)
            
            # 更新统计
            self.execution_stats["total_tools"] += 1
            
            # 发送额外的推理信息（如果有）
            if self.enable_detailed_logging and action.log:
                reasoning = self._extract_reasoning(action.log)
                if reasoning and reasoning != "无推理信息":
                    send_progress(
                        self.state,
                        self.node_name,
                        "processing",
                        f"💭 推理: {reasoning}",
                        0.0,
                        {
                            "action": "tool_reasoning",
                            "tool_name": action.tool,
                            "reasoning": reasoning
                        }
                    )
                
        except Exception as e:
            logger.error(f"增强监控Agent动作失败: {e}")
    
    def on_tool_start(
        self, 
        serialized: Dict[str, Any], 
        input_str: str, 
        **kwargs: Any
    ) -> Any:
        """增强的工具开始执行监控 - 提供更详细的参数展示"""
        try:
            # 调用父类方法，已经发送了基本的工具执行信息
            super().on_tool_start(serialized, input_str, **kwargs)
            
            # 如果是第一个工具，记录开始时间
            if self.execution_stats["start_time"] is None:
                self.execution_stats["start_time"] = time.time()
            
        except Exception as e:
            logger.error(f"增强监控工具开始失败: {e}")
    
    def on_tool_end(self, output: str, **kwargs: Any) -> Any:
        """增强的工具完成监控"""
        try:
            # 调用父类方法
            super().on_tool_end(output, **kwargs)
            
            # 更新统计
            self.execution_stats["successful_tools"] += 1
            
        except Exception as e:
            logger.error(f"增强监控工具完成失败: {e}")
    
    def on_tool_error(self, error: Exception, **kwargs: Any) -> Any:
        """增强的工具错误监控"""
        try:
            # 调用父类方法
            super().on_tool_error(error, **kwargs)
            
            # 更新统计
            self.execution_stats["failed_tools"] += 1
            
        except Exception as e:
            logger.error(f"增强监控工具错误失败: {e}")
    
    def on_agent_finish(self, finish: AgentFinish, **kwargs: Any) -> Any:
        """增强的Agent完成监控"""
        try:
            # 生成执行摘要
            stats = self.execution_stats
            success_rate = (stats["successful_tools"] / stats["total_tools"] * 100) if stats["total_tools"] > 0 else 0
            
            send_progress(
                self.state,
                self.node_name,
                "completed",
                f"📊 任务完成! 成功率: {success_rate:.1f}% ({stats['successful_tools']}/{stats['total_tools']})",
                1.0,
                {
                    "action": "execution_summary",
                    "stats": stats,
                    "success_rate": round(success_rate, 1)
                }
            )
            
        except Exception as e:
            logger.error(f"增强监控Agent完成失败: {e}")
    
    def _extract_reasoning(self, log: str) -> str:
        """从Agent日志中提取推理过程"""
        try:
            if not log:
                return "无推理信息"
            
            # 尝试提取关键推理信息
            lines = log.split('\n')
            reasoning_lines = []
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('[') and len(line) > 10:
                    reasoning_lines.append(line)
                    if len(reasoning_lines) >= 2:  # 最多提取2行
                        break
            
            if reasoning_lines:
                return " ".join(reasoning_lines)[:200]
            else:
                return log[:200] if log else "无推理信息"
                
        except Exception:
            return "推理信息解析失败"


def create_tool_monitor(
    state: EDWState,
    node_name: str = "agent",
    agent_type: str = "general",
    enhanced: bool = True
) -> BaseCallbackHandler:
    """
    创建工具监控器
    
    Args:
        state: EDW状态对象
        node_name: 节点名称
        agent_type: Agent类型
        enhanced: 是否使用增强监控器
    
    Returns:
        监控器实例
    """
    if enhanced:
        return EnhancedToolMonitor(state, node_name, agent_type)
    else:
        return ToolCallMonitor(state, node_name)