from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import os
import json
import time
import uuid
import asyncio
import threading
import signal
import atexit
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
import logging

# 导入EDW相关模块
from src.server.edw_service import EDWStreamService, EDWStreamConfig
from src.agent.edw_agents import get_agent_manager
from pydantic import BaseModel
from openai.types.responses import ResponseTextDeltaEvent
from collections import defaultdict

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', static_url_path='/static')
app.config['SECRET_KEY'] = 'your-secret-key-here'
CORS(app, origins="*")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')


@dataclass
class ChatMessage:
    """聊天消息"""
    role: str  # 'user' 或 'assistant'
    content: str
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


class SocketIOAgentMessageQueue:
    """基于SocketIO的实时Agent消息队列"""

    def __init__(self, socketio_instance):
        self.socketio = socketio_instance
        self.session_sockets = {}  # session_id -> socket_id
        self.socket_sessions = {}  # socket_id -> session_id
        self.lock = threading.Lock()

    def register_socket(self, socket_id: str, session_id: str):
        """注册socket和会话的映射关系"""
        with self.lock:
            self.session_sockets[session_id] = socket_id
            self.socket_sessions[socket_id] = session_id
            logger.info(f"🔗 注册Socket映射: {session_id[:8]} -> {socket_id[:8]}")

    def unregister_socket(self, socket_id: str):
        """注销socket映射"""
        with self.lock:
            if socket_id in self.socket_sessions:
                session_id = self.socket_sessions[socket_id]
                del self.socket_sessions[socket_id]
                if session_id in self.session_sockets:
                    del self.session_sockets[session_id]
                logger.info(f"🔌 注销Socket映射: {socket_id[:8]}")

    def send_message(self, session_id: str, message_type: str, data: dict):
        """通过SocketIO实时发送消息"""
        try:
            message = {
                'type': message_type,
                'data': data,
                'timestamp': datetime.now().isoformat(),
                'session_id': session_id
            }

            # 查找对应的socket
            with self.lock:
                socket_id = self.session_sockets.get(session_id)

            if socket_id:
                # 通过SocketIO立即发送消息
                self.socketio.emit('agent_message', message, room=socket_id)
                logger.info(f"📨 实时发送Agent消息: {message_type} -> {session_id[:8]}")
                return True
            else:
                logger.warning(f"⚠️ 未找到会话对应的Socket: {session_id[:8]}")
                # 广播给所有连接的客户端（备用方案）
                self.socketio.emit('agent_message', message)
                return False

        except Exception as e:
            logger.error(f"❌ SocketIO发送消息失败: {e}")
            return False

    def cleanup_session(self, session_id: str):
        """清理会话"""
        with self.lock:
            if session_id in self.session_sockets:
                socket_id = self.session_sockets[session_id]
                del self.session_sockets[session_id]
                if socket_id in self.socket_sessions:
                    del self.socket_sessions[socket_id]


class SessionManager:
    """会话历史管理器 - SocketIO版本"""

    def __init__(self, max_history_per_session=50, session_timeout_hours=24):
        self.sessions = defaultdict(list)  # session_id -> List[ChatMessage]
        self.session_last_activity = {}  # session_id -> timestamp
        self.session_agents = {}  # session_id -> Agent instance
        self.max_history_per_session = max_history_per_session
        self.session_timeout_hours = session_timeout_hours

        # 启动清理线程
        self._start_cleanup_thread()

    async def get_or_create_agent(self, session_id: str, message_queue: SocketIOAgentMessageQueue) -> Any:
        """获取或创建会话绑定的Agent（简化版本）"""
        # 注：原有Agent机制暂时禁用，EDW任务使用EDWStreamService处理
        # 如果需要启用普通聊天，请实现相应的Agent创建逻辑
        return None

    def add_message(self, session_id: str, role: str, content: str) -> None:
        """添加消息到会话历史"""
        message = ChatMessage(role=role, content=content)
        self.sessions[session_id].append(message)
        self.session_last_activity[session_id] = datetime.now()

        # 限制历史记录长度
        if len(self.sessions[session_id]) > self.max_history_per_session:
            self._trim_session_history(session_id)

        logger.info(f"📝 会话 {session_id[:8]} 添加消息: {role}")

    def get_recent_messages(self, session_id: str, max_messages: int = 20) -> List[Dict]:
        """获取最近的消息记录，格式化为API调用格式"""
        if session_id not in self.sessions:
            return []

        messages = self.sessions[session_id]

        # 获取最近的消息，但确保对话的完整性
        recent_messages = []
        if messages:
            if len(messages) <= max_messages:
                recent_messages = messages
            else:
                temp_messages = messages[-max_messages:]
                if temp_messages and temp_messages[0].role == 'assistant':
                    temp_messages = temp_messages[1:]
                recent_messages = temp_messages

        # 转换为API格式
        api_messages = []
        for msg in recent_messages:
            api_messages.append({
                "role": msg.role,
                "content": msg.content
            })

        logger.info(f"📋 会话 {session_id[:8]} 获取最近 {len(api_messages)} 条消息")
        return api_messages

    def get_session_info(self, session_id: str) -> Dict:
        """获取会话信息"""
        if session_id not in self.sessions:
            return {
                "session_id": session_id,
                "message_count": 0,
                "created_at": None,
                "last_activity": None
            }

        messages = self.sessions[session_id]
        return {
            "session_id": session_id,
            "message_count": len(messages),
            "created_at": messages[0].timestamp if messages else None,
            "last_activity": self.session_last_activity.get(session_id, datetime.now()).isoformat()
        }

    def clear_session(self, session_id: str) -> bool:
        """清空指定会话的历史"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            if session_id in self.session_last_activity:
                del self.session_last_activity[session_id]
            if session_id in self.session_agents:
                # 清理Agent实例时也要清理其MCP服务器
                agent = self.session_agents[session_id]
                if hasattr(agent, 'cleanup'):
                    try:
                        agent.cleanup()
                    except Exception as e:
                        logger.error(f"❌ 清理Agent时出错: {e}")
                del self.session_agents[session_id]
            logger.info(f"🗑️ 会话 {session_id[:8]} 历史已清空")
            return True
        return False

    def cleanup_all_sessions(self) -> None:
        """清理所有会话和Agent实例"""
        logger.info("🧹 开始清理所有会话和Agent实例...")

        # 清理所有Agent实例
        for session_id, agent in self.session_agents.items():
            if hasattr(agent, 'cleanup'):
                try:
                    agent.cleanup()
                    logger.info(f"✅ 清理Agent实例: {session_id[:8]}")
                except Exception as e:
                    logger.error(f"❌ 清理Agent {session_id[:8]} 时出错: {e}")

        # 清空所有数据
        self.sessions.clear()
        self.session_last_activity.clear()
        self.session_agents.clear()

        logger.info("✅ 所有会话和Agent实例已清理完毕")

    def get_all_sessions(self) -> List[Dict]:
        """获取所有会话的基本信息"""
        sessions_info = []
        for session_id in self.sessions.keys():
            sessions_info.append(self.get_session_info(session_id))

        sessions_info.sort(key=lambda x: x["last_activity"] or "", reverse=True)
        return sessions_info

    def _trim_session_history(self, session_id: str) -> None:
        """修剪会话历史，保持对话完整性"""
        messages = self.sessions[session_id]
        if len(messages) <= self.max_history_per_session:
            return

        target_length = self.max_history_per_session - 10
        if target_length < 10:
            target_length = 10

        trimmed_messages = messages[-target_length:]
        while trimmed_messages and trimmed_messages[0].role == 'assistant':
            trimmed_messages = trimmed_messages[1:]

        self.sessions[session_id] = trimmed_messages
        logger.info(f"✂️ 会话 {session_id[:8]} 历史已修剪，保留 {len(trimmed_messages)} 条消息")

    def _cleanup_old_sessions(self) -> None:
        """清理过期的会话"""
        cutoff_time = datetime.now() - timedelta(hours=self.session_timeout_hours)
        expired_sessions = []

        for session_id, last_activity in self.session_last_activity.items():
            if last_activity < cutoff_time:
                expired_sessions.append(session_id)

        # 先清理Agent实例
        self.cleanup_expired_agents(expired_sessions)

        for session_id in expired_sessions:
            del self.sessions[session_id]
            del self.session_last_activity[session_id]

        if expired_sessions:
            logger.info(f"🧹 清理了 {len(expired_sessions)} 个过期会话")

    def cleanup_expired_agents(self, expired_sessions: List[str]) -> None:
        """清理过期会话的Agent实例"""
        for session_id in expired_sessions:
            if session_id in self.session_agents:
                agent = self.session_agents[session_id]
                if hasattr(agent, 'cleanup'):
                    try:
                        agent.cleanup()
                        logger.info(f"✅ 清理过期Agent实例: {session_id[:8]}")
                    except Exception as e:
                        logger.error(f"❌ 清理过期Agent {session_id[:8]} 时出错: {e}")
                del self.session_agents[session_id]

    def _start_cleanup_thread(self) -> None:
        """启动后台清理线程"""
        def cleanup_worker():
            while True:
                try:
                    time.sleep(3600)  # 每小时清理一次
                    self._cleanup_old_sessions()
                except Exception as e:
                    logger.error(f"会话清理线程错误: {e}")

        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
        logger.info("🧹 会话清理线程已启动")


class AIModelService:
    """AI模型服务 - 集成EDW图的SocketIO版本"""

    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager
        self.message_queue = SocketIOAgentMessageQueue(socketio)
        self.edw_stream_services = {}  # session_id -> EDWStreamService 映射

    async def general_chat_stream(self, message: str, session_id: str = None):
        """流式通用聊天 - 统一通过EDW图处理所有消息"""
        try:
            # 生成session_id如果没有提供
            if not session_id:
                session_id = f"session-{int(time.time())}-{uuid.uuid4().hex[:8]}"

            logger.info(f"🌐 处理消息: {message[:50]}... (会话: {session_id[:8]})")

            # 创建或获取EDW服务实例
            if session_id not in self.edw_stream_services:
                config = EDWStreamConfig(
                    session_id=session_id,
                    user_id=session_id,  # 这里简化处理，实际应从认证系统获取
                    socket_queue=self.message_queue
                )
                self.edw_stream_services[session_id] = EDWStreamService(config)

            service = self.edw_stream_services[session_id]

            # 检查是否是中断响应
            if service.is_interrupted:
                # 这是对中断的响应，恢复执行
                logger.info(f"📝 处理中断响应: {message[:30]}...")
                async for chunk in service.resume_from_interrupt(message):
                    yield chunk
            else:
                # 所有新消息都通过EDW图处理
                # 图内部的navigate_node会自动判断是聊天还是EDW任务
                # 并路由到相应的节点（chat_node或model_node）
                logger.info(f"🧭 通过EDW图处理消息，由导航节点自动识别任务类型")
                async for chunk in service.stream_workflow(message):
                    yield chunk

        except Exception as e:
            logger.error(f"❌ 流式处理失败: {e}", exc_info=True)

            error_msg = f"服务异常: {str(e)}"
            if session_id:
                self.session_manager.add_message(session_id, "assistant", error_msg)

            yield {
                'type': 'error',
                'error': error_msg,
                'session_id': session_id
            }


# 创建全局实例
session_manager = SessionManager(max_history_per_session=50, session_timeout_hours=24)
ai_service = AIModelService(session_manager)

# 资源清理将在文件末尾统一注册

# SocketIO事件处理


@socketio.on('connect')
def handle_connect():
    """客户端连接"""
    logger.info(f"🔗 Socket连接: {request.sid}")
    emit('connected', {'message': '连接成功', 'socket_id': request.sid})


@socketio.on('disconnect')
def handle_disconnect():
    """客户端断开连接"""
    logger.info(f"🔌 Socket断开: {request.sid}")
    ai_service.message_queue.unregister_socket(request.sid)


@socketio.on('join_session')
def handle_join_session(data):
    """客户端加入会话"""
    session_id = data.get('session_id')
    if session_id:
        ai_service.message_queue.register_socket(request.sid, session_id)
        join_room(session_id)  # 加入房间
        emit('session_joined', {
            'session_id': session_id,
            'message': f'已加入会话 {session_id[:8]}'
        })
        logger.info(f"🏠 Socket {request.sid[:8]} 加入会话 {session_id[:8]}")


@socketio.on('leave_session')
def handle_leave_session(data):
    """客户端离开会话"""
    session_id = data.get('session_id')
    if session_id:
        leave_room(session_id)
        emit('session_left', {
            'session_id': session_id,
            'message': f'已离开会话 {session_id[:8]}'
        })
        logger.info(f"🚪 Socket {request.sid[:8]} 离开会话 {session_id[:8]}")


@socketio.on('edw_task')
def handle_edw_task(data):
    """处理EDW任务请求（通过SocketIO）"""
    session_id = data.get('session_id')
    message = data.get('message')

    if not session_id or not message:
        emit('error', {'message': '缺少必要参数'})
        return

    logger.info(f"📋 收到EDW任务请求: {message[:50]}... (会话: {session_id[:8]})")

    # 发送任务开始确认
    emit('task_started', {
        'session_id': session_id,
        'message': 'EDW任务已开始处理',
        'timestamp': datetime.now().isoformat()
    })

    # 注意：实际的EDW任务处理通过HTTP流式接口进行
    # 这里只是提供了SocketIO的备用接口


@socketio.on('interrupt_response')
def handle_interrupt_response(data):
    """处理中断响应（微调输入）"""
    session_id = data.get('session_id')
    user_input = data.get('input')

    if not session_id or not user_input:
        emit('error', {'message': '缺少必要参数'})
        return

    logger.info(f"✏️ 收到中断响应: {user_input[:50]}... (会话: {session_id[:8]})")

    # 标记中断已处理
    emit('interrupt_handled', {
        'session_id': session_id,
        'message': '已收到您的反馈，正在处理...',
        'timestamp': datetime.now().isoformat()
    })

    # 注意：实际的中断处理通过继续发送消息到流式接口完成

# HTTP路由保持不变


@app.route('/')
def index():
    """主页"""
    return app.send_static_file('index.html')


@app.route('/api/chat/stream', methods=['POST'])
def chat_stream():
    """流式聊天接口 - 简化版（只处理AI文本）"""
    logger.info(f"📡 收到chat/stream请求: {request.method} {request.url}")
    logger.info(f"📋 请求头: {dict(request.headers)}")
    logger.info(f"🌐 客户端地址: {request.environ.get('REMOTE_ADDR')}")
    
    try:
        data = request.get_json()
        logger.info(f"📦 请求数据: {data}")
        
        if not data:
            logger.error("❌ 缺少请求数据")
            return jsonify({'success': False, 'error': '缺少请求数据'}), 400

        message = data.get('message')
        session_id = data.get('session_id')

        logger.info(f"📝 解析消息参数: message='{message[:50] if message else None}...', session_id='{session_id}'")

        if not message or not message.strip():
            logger.error("❌ 消息内容为空")
            return jsonify({'success': False, 'error': '消息内容不能为空'}), 400

        if not session_id:
            session_id = f"session-{int(time.time())}-{uuid.uuid4().hex[:8]}"
            logger.info(f"🆔 生成新的session_id: {session_id}")

        logger.info(f"🎯 开始处理流式聊天: {message[:50]}... (会话: {session_id[:8]})")

        def generate():
            """生成器函数，只处理AI文本响应"""
            loop = None
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                async def stream_chat():
                    async for chunk in ai_service.general_chat_stream(message.strip(), session_id):
                        if isinstance(chunk, dict):
                            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                async_gen = stream_chat()
                while True:
                    try:
                        chunk = loop.run_until_complete(async_gen.__anext__())
                        yield chunk
                    except StopAsyncIteration:
                        break

            except Exception as e:
                logger.error(f"❌ 流式聊天生成器错误: {e}", exc_info=True)
                error_chunk = {
                    'type': 'error',
                    'error': str(e),
                    'session_id': session_id
                }
                yield f"data: {json.dumps(error_chunk, ensure_ascii=False)}\n\n"
            finally:
                if loop and not loop.is_closed():
                    loop.close()

        return Response(
            generate(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'X-Session-ID': session_id,
            }
        )

    except Exception as e:
        logger.error(f"❌ 流式聊天API错误: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': f'服务器内部错误: {str(e)}'
        }), 500

# 其他API路由保持不变...


@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    """获取所有会话信息"""
    try:
        sessions = session_manager.get_all_sessions()
        return jsonify({
            'success': True,
            'data': {
                'sessions': sessions,
                'total_count': len(sessions)
            }
        })
    except Exception as e:
        logger.error(f"获取会话列表失败: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    total_sessions = len(session_manager.sessions)
    total_messages = sum(len(messages) for messages in session_manager.sessions.values())
    socket_connections = len(ai_service.message_queue.socket_sessions)

    return jsonify({
        'success': True,
        'message': '服务运行正常',
        'timestamp': datetime.now().isoformat(),
        'service_type': 'AI Chat Service with SocketIO',
        'session_stats': {
            'total_sessions': total_sessions,
            'total_messages': total_messages,
            'socket_connections': socket_connections,
            'max_history_per_session': session_manager.max_history_per_session,
            'session_timeout_hours': session_manager.session_timeout_hours,
            'active_agents': len(session_manager.session_agents)
        }
    })


# 应用关闭清理逻辑
def cleanup_on_exit():
    """应用关闭时的清理函数"""
    try:
        logger.info("🧹 开始应用关闭清理...")

        # 清理所有会话和Agent实例
        if 'session_manager' in globals():
            session_manager.cleanup_all_sessions()

        # 清理SocketIO连接
        if 'ai_service' in globals():
            ai_service.message_queue.session_sockets.clear()
            ai_service.message_queue.socket_sessions.clear()

        # 清理EDW服务实例
        try:
            if 'ai_service' in globals() and hasattr(ai_service, 'edw_stream_services'):
                for session_id, service in ai_service.edw_stream_services.items():
                    service.cleanup()
                    logger.info(f"✅ 清理EDW服务: {session_id[:8]}")
                ai_service.edw_stream_services.clear()
        except Exception as e:
            logger.error(f"⚠️ 清理EDW服务时出错: {e}")

        logger.info("✅ 应用关闭清理完成")
    except Exception as e:
        logger.error(f"❌ 应用关闭清理失败: {e}")


def signal_handler(signum, frame):
    """处理系统信号"""
    logger.info(f"🛑 收到系统信号 {signum}，开始清理...")
    cleanup_on_exit()
    exit(0)


# 注册清理函数和信号处理器
atexit.register(cleanup_on_exit)
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == '__main__':
    print("EDW智能聊天助手后端服务启动中...")
    print("实时通信: SocketIO")
    print("AI模型类型: deepseek")
    print(f"会话管理: 最大历史记录 {session_manager.max_history_per_session} 条")
    print("\n通信架构:")
    print("   HTTP Stream: AI文本响应")
    print("   SocketIO: 实时Agent消息（页面切换、工具状态等）")
    print("\nSocketIO事件:")
    print("   connect/disconnect - 连接管理")
    print("   join_session/leave_session - 会话管理")
    print("   agent_message - Agent实时消息")
    print("\n资源管理:")
    print("   已注册应用关闭清理函数")
    print("   已注册信号处理器 (SIGINT/SIGTERM)")
    print("   MCP服务器会在应用关闭时正确清理")
    print("   Agent实例会在会话结束时自动清理")

    # 初始化功能Agent
    print("\n功能Agent初始化:")
    try:
        import asyncio
        from src.agent.edw_agents import get_agent_manager
        
        def initialize_function_agent():
            async def _init():
                agent_manager = get_agent_manager()
                await agent_manager.async_initialize()
                print("   ✅ 功能Agent初始化完成")
                
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_init())
            finally:
                loop.close()
        
        initialize_function_agent()
    except Exception as e:
        print(f"   ❌ 功能Agent初始化失败: {e}")
        logger.error(f"功能Agent初始化失败: {e}")

    try:
        socketio.run(app, debug=True, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        logger.info("🛑 收到键盘中断信号，应用即将关闭...")
    except Exception as e:
        logger.error(f"❌ 应用运行时出错: {e}")
    finally:
        logger.info("🏁 应用已关闭")
