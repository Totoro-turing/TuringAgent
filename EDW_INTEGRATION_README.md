# EDW集成系统使用指南

## 概述

本系统实现了Flask-SocketIO与EDW（企业数据仓库）工作流图的完整集成，支持：
- 🚀 流式输出
- 💬 中断机制（微调询问）
- 📊 实时进度推送
- 🔄 双通道通信（SSE + WebSocket）

## 系统架构

```
前端（HTML/JS）
    ├── SSE通道 ──→ 流式文本输出
    └── SocketIO通道 ──→ 实时状态更新
              ↓
    Flask服务器（app.py）
              ↓
    EDW流式服务（edw_service.py）
              ↓
    LangGraph工作流（edw_graph.py）
        ├── navigate_node（自动识别任务类型）
        ├── chat_node（处理普通聊天）
        └── model_node → model_dev（处理EDW任务）
```

### 智能路由机制

系统不再使用硬编码关键词判断，而是通过EDW图的`navigate_node`智能识别：
- **所有消息**都通过统一的EDW图处理
- `navigate_node`自动判断任务类型
- 根据识别结果路由到`chat_node`（普通聊天）或`model_node`（EDW任务）
- 无需在代码中硬编码判断逻辑

## 快速开始

### 1. 环境准备

```bash
# 安装依赖
pip install flask flask-cors flask-socketio
pip install langchain langgraph
pip install openai

# 设置环境变量（在.env文件中）
OPENAI_API_KEY=your_api_key_here
```

### 2. 启动服务器

```bash
# 方式1：使用启动脚本
python start_server.py

# 方式2：直接运行app
python src/server/app.py
```

### 3. 访问界面

打开浏览器访问：http://localhost:5000

## 功能测试

### 测试普通聊天
```
输入：你好
预期：返回普通聊天响应
```

### 测试EDW任务
```
输入：我要给 user_table 表增强一个字段 email
预期：
1. 识别为EDW任务
2. 显示进度条
3. 返回增强后的代码
```

### 测试中断机制
```
在EDW任务执行中：
1. 系统会弹出微调询问
2. 输入您的反馈
3. 系统继续执行并返回微调后的结果
```

## API接口

### 1. 流式聊天接口
```
POST /api/chat/stream
Content-Type: application/json

{
    "message": "用户消息",
    "session_id": "会话ID（可选）"
}

响应：SSE流式数据
```

### 2. 健康检查
```
GET /api/health

响应：
{
    "success": true,
    "message": "服务运行正常",
    "session_stats": {...}
}
```

## WebSocket事件

### 客户端发送
- `join_session`: 加入会话
- `leave_session`: 离开会话
- `edw_task`: EDW任务请求
- `interrupt_response`: 中断响应

### 服务器推送
- `agent_message`: Agent消息
- `workflow_start`: 工作流开始
- `node_progress`: 节点进度
- `workflow_interrupted`: 工作流中断
- `workflow_complete`: 工作流完成

## 数据类型说明

### SSE数据类型
```javascript
// 文本内容
{ type: 'content', content: '...' }

// 进度更新
{ type: 'progress', step: '...', progress: 50 }

// 增强代码
{ type: 'enhanced_code', content: '...', table_name: '...' }

// 中断询问
{ type: 'interrupt', prompt: '...', node: '...' }

// 完成
{ type: 'done', session_id: '...' }

// 错误
{ type: 'error', error: '...' }
```

## 故障排除

### 1. 服务器无法启动
- 检查端口5000是否被占用
- 确认所有依赖已安装
- 查看错误日志

### 2. EDW任务不识别
- 确认消息包含关键词（增强、新增、模型等）
- 检查EDW图模块是否正确导入

### 3. 中断机制不工作
- 确认WebSocket连接正常
- 检查浏览器控制台错误

### 4. 流式输出中断
- 检查网络连接
- 确认SSE支持（现代浏览器都支持）

## 开发调试

### 启用详细日志
```python
# 在app.py中设置
logging.basicConfig(level=logging.DEBUG)
```

### 测试脚本
```bash
# 运行集成测试
python test_edw_integration.py
```

## 注意事项

1. **环境隔离**：EDW服务实例按会话隔离，支持多用户并发
2. **中断处理**：中断响应会继续在同一会话中执行
3. **状态管理**：工作流状态由LangGraph的checkpointer管理
4. **错误恢复**：系统会自动清理失败的任务

## 扩展开发

### 添加新的数据类型
1. 在`edw_service.py`的`_process_node_output`方法中添加处理逻辑
2. 在前端`index.html`的流式数据处理中添加对应类型
3. 添加相应的UI展示函数

### 自定义节点处理
1. 在EDW图中添加新节点
2. 在`edw_service.py`中添加节点输出处理
3. 通过SocketIO推送节点状态

## 版本信息

- 版本：1.0.0
- 更新日期：2024-08
- 作者：EDW团队

## 许可证

本项目采用MIT许可证