"""
模型增强节点
实现模型增强处理的核心逻辑
"""

import json
import logging
from datetime import datetime
from langchain.schema.messages import AIMessage
from src.models.states import EDWState
from src.graph.utils.enhancement import execute_code_enhancement_task
from src.graph.utils.message_sender import (
    send_node_message,
    send_code_message
)

logger = logging.getLogger(__name__)


async def edw_model_enhance_node(state: EDWState):
    """模型增强处理节点"""
    

    try:
        # 提取状态中的信息
        table_name = state.get("table_name")
        source_code = state.get("source_code")
        adb_code_path = state.get("adb_code_path")
        code_path = state.get("code_path")
        fields = state.get("fields", [])
        logic_detail = state.get("logic_detail")
        user_id = state.get("user_id", "")
        enhancement_type = state.get("enhancement_type", "add_field")
        
        # 🎯 发送验证进度
        send_node_message(state, "AI", "processing", "我最后检查一下信息是否完整...", 0.1)
        
        # 验证必要信息
        if not table_name or not source_code:
            error_msg = "缺少必要信息：表名或源代码为空"
            send_node_message(state, "model_enhance", "failed", f"错误: {error_msg}", 0.0)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        if not fields:
            error_msg = "没有找到新增字段信息"
            send_node_message(state, "model_enhance", "failed", f"错误: {error_msg}", 0.0)
            return {
                "error_message": error_msg,
                "user_id": user_id
            }
        
        # 🎯 发送代码增强进度
        send_node_message(state, "AI", "processing", f"让我基于您的需求生成新的代码...", 0.3)
        
        # 异步执行代码增强 - 优化版本：只传递state，所有参数都从state获取
        enhancement_result = await execute_code_enhancement_task(
            state=state,
            enhancement_mode="initial_enhancement"
        )
        
        if enhancement_result.get("success"):
            # 直接使用从数据校验节点传递过来的模型名称
            model_name = state.get("model_attribute_name", "")
            logger.info(f"使用数据校验节点提取的模型名称: {model_name}")
            
            # 🎯 发送增强代码到前端显示 - 使用统一消息接口
            success = send_code_message(
                state=state,
                code_type="enhanced",
                content=enhancement_result.get("enhanced_code"),
                table_name=table_name,
                enhancement_mode="initial_enhancement",
                create_table_sql=enhancement_result.get("new_table_ddl"),
                alter_table_sql=enhancement_result.get("alter_statements"),
                fields_count=len(fields),
                enhancement_type=enhancement_type,
                model_name=model_name,
                file_path=code_path,
                adb_path=adb_code_path,
                optimization_summary=enhancement_result.get("optimization_summary", "")
            )
            
            if success:
                logger.info(f"✅ 统一接口发送增强代码成功: {table_name} (长度: {len(enhancement_result.get('enhanced_code', ''))} 字符)")
            else:
                logger.warning(f"❌ 统一接口发送增强代码失败: {table_name}")
            
            # 格式化增强结果为用户友好的消息
            formatted_message = f"""## 🎉 代码增强完成

**目标表**: {table_name}
**新增字段**: {len(fields)} 个
**增强类型**: {enhancement_type}
**模型名称**: {model_name or '未指定'}

### ✅ 生成的内容
- 增强代码已生成
- CREATE TABLE 语句已生成
- ALTER TABLE 语句已生成

### 📊 详细结果
```json
{json.dumps(enhancement_result, ensure_ascii=False, indent=2)}
```

### 📋 新增字段列表
"""
            # 添加字段详情
            for field in fields:
                if isinstance(field, dict):
                    source_name = field.get('source_name', '')
                    physical_name = field.get('physical_name', '')
                    attribute_name = field.get('attribute_name', '')
                else:
                    source_name = getattr(field, 'source_name', '')
                    physical_name = getattr(field, 'physical_name', '')
                    attribute_name = getattr(field, 'attribute_name', '')
                formatted_message += f"- {physical_name} ({attribute_name}) <- 源字段: {source_name}\n"
            
            # 🎯 发送完成进度
            send_node_message(
                state=state,
                node_name="model_enhance",
                status="completed",
                message=f"成功生成{len(fields)}个字段的增强代码",
                progress=1.0,
                extra_data={
                    "table_name": table_name,
                    "fields_count": len(fields),
                    "enhancement_type": enhancement_type
                }
            )
            
            return {
                "messages": [AIMessage(content=formatted_message)],  # 添加 AI 消息到状态
                "user_id": user_id,
                "enhance_code": enhancement_result.get("enhanced_code"),
                "create_table_sql": enhancement_result.get("new_table_ddl"),
                "alter_table_sql": enhancement_result.get("alter_statements"),
                "model_name": model_name,  # 使用数据校验节点提取的模型名称
                "field_mappings": enhancement_result.get("field_mappings"),
                "enhancement_type": enhancement_type,  # 保留增强类型供路由使用
                "enhancement_summary": {
                    "table_name": table_name,
                    "fields_added": len(fields),
                    "base_tables_analyzed": enhancement_result.get("base_tables_analyzed", 0),
                    "timestamp": datetime.now().isoformat()
                },
                "session_state": "enhancement_completed"
            }
        else:
            error_msg = enhancement_result.get("error", "未知错误")
            logger.error(f"代码增强失败: {error_msg}")
            # 🎯 发送失败进度
            send_node_message(state, "model_enhance", "failed", f"错误: {error_msg}", 0.0)
            return {
                "error_message": error_msg,
                "user_id": user_id,
                "enhancement_type": enhancement_type  # 保留增强类型
            }
    
    except Exception as e:
        error_msg = f"模型增强节点处理失败: {str(e)}"
        logger.error(error_msg)
        # 🎯 发送异常失败进度
        send_node_failed(state, "model_enhance", error_msg)
        return {
            "error_message": error_msg,
            "user_id": state.get("user_id", ""),
            "enhancement_type": state.get("enhancement_type", "")  # 保留增强类型
        }


__all__ = ['edw_model_enhance_node']