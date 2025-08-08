"""
测试上下文感知提示生成功能
"""

import asyncio
from langchain.schema.messages import HumanMessage, AIMessage
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt


def test_validation_error_scenario():
    """测试验证错误场景的提示生成"""
    print("\n=== 测试验证错误场景 ===")
    
    # 模拟第一次失败的状态
    state_first_attempt = {
        "messages": [
            HumanMessage(content="我想给表增加字段"),
            AIMessage(content="好的，我需要了解您要增强哪个表以及要添加的字段信息。")
        ],
        "error_message": "缺少表名和字段信息",
        "missing_info": ["表名", "字段定义"],
        "validation_status": "incomplete_info",
        "retry_count": 0,
        "user_id": "test_user"
    }
    
    prompt1 = generate_contextual_prompt(state_first_attempt, "validation_error")
    print("第一次失败的提示：")
    print(prompt1)
    print("-" * 80)
    
    # 模拟第二次失败的状态（用户提供了部分信息）
    state_second_attempt = {
        "messages": [
            HumanMessage(content="我想给表增加字段"),
            AIMessage(content="好的，我需要了解您要增强哪个表以及要添加的字段信息。"),
            HumanMessage(content="dwd_fi.fi_invoice_item表"),
            AIMessage(content="很好，我知道了是dwd_fi.fi_invoice_item表，但还需要知道具体要添加的字段信息。")
        ],
        "table_name": "dwd_fi.fi_invoice_item",
        "error_message": "还需要字段的详细信息",
        "missing_info": ["字段定义"],
        "validation_status": "incomplete_info",
        "retry_count": 1,
        "user_id": "test_user"
    }
    
    prompt2 = generate_contextual_prompt(state_second_attempt, "validation_error")
    print("\n第二次失败的提示（已有部分信息）：")
    print(prompt2)


def test_code_refinement_scenario():
    """测试代码微调场景的提示生成"""
    print("\n\n=== 测试代码微调场景 ===")
    
    # 模拟代码生成完成后的状态
    state_refinement = {
        "messages": [
            HumanMessage(content="给dwd_fi.fi_invoice_item表增加invoice_doc_no（Invoice Document Number）字段"),
            AIMessage(content="好的，我来为您增强这个表..."),
            AIMessage(content="代码增强完成，已经生成了包含新字段的SQL和Python代码。")
        ],
        "table_name": "dwd_fi.fi_invoice_item",
        "fields": [{"physical_name": "invoice_doc_no", "attribute_name": "Invoice Document Number"}],
        "enhancement_type": "add_field",
        "enhance_code": "-- 这里是生成的增强代码...",
        "current_refinement_round": 1,
        "user_id": "test_user"
    }
    
    prompt = generate_contextual_prompt(state_refinement, "code_refinement")
    print("代码微调询问提示：")
    print(prompt)


def test_general_scenario():
    """测试通用场景（没有特定场景提示）"""
    print("\n\n=== 测试通用场景 ===")
    
    state_general = {
        "messages": [
            HumanMessage(content="我有一些数据模型的问题"),
            AIMessage(content="很高兴帮助您解决数据模型相关的问题。")
        ],
        "user_id": "test_user"
    }
    
    prompt = generate_contextual_prompt(state_general)
    print("通用场景提示：")
    print(prompt)


def test_error_handling():
    """测试错误处理和降级"""
    print("\n\n=== 测试错误处理 ===")
    
    # 空状态
    empty_state = {}
    prompt = generate_contextual_prompt(empty_state, "validation_error")
    print("空状态降级提示：")
    print(prompt)


async def main():
    """运行所有测试"""
    print("开始测试上下文感知提示生成功能...")
    
    # 初始化必要的组件
    from src.agent.edw_agents import get_agent_manager
    agent_manager = get_agent_manager()
    agent_manager.initialize()
    
    # 运行测试
    test_validation_error_scenario()
    test_code_refinement_scenario()
    test_general_scenario()
    test_error_handling()
    
    print("\n测试完成！")


if __name__ == "__main__":
    asyncio.run(main())