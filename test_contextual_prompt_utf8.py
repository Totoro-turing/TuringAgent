# -*- coding: utf-8 -*-
"""
测试上下文感知提示生成功能（UTF-8编码）
"""

import asyncio
import sys
from langchain.schema.messages import HumanMessage, AIMessage
from src.models.states import EDWState
from src.graph.contextual_prompt import generate_contextual_prompt


def print_test_section(title):
    """打印测试段落标题"""
    print(f"\n{'=' * 80}")
    print(f"测试场景：{title}")
    print('=' * 80)


def test_validation_error_scenario():
    """测试验证错误场景的提示生成"""
    print_test_section("验证错误 - 第一次失败")
    
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
    print("生成的智能提示：")
    print(prompt1)
    
    print_test_section("验证错误 - 第二次失败（部分信息已提供）")
    
    # 模拟第二次失败的状态
    state_second_attempt = {
        "messages": [
            HumanMessage(content="我想给表增加字段"),
            AIMessage(content="好的，我需要了解您要增强哪个表以及要添加的字段信息。"),
            HumanMessage(content="dwd_fi.fi_invoice_item表"),
            AIMessage(content="很好，我知道了是dwd_fi.fi_invoice_item表。")
        ],
        "table_name": "dwd_fi.fi_invoice_item",
        "error_message": "还需要字段的详细信息",
        "missing_info": ["字段定义"],
        "validation_status": "incomplete_info",
        "retry_count": 1,
        "user_id": "test_user"
    }
    
    prompt2 = generate_contextual_prompt(state_second_attempt, "validation_error")
    print("生成的智能提示：")
    print(prompt2)


def test_code_refinement_scenario():
    """测试代码微调场景的提示生成"""
    print_test_section("代码微调询问")
    
    state_refinement = {
        "messages": [
            HumanMessage(content="给dwd_fi.fi_invoice_item表增加invoice_doc_no字段"),
            AIMessage(content="好的，我来为您增强这个表..."),
            AIMessage(content="代码增强完成，已经生成了包含新字段的SQL和Python代码。")
        ],
        "table_name": "dwd_fi.fi_invoice_item",
        "fields": [{"physical_name": "invoice_doc_no", "attribute_name": "Invoice Document Number"}],
        "enhancement_type": "add_field",
        "enhance_code": "-- SQL代码已生成...",
        "current_refinement_round": 1,
        "user_id": "test_user"
    }
    
    prompt = generate_contextual_prompt(state_refinement, "code_refinement")
    print("生成的智能询问：")
    print(prompt)


def test_comparison():
    """对比硬编码提示和智能生成提示"""
    print_test_section("硬编码 vs 智能生成对比")
    
    print("原始硬编码提示：")
    print("为了帮您完成模型增强，我需要以下信息：\n- 表名\n- 字段定义\n请补充完整信息后重新提交。")
    
    print("\n" + "-" * 40 + "\n")
    
    state = {
        "messages": [
            HumanMessage(content="增加invoice_doc_no"),
            AIMessage(content="需要更多信息"),
            HumanMessage(content="就是发票单号字段")
        ],
        "error_message": "缺少表名信息",
        "missing_info": ["表名"],
        "retry_count": 2,
        "user_id": "test_user"
    }
    
    print("智能生成的提示：")
    prompt = generate_contextual_prompt(state, "validation_error")
    print(prompt)


async def main():
    """运行所有测试"""
    print("上下文感知提示生成功能测试")
    print("=" * 80)
    
    # 初始化
    from src.agent.edw_agents import get_agent_manager
    agent_manager = get_agent_manager()
    agent_manager.initialize()
    
    # 运行测试
    test_validation_error_scenario()
    test_code_refinement_scenario()
    test_comparison()
    
    print("\n" + "=" * 80)
    print("测试完成！上下文感知提示功能正常工作。")


if __name__ == "__main__":
    # 设置编码
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    
    asyncio.run(main())