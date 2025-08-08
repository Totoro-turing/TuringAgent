"""
测试代码Review功能
验证代码评估和自动改进机制是否正常工作
"""

import asyncio
import logging
from src.graph.edw_graph import guid, EDWState, SessionManager
from langchain.schema.messages import HumanMessage
import uuid

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_code_review():
    """测试代码review流程"""
    
    # 生成测试用户ID
    user_id = str(uuid.uuid4())[:8]
    logger.info(f"测试用户ID: {user_id}")
    
    # 获取配置
    config = SessionManager.get_config(user_id, "test")
    
    # 构建测试消息 - 一个会触发EDW模型增强的请求，包含所有必要信息
    test_message = """我要给dwd_fi.fi_invoice_item表增加字段invoice_doc_no（Invoice Document Number），
    字段类型为STRING，模型名称为Finance Invoice Item，业务目的是支持发票单据编号的追踪"""
    
    # 创建初始状态
    initial_state = {
        "messages": [HumanMessage(content=test_message)],
        "user_id": user_id,
        "max_review_rounds": 2  # 设置最大review轮次为2，方便测试
    }
    
    logger.info("开始测试代码review流程...")
    logger.info(f"测试消息: {test_message}")
    
    # 收集输出
    outputs = []
    review_outputs = []
    
    try:
        # 执行图流
        async for chunk in guid.astream(initial_state, config, stream_mode="updates"):
            for node_name, node_output in chunk.items():
                logger.info(f"节点输出 - {node_name}")
                outputs.append({
                    "node": node_name,
                    "output": node_output
                })
                
                # 特别关注review相关节点（只对dict类型的输出进行处理）
                if isinstance(node_output, dict) and "review" in node_name.lower():
                    review_score = node_output.get("review_score", -1)
                    review_feedback = node_output.get("review_feedback", "")
                    review_round = node_output.get("review_round", 0)
                    
                    logger.info(f"  Review轮次: {review_round}")
                    logger.info(f"  Review评分: {review_score}")
                    logger.info(f"  Review反馈: {review_feedback[:100]}...")
                    
                    review_outputs.append({
                        "round": review_round,
                        "score": review_score,
                        "feedback": review_feedback
                    })
                
                # 检查是否生成了增强代码（只对dict类型的输出进行检查）
                if isinstance(node_output, dict):
                    if node_output.get("enhance_code"):
                        logger.info(f"  生成了增强代码（长度: {len(node_output['enhance_code'])}）")
                    
                    # 检查是否有错误
                    if node_output.get("error_message"):
                        logger.error(f"  错误: {node_output['error_message']}")
        
        # 分析测试结果
        logger.info("\n" + "="*50)
        logger.info("测试结果分析")
        logger.info("="*50)
        
        if review_outputs:
            logger.info(f"✅ 代码review功能正常工作")
            logger.info(f"   - 执行了 {len(review_outputs)} 轮review")
            for review in review_outputs:
                logger.info(f"   - 第{review['round']}轮评分: {review['score']}")
        else:
            logger.warning("⚠️ 未检测到代码review输出")
        
        # 检查是否有最终的增强代码
        final_code_generated = any(
            isinstance(output.get("output"), dict) and "enhance_code" in output.get("output", {}) 
            for output in outputs
        )
        
        if final_code_generated:
            logger.info(f"✅ 最终生成了增强代码")
        else:
            logger.warning("⚠️ 未生成最终的增强代码")
        
        return {
            "success": True,
            "review_count": len(review_outputs),
            "total_nodes": len(outputs),
            "review_outputs": review_outputs
        }
        
    except Exception as e:
        logger.error(f"测试失败: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def main():
    """主函数"""
    logger.info("开始代码Review功能测试")
    logger.info("="*50)
    
    # 运行异步测试
    result = asyncio.run(test_code_review())
    
    # 打印最终结果
    logger.info("\n" + "="*50)
    logger.info("测试完成")
    logger.info("="*50)
    
    if result["success"]:
        logger.info("✅ 代码Review机制测试通过")
        logger.info(f"   - Review轮次: {result['review_count']}")
        logger.info(f"   - 处理节点数: {result['total_nodes']}")
    else:
        logger.error(f"❌ 测试失败: {result['error']}")
    
    return result


if __name__ == "__main__":
    # 设置环境变量（如果需要）
    import os
    from dotenv import load_dotenv
    
    # 加载.env文件
    load_dotenv()
    
    # 确保有必要的环境变量
    if not os.getenv("OPENAI_API_KEY") and not os.getenv("DEEPSEEK_API_KEY"):
        logger.error("请设置OPENAI_API_KEY或DEEPSEEK_API_KEY环境变量")
        exit(1)
    
    # 运行测试
    main()