"""
测试属性名称Review功能
验证属性命名规范检查和优化建议机制
"""

import logging
from src.graph.attribute_review_nodes import AttributeNameReviewer

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_attribute_reviewer():
    """测试属性名称评审器"""
    
    logger.info("开始测试属性名称Review功能")
    logger.info("="*50)
    
    # 创建评审器
    reviewer = AttributeNameReviewer()
    
    # 测试用例
    test_cases = [
        # (物理名称, 当前属性名称, 预期评分范围)
        ("invoice_doc_no", "Invoice Document Number", (70, 100)),  # 知识库中存在，但名称过长
        ("invoice_doc_no", "InvoiceDocumentNumber", (85, 100)),    # 知识库标准名称
        ("invoice_doc_no", "invoice_doc_no", (50, 70)),            # 下划线命名
        ("customer_name", "CustomerName", (90, 100)),              # 标准命名
        ("customer_name", "cust_nm", (40, 60)),                    # 缩写+下划线
        ("order_amount", "OrderAmount", (90, 100)),                # 标准命名
        ("order_amount", "orderAmount", (70, 85)),                 # 驼峰命名
        ("create_date", "CreateDate", (90, 100)),                  # 标准命名
        ("custom_field", "CustomField", (70, 90)),                 # 非知识库字段，但符合规范
        ("custom_field", "custom_field", (50, 70)),                # 非知识库字段，下划线命名
    ]
    
    results = []
    
    for physical_name, attribute_name, expected_range in test_cases:
        logger.info(f"\n测试: {physical_name} -> {attribute_name}")
        logger.info("-"*30)
        
        # 执行review
        result = reviewer.review_attribute_name(
            physical_name=physical_name,
            attribute_name=attribute_name,
            context="测试环境"
        )
        
        # 记录结果
        score = result["score"]
        kb_match = result.get("kb_match")
        suggestions = result.get("suggestions", [])
        
        logger.info(f"  评分: {score}")
        logger.info(f"  反馈: {result['feedback']}")
        
        if kb_match:
            logger.info(f"  知识库匹配: {kb_match.get('standard_name')} ({kb_match.get('category')})")
        
        if suggestions:
            logger.info(f"  改进建议:")
            for i, sugg in enumerate(suggestions, 1):
                logger.info(f"    {i}. {sugg['suggested_name']} - {sugg['reason']}")
        
        # 验证评分是否在预期范围内
        min_score, max_score = expected_range
        if min_score <= score <= max_score:
            logger.info(f"  ✅ 评分在预期范围内 ({min_score}-{max_score})")
            status = "PASS"
        else:
            logger.error(f"  ❌ 评分不在预期范围内 ({min_score}-{max_score})")
            status = "FAIL"
        
        results.append({
            "physical": physical_name,
            "attribute": attribute_name,
            "score": score,
            "expected": expected_range,
            "status": status,
            "suggestions": [s["suggested_name"] for s in suggestions[:1]] if suggestions else []
        })
    
    # 汇总结果
    logger.info("\n" + "="*50)
    logger.info("测试结果汇总")
    logger.info("="*50)
    
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    
    logger.info(f"总测试数: {len(results)}")
    logger.info(f"通过: {passed}")
    logger.info(f"失败: {failed}")
    
    if failed > 0:
        logger.error("\n失败的测试用例:")
        for r in results:
            if r["status"] == "FAIL":
                logger.error(f"  {r['physical']} -> {r['attribute']}: 评分 {r['score']} (预期 {r['expected']})")
    
    # 展示改进建议的效果
    logger.info("\n" + "="*50)
    logger.info("改进建议展示")
    logger.info("="*50)
    
    for r in results:
        if r["suggestions"]:
            logger.info(f"{r['attribute']:30} -> {r['suggestions'][0]:30} (评分: {r['score']:.1f})")
    
    return {
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "success": failed == 0
    }


def test_knowledge_base_loading():
    """测试知识库加载"""
    logger.info("\n测试知识库加载")
    logger.info("="*50)
    
    try:
        reviewer = AttributeNameReviewer()
        kb = reviewer.knowledge_base
        
        # 统计知识库内容
        categories = kb.get("common_attributes", {})
        total_attributes = 0
        
        for category_name, category_data in categories.items():
            if isinstance(category_data, dict):
                for subcategory, items in category_data.items():
                    if isinstance(items, list):
                        total_attributes += len(items)
                        logger.info(f"  {category_name}/{subcategory}: {len(items)} 个属性")
            elif isinstance(category_data, list):
                total_attributes += len(category_data)
                logger.info(f"  {category_name}: {len(category_data)} 个属性")
        
        logger.info(f"\n知识库总计: {total_attributes} 个标准属性")
        
        return True
        
    except Exception as e:
        logger.error(f"知识库加载失败: {e}")
        return False


def test_naming_convention_check():
    """测试命名规范检查"""
    logger.info("\n测试命名规范检查")
    logger.info("="*50)
    
    reviewer = AttributeNameReviewer()
    
    test_names = [
        ("InvoiceNumber", "帕斯卡命名法", 100),
        ("invoiceNumber", "驼峰命名法", 80),
        ("invoice_number", "下划线命名", 60),
        ("invoice-number", "连字符命名", 50),
        ("inv_no", "缩写+下划线", 50),
        ("I", "过短", 90),
        ("ThisIsAVeryLongAttributeNameThatExceedsThirtyCharacters", "过长", 85),
    ]
    
    for name, description, expected_min in test_names:
        result = reviewer._check_naming_convention(name)
        score = result["score"]
        issues = result["issues"]
        
        logger.info(f"{name:40} - {description:15} - 评分: {score:3}")
        if issues:
            logger.info(f"  问题: {', '.join(issues)}")
        
        if score >= expected_min:
            logger.info(f"  ✅ 通过 (预期最低 {expected_min})")
        else:
            logger.error(f"  ❌ 失败 (预期最低 {expected_min})")
    
    return True


def main():
    """主函数"""
    logger.info("开始属性名称Review功能测试")
    logger.info("="*60)
    
    # 1. 测试知识库加载
    kb_success = test_knowledge_base_loading()
    
    # 2. 测试命名规范检查
    convention_success = test_naming_convention_check()
    
    # 3. 测试完整的属性review功能
    review_result = test_attribute_reviewer()
    
    # 总结
    logger.info("\n" + "="*60)
    logger.info("测试完成")
    logger.info("="*60)
    
    if kb_success and convention_success and review_result["success"]:
        logger.info("✅ 所有测试通过")
        return 0
    else:
        logger.error("❌ 部分测试失败")
        return 1


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # 加载.env文件
    load_dotenv()
    
    # 运行测试
    exit_code = main()
    exit(exit_code)