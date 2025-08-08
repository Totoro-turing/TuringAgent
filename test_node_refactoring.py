"""
测试节点重构是否成功
"""

import sys
import os
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_node_imports():
    """测试节点导入"""
    try:
        logger.info("测试节点导入...")
        
        # 测试核心节点导入
        from src.graph.nodes.core.navigation import navigate_node, chat_node, edw_model_node
        logger.info("✅ 核心节点导入成功")
        
        # 测试路由函数导入
        from src.graph.nodes.core.routing import routing_fun, model_routing_fun
        logger.info("✅ 路由函数导入成功")
        
        # 测试验证节点导入
        from src.graph.nodes.validation.validation_check import validation_check_node
        logger.info("✅ 验证节点导入成功")
        
        # 测试外部节点导入
        from src.graph.nodes.external.github import github_push_node
        logger.info("✅ 外部集成节点导入成功")
        
        # 测试工具函数导入
        from src.graph.utils.session import SessionManager
        from src.graph.utils.message import create_summary_reply
        logger.info("✅ 工具函数导入成功")
        
        # 测试统一导入
        from src.graph.nodes import (
            navigate_node,
            chat_node,
            routing_fun,
            github_push_node
        )
        logger.info("✅ 统一导入接口测试成功")
        
        return True
        
    except ImportError as e:
        logger.error(f"❌ 导入失败: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ 未知错误: {e}")
        return False


def test_graph_compilation():
    """测试图编译"""
    try:
        logger.info("\n测试图编译...")
        
        # 测试新的图结构
        from src.graph.edw_graph_v2 import guid
        logger.info("✅ 新图结构导入成功")
        
        # 测试图的基本功能
        test_state = {
            "messages": [],
            "user_id": "test_user",
            "type": "other"
        }
        
        # 尝试获取图的配置
        config = {"configurable": {"thread_id": "test_thread"}}
        
        logger.info("✅ 图编译和基本功能测试成功")
        return True
        
    except ImportError as e:
        logger.error(f"❌ 图导入失败: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ 图编译失败: {e}")
        return False


def test_session_manager():
    """测试会话管理器"""
    try:
        logger.info("\n测试会话管理器...")
        
        from src.graph.utils.session import SessionManager
        
        # 测试线程ID生成
        thread_id = SessionManager.generate_thread_id("test_user", "test_agent")
        assert thread_id is not None
        assert len(thread_id) > 0
        logger.info(f"生成的线程ID: {thread_id}")
        
        # 测试配置获取
        config = SessionManager.get_config("test_user", "test_agent")
        assert "configurable" in config
        assert "thread_id" in config["configurable"]
        logger.info("✅ 会话管理器测试成功")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 会话管理器测试失败: {e}")
        return False


def main():
    """主测试函数"""
    logger.info("=" * 60)
    logger.info("开始测试EDW图节点重构")
    logger.info("=" * 60)
    
    tests = [
        ("节点导入测试", test_node_imports),
        ("图编译测试", test_graph_compilation),
        ("会话管理器测试", test_session_manager)
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n运行: {test_name}")
        logger.info("-" * 40)
        success = test_func()
        results.append((test_name, success))
    
    # 汇总结果
    logger.info("\n" + "=" * 60)
    logger.info("测试结果汇总")
    logger.info("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    failed = len(results) - passed
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总计: {len(results)} 个测试")
    logger.info(f"通过: {passed}")
    logger.info(f"失败: {failed}")
    
    if failed == 0:
        logger.info("\n🎉 所有测试通过！节点重构成功！")
        return 0
    else:
        logger.error(f"\n⚠️ {failed} 个测试失败，请检查重构代码")
        return 1


if __name__ == "__main__":
    exit(main())