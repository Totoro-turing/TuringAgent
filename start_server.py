"""
启动EDW集成服务器

这个脚本用于启动Flask服务器，集成了EDW工作流。
"""

import os
import sys
import logging

# 设置Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def check_environment():
    """检查环境配置"""
    print("🔍 检查环境配置...")
    
    # 检查必要的模块
    required_modules = [
        'flask',
        'flask_cors',
        'flask_socketio',
        'langchain',
        'langgraph'
    ]
    
    missing_modules = []
    for module in required_modules:
        try:
            __import__(module)
            print(f"  ✅ {module} 已安装")
        except ImportError:
            print(f"  ❌ {module} 未安装")
            missing_modules.append(module)
    
    if missing_modules:
        print(f"\n⚠️ 缺少以下模块: {', '.join(missing_modules)}")
        print("请运行: pip install " + " ".join(missing_modules))
        return False
    
    # 检查环境变量
    if not os.getenv('OPENAI_API_KEY'):
        print("\n⚠️ 未设置OPENAI_API_KEY环境变量")
        print("请在.env文件中设置或导出环境变量")
        # 不强制要求，因为可能使用其他模型
    
    return True


def start_server():
    """启动Flask服务器"""
    try:
        # 导入app
        from src.server.app import app, socketio
        
        print("\n" + "=" * 60)
        print("🚀 启动EDW集成服务器")
        print("=" * 60)
        print("\n配置信息:")
        print("  - 地址: http://localhost:5000")
        print("  - 模式: 开发模式")
        print("  - WebSocket: 已启用")
        print("  - CORS: 已启用")
        print("\n功能支持:")
        print("  ✅ 普通聊天")
        print("  ✅ EDW任务处理")
        print("  ✅ 流式输出")
        print("  ✅ 中断机制")
        print("  ✅ 实时状态推送")
        print("\n访问地址:")
        print("  🌐 主页: http://localhost:5000")
        print("  📡 API: http://localhost:5000/api/chat/stream")
        print("  ❤️ 健康检查: http://localhost:5000/api/health")
        print("\n按 Ctrl+C 停止服务器")
        print("=" * 60 + "\n")
        
        # 启动服务器
        socketio.run(
            app,
            debug=True,
            host='0.0.0.0',
            port=5000,
            allow_unsafe_werkzeug=True
        )
        
    except ImportError as e:
        print(f"\n❌ 无法导入必要的模块: {e}")
        print("请确保所有依赖已正确安装")
        return False
    except KeyboardInterrupt:
        print("\n\n👋 服务器已停止")
        return True
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        return False


def main():
    """主函数"""
    print("\n🎯 EDW集成服务器启动器\n")
    
    # 检查环境
    if not check_environment():
        print("\n❌ 环境检查失败，请修复后重试")
        return 1
    
    print("\n✅ 环境检查通过\n")
    
    # 启动服务器
    if start_server():
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())