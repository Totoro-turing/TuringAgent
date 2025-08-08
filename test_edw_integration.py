"""
测试EDW集成功能

测试项目：
1. Flask服务启动
2. EDW任务识别
3. 流式输出
4. 中断机制
"""

import asyncio
import json
import requests
from typing import Generator


def test_server_health():
    """测试服务器健康状态"""
    try:
        response = requests.get("http://localhost:5000/api/health")
        if response.status_code == 200:
            data = response.json()
            print("✅ 服务器健康检查通过")
            print(f"   服务类型: {data.get('service_type')}")
            print(f"   会话统计: {data.get('session_stats')}")
            return True
        else:
            print(f"❌ 服务器健康检查失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 无法连接到服务器: {e}")
        return False


def test_normal_chat():
    """测试普通聊天功能"""
    print("\n📋 测试普通聊天...")
    
    url = "http://localhost:5000/api/chat/stream"
    payload = {
        "message": "你好，请介绍一下自己",
        "session_id": "test-session-normal"
    }
    
    try:
        response = requests.post(url, json=payload, stream=True)
        
        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code}")
            return False
            
        print("📨 收到流式响应:")
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    try:
                        data = json.loads(line_str[6:])
                        if data['type'] == 'content':
                            print(data['content'], end='', flush=True)
                        elif data['type'] == 'done':
                            print("\n✅ 普通聊天测试完成")
                            return True
                        elif data['type'] == 'error':
                            print(f"\n❌ 错误: {data['error']}")
                            return False
                    except json.JSONDecodeError:
                        continue
                        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
        
    return False


def test_edw_task():
    """测试EDW任务识别和执行"""
    print("\n📋 测试EDW任务...")
    
    url = "http://localhost:5000/api/chat/stream"
    payload = {
        "message": "我要给 model_1234 表增强一个字段 user_name",
        "session_id": "test-session-edw"
    }
    
    try:
        response = requests.post(url, json=payload, stream=True)
        
        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code}")
            return False
            
        print("📨 收到EDW流式响应:")
        received_types = set()
        
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    try:
                        data = json.loads(line_str[6:])
                        received_types.add(data['type'])
                        
                        if data['type'] == 'task_classified':
                            print(f"  ✓ 任务分类: {data.get('task_type')}")
                        elif data['type'] == 'progress':
                            print(f"  ✓ 进度: {data.get('step')} - {data.get('progress')}%")
                        elif data['type'] == 'validation_progress':
                            print(f"  ✓ 验证: {data.get('message')}")
                        elif data['type'] == 'enhanced_code':
                            print(f"  ✓ 收到增强代码 (表: {data.get('table_name')})")
                        elif data['type'] == 'interrupt':
                            print(f"  ⚠️ 中断: {data.get('prompt')[:50]}...")
                            # 这里应该发送中断响应
                            return test_interrupt_response(payload['session_id'])
                        elif data['type'] == 'done':
                            print("\n✅ EDW任务测试完成")
                            print(f"   收到的数据类型: {received_types}")
                            return True
                        elif data['type'] == 'error':
                            print(f"\n❌ 错误: {data['error']}")
                            return False
                            
                    except json.JSONDecodeError as e:
                        print(f"解析错误: {e}")
                        continue
                        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
        
    return False


def test_interrupt_response(session_id: str):
    """测试中断响应"""
    print("\n📋 测试中断响应...")
    
    url = "http://localhost:5000/api/chat/stream"
    payload = {
        "message": "很好，继续",  # 对中断的响应
        "session_id": session_id
    }
    
    try:
        response = requests.post(url, json=payload, stream=True)
        
        if response.status_code != 200:
            print(f"❌ 请求失败: {response.status_code}")
            return False
            
        print("📨 收到中断恢复响应:")
        
        for line in response.iter_lines():
            if line:
                line_str = line.decode('utf-8')
                if line_str.startswith('data: '):
                    try:
                        data = json.loads(line_str[6:])
                        
                        if data['type'] == 'refined_code':
                            print(f"  ✓ 收到微调代码 (轮次: {data.get('round')})")
                        elif data['type'] == 'done':
                            print("\n✅ 中断响应测试完成")
                            return True
                        elif data['type'] == 'error':
                            print(f"\n❌ 错误: {data['error']}")
                            return False
                            
                    except json.JSONDecodeError:
                        continue
                        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        return False
        
    return False


def main():
    """运行所有测试"""
    print("=" * 60)
    print("🧪 EDW集成测试套件")
    print("=" * 60)
    
    # 测试结果汇总
    results = {
        "服务器健康检查": test_server_health(),
        "普通聊天功能": False,  # 暂时跳过，因为依赖未实现的模块
        "EDW任务识别": False,  # 需要完整环境才能测试
    }
    
    # 输出测试汇总
    print("\n" + "=" * 60)
    print("📊 测试结果汇总:")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {test_name}: {status}")
        
    # 计算通过率
    total_tests = len(results)
    passed_tests = sum(1 for passed in results.values() if passed)
    pass_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\n通过率: {passed_tests}/{total_tests} ({pass_rate:.1f}%)")
    
    if pass_rate == 100:
        print("\n🎉 所有测试通过！")
    elif pass_rate >= 50:
        print("\n⚠️ 部分测试通过，请检查失败的测试")
    else:
        print("\n❌ 大部分测试失败，请检查系统配置")


if __name__ == "__main__":
    print("\n⚠️ 注意事项:")
    print("1. 确保Flask服务器正在运行: python src/server/app.py")
    print("2. 确保EDW图模块已正确配置")
    print("3. 某些测试可能需要完整的环境配置\n")
    
    input("按Enter键开始测试...")
    main()