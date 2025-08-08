# EDW图节点重构指南

## 概述
本次重构将EDW图的节点从单一大文件（edw_graph.py，2796行）拆分为模块化的包结构，提高代码的可维护性和可扩展性。

## 新的目录结构

```
src/graph/
├── nodes/                       # 所有节点模块
│   ├── __init__.py             # 统一导出接口
│   ├── core/                   # 核心流程节点
│   │   ├── navigation.py       # 导航、聊天、模型分类节点
│   │   └── routing.py          # 所有路由函数
│   ├── validation/             # 验证相关节点
│   │   ├── validation_check.py # 验证检查节点
│   │   └── model_validation.py # 模型验证节点
│   ├── enhancement/            # 增强相关节点
│   │   ├── model_enhance.py   # 模型增强节点
│   │   └── model_addition.py  # 模型新增节点
│   ├── review/                 # Review相关节点
│   │   ├── code_review.py     # 代码review子图
│   │   └── attribute_review.py # 属性review子图
│   ├── refinement/             # 微调相关节点
│   │   ├── inquiry.py          # 微调询问节点
│   │   ├── intent.py           # 意图识别节点
│   │   └── execution.py       # 微调执行节点
│   └── external/               # 外部系统集成
│       ├── github.py           # GitHub推送节点
│       ├── email.py            # 邮件发送节点
│       ├── confluence.py       # Confluence文档节点
│       └── adb.py             # ADB更新节点
├── utils/                      # 工具函数
│   ├── session.py             # 会话管理
│   ├── message.py             # 消息处理
│   ├── code.py                # 代码处理
│   └── field.py               # 字段处理
├── edw_graph.py               # 原始文件（待废弃）
├── edw_graph_v2.py            # 重构后的主图文件
└── REFACTORING_GUIDE.md       # 本文档
```

## 节点分类

### 1. 核心节点（core）
- **navigate_node**: 任务分类导航
- **chat_node**: 普通聊天处理
- **edw_model_node**: 模型任务分类
- **路由函数**: 所有条件路由逻辑

### 2. 验证节点（validation）
- **validation_check_node**: 验证状态检查和中断处理
- **edw_model_add_data_validation_node**: 模型新增验证

### 3. 增强节点（enhancement）
- **edw_model_enhance_node**: 模型增强处理
- **edw_model_addition_node**: 模型新增处理

### 4. Review节点（review）
- **create_review_subgraph**: 代码质量review子图
- **create_attribute_review_subgraph**: 属性命名review子图

### 5. 微调节点（refinement）
- **refinement_inquiry_node**: 询问用户是否需要微调
- **refinement_intent_node**: 识别用户微调意图
- **code_refinement_node**: 执行代码微调

### 6. 外部集成节点（external）
- **github_push_node**: 推送代码到GitHub
- **edw_email_node**: 发送邮件通知
- **edw_confluence_node**: 创建Confluence文档
- **edw_adb_update_node**: 更新ADB数据库

## 迁移步骤

### 第一阶段：创建新结构（已完成）
1. ✅ 创建nodes包和子包结构
2. ✅ 创建各个节点模块文件
3. ✅ 创建utils工具函数模块
4. ✅ 创建edw_graph_v2.py演示新结构

### 第二阶段：逐步迁移（进行中）
1. 将edw_graph.py中的节点代码移到对应模块
2. 更新所有导入路径
3. 处理循环依赖问题
4. 测试每个节点的功能

### 第三阶段：完成重构
1. 更新所有引用edw_graph.py的文件
2. 运行完整的测试套件
3. 删除或归档原始的edw_graph.py
4. 更新文档

## 使用新结构

### 导入单个节点
```python
from src.graph.nodes.core.navigation import navigate_node
from src.graph.nodes.external.github import github_push_node
```

### 导入所有节点
```python
from src.graph.nodes import (
    navigate_node,
    chat_node,
    github_push_node,
    # ... 其他节点
)
```

### 使用工具函数
```python
from src.graph.utils.session import SessionManager
from src.graph.utils.message import create_summary_reply
from src.graph.utils.code import extract_tables_from_code
```

## 优势

1. **模块化**: 每个节点在独立文件中，易于维护
2. **清晰的组织**: 按功能分类，便于查找
3. **减少耦合**: 节点之间依赖关系更清晰
4. **更好的测试性**: 可以独立测试每个节点
5. **团队协作**: 避免多人编辑同一个大文件的冲突

## 注意事项

1. **循环依赖**: 某些节点可能相互引用，需要仔细处理
2. **向后兼容**: 保持原有接口不变，确保平滑迁移
3. **测试覆盖**: 重构后需要完整测试所有功能
4. **性能影响**: 多文件结构可能略微增加导入时间

## 下一步计划

1. 完成所有节点代码的实际迁移
2. 创建单元测试
3. 更新集成测试
4. 优化导入结构
5. 编写详细的API文档

## 贡献指南

如果要添加新节点：
1. 确定节点类别（core/validation/enhancement等）
2. 在相应目录创建新文件
3. 在__init__.py中导出
4. 更新本文档

---

*最后更新: 2025-08-08*