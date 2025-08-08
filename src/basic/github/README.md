# GitHub工具使用说明

## 功能介绍

GitHubTool是一个用于在企业GitHub仓库中搜索和读取代码的工具，设计用于替代本地文件系统搜索。主要功能包括：

1. **文件搜索**：在GitHub仓库中搜索特定模式的文件
2. **代码读取**：读取GitHub上的文件内容
3. **表代码查询**：根据表名查找对应的数据处理代码（兼容原有接口）
4. **文件提交**：提交或更新文件到GitHub仓库（预留功能）

## 安装依赖

```bash
pip install PyGithub
```

## 配置

在项目根目录的`.env`文件中添加以下配置：

### 公共GitHub配置

```env
# GitHub访问令牌（必需）
GITHUB_TOKEN=your_github_personal_access_token

# GitHub仓库名称（必需）
GITHUB_REPO=owner/repository_name

# 默认分支（可选，默认为main）
GITHUB_BRANCH=main
```

### 企业GitHub配置

对于企业内部的GitHub实例，需要额外配置API地址：

```env
# GitHub访问令牌（必需）
GITHUB_TOKEN=your_github_personal_access_token

# GitHub仓库名称（支持两种格式）
# 格式1：owner/repository
GITHUB_REPO=Magellan-Finance/Magellan-WW-Databricks
# 格式2：完整URL（会自动提取owner/repo部分）
# GITHUB_REPO=https://magellancicd.ludp.lenovo.com/Magellan-Finance/Magellan-WW-Databricks

# 默认分支（可选，默认为main）
GITHUB_BRANCH=main

# 企业GitHub API地址（必需）
GITHUB_BASE_URL=https://magellancicd.ludp.lenovo.com/api/v3
```

### 获取GitHub Token

1. 登录GitHub（或企业GitHub）
2. 进入 Settings -> Developer settings -> Personal access tokens -> Tokens (classic)
3. 点击 "Generate new token"
4. 选择权限：至少需要 `repo` 权限
5. 生成并复制token

**注意**：如果遇到 "Bad credentials" 错误，请检查：
- Token是否正确
- Token是否有足够的权限
- 对于企业GitHub，GITHUB_BASE_URL是否正确配置

## 使用示例

### 基本使用

```python
from src.basic.github import GitHubTool

# 初始化工具
github_tool = GitHubTool()

# 搜索表代码
result = github_tool.search_table_code("dwd_fi.fi_invoice_item")
if result['status'] == 'success':
    print(f"找到文件: {result['file_name']}")
    print(f"代码内容: {result['code'][:100]}...")
```

### 高级功能

```python
# 搜索文件
files = github_tool.search_files_by_name("nb_*")

# 读取文件
content = github_tool.read_file("path/to/file.py")

# 列出文件
files = github_tool.list_files("", recursive=True)

# 提交文件（需要写权限）
result = github_tool.commit_file(
    file_path="path/to/new_file.py",
    content="# New file content",
    message="Add new file"
)
```

## 测试

运行测试脚本验证功能：

```bash
# 基础功能测试
python test_github_tool.py

# 对比本地和GitHub搜索结果
python test_github_tool.py --compare
```

## 与原有代码集成

GitHubTool的`search_table_code`方法返回格式与原有`search_table_cd`函数完全一致，可以直接替换：

```python
# 原有代码
from src.graph.edw_graph import search_table_cd
result = search_table_cd("dwd_fi.fi_invoice_item")

# 使用GitHub工具
from src.basic.github import GitHubTool
github_tool = GitHubTool()
result = github_tool.search_table_code("dwd_fi.fi_invoice_item")
```

## 优势

1. **无需本地克隆**：直接访问GitHub上的代码，节省磁盘空间
2. **实时更新**：始终获取最新的代码版本
3. **多分支支持**：可以访问不同分支的代码
4. **版本追踪**：可以获取文件的提交历史和修改信息
5. **权限控制**：通过GitHub的权限系统管理访问权限

## 注意事项

1. **API限制**：GitHub API有请求频率限制，大量请求时需要注意
2. **网络依赖**：需要稳定的网络连接
3. **认证要求**：必须配置有效的GitHub Token
4. **性能考虑**：相比本地文件系统，网络请求会有一定延迟

## 后续扩展

1. 实现代码提交和PR创建功能
2. 添加缓存机制减少API调用
3. 支持批量操作
4. 集成GitHub Actions工作流