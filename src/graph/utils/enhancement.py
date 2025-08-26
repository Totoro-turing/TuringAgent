"""
代码增强工具函数
包含代码增强、微调和改进的核心逻辑
支持分阶段生成以处理大型任务，避免LLM输出截断
"""

import logging
import json
import re
from typing import Dict, Any, List, Optional, Tuple
from langchain.schema.messages import HumanMessage
from src.graph.utils.session import SessionManager
from src.graph.utils.code import parse_agent_response

# Git diff解析库
try:
    from unidiff import PatchSet
    UNIDIFF_AVAILABLE = True
except ImportError:
    UNIDIFF_AVAILABLE = False
    logger.warning("unidiff库未安装，将使用正则表达式降级方案")

logger = logging.getLogger(__name__)


def extract_json_from_response(content: str, fallback_data: dict = None) -> dict:
    """
    从LLM响应中智能提取JSON数据

    处理AI经常在JSON前面添加说明文字的情况，例如：
    "以下是增强结果：\n{\n  \"enhanced_code\": \"...\"\n}"

    Args:
        content: LLM响应内容
        fallback_data: 解析失败时的默认数据

    Returns:
        解析出的JSON字典，如果失败返回fallback_data或空字典
    """
    if not content:
        return fallback_data or {}

    try:
        # 尝试直接解析JSON
        return json.loads(content.strip())
    except json.JSONDecodeError:
        logger.debug("直接JSON解析失败，尝试智能提取")

    # 方法1: 从JSON代码块中提取
    json_block_patterns = [
        r'```json\n(.*?)\n```',
        r'```\n(\{.*?\})\n```',
        r'```(\{.*?\})```'
    ]

    for pattern in json_block_patterns:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            try:
                result = json.loads(match.group(1).strip())
                logger.debug("从JSON代码块中成功提取")
                return result
            except json.JSONDecodeError:
                continue

    # 方法2: 查找第一个完整的JSON对象
    # 匹配最外层的大括号内容
    brace_pattern = r'\{(?:[^{}]*(?:\{[^{}]*\}[^{}]*)*)*\}'
    matches = re.finditer(brace_pattern, content, re.DOTALL)

    for match in matches:
        json_candidate = match.group(0)
        try:
            result = json.loads(json_candidate)
            logger.debug("从花括号内容中成功提取")
            return result
        except json.JSONDecodeError:
            continue

    # 方法3: 尝试找到JSON的开始和结束位置
    start_markers = ['{', 'json', '"enhanced_code"', '"git_diffs"']
    for marker in start_markers:
        start_idx = content.find(marker)
        if start_idx != -1 and marker == '{':
            # 从第一个 { 开始尝试解析
            try:
                # 找到匹配的 }
                brace_count = 0
                end_idx = -1
                for i in range(start_idx, len(content)):
                    if content[i] == '{':
                        brace_count += 1
                    elif content[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_idx = i + 1
                            break

                if end_idx > 0:
                    json_candidate = content[start_idx:end_idx]
                    result = json.loads(json_candidate)
                    logger.debug("通过括号计数成功提取")
                    return result
            except (json.JSONDecodeError, IndexError):
                continue

    logger.warning(f"所有JSON提取方法都失败，响应内容前100字符: {content[:100]}...")
    return fallback_data or {}


def choose_enhancement_strategy(source_code: str, fields: List[Dict], enhancement_mode: str) -> str:
    """
    智能选择代码增强策略 - 简化版本（两种策略）

    Args:
        source_code: 源代码
        fields: 字段列表
        enhancement_mode: 增强模式

    Returns:
        增强策略: 'traditional', 'single_git_diff'
    """
    # 对支持的增强模式进行策略选择
    if enhancement_mode not in ["initial_enhancement", "review_improvement"]:
        return "traditional"

    # 计算任务复杂度指标
    source_lines = len(source_code.splitlines()) if source_code else 0
    field_count = len(fields) if fields else 0

    # 策略1: 小任务直接用传统方案（简单直接）
    if source_lines <= 200 and field_count <= 5:
        logger.info(f"选择传统方案: 代码行数={source_lines}<=100, 字段数={field_count}<=5")
        return "traditional"

    # 策略2: 其他任务都用单次Git diff（统一方案）
    logger.info(f"选择单次Git diff: 代码行数={source_lines}, 字段数={field_count}")
    return "single_git_diff"


def should_use_phased_approach(source_code: str, fields: List[Dict], enhancement_mode: str) -> bool:
    """
    判断是否应该使用分阶段生成方法

    Args:
        source_code: 源代码
        fields: 字段列表
        enhancement_mode: 增强模式

    Returns:
        True表示应该使用分阶段方法，False表示使用传统单次生成
    """
    # 只对initial_enhancement模式启用分阶段（其他模式通常输出较小）
    if enhancement_mode != "initial_enhancement":
        return False

    # 计算复杂度指标
    source_lines = len(source_code.splitlines()) if source_code else 0
    field_count = len(fields) if fields else 0

    # 触发条件（满足任一即可）
    if source_lines > 1000:
        logger.info(f"触发分阶段生成: 代码行数 {source_lines} > 1000")
        return True

    if field_count > 20:
        logger.info(f"触发分阶段生成: 字段数量 {field_count} > 20")
        return True

    # 边界情况：接近阈值时也使用分阶段
    if source_lines > 800 and field_count > 15:
        logger.info(f"触发分阶段生成: 接近阈值 (行数={source_lines}, 字段={field_count})")
        return True

    # 估算输出大小
    estimated_tokens = estimate_output_tokens(source_lines, field_count)
    if estimated_tokens > 8000:
        logger.info(f"触发分阶段生成: 预估输出 {estimated_tokens} tokens > 8000")
        return True

    return False


def estimate_output_tokens(source_lines: int, field_count: int) -> int:
    """
    估算输出大小（tokens）

    经验公式：
    - 增强代码 ≈ 源代码 * 1.2 + 字段数 * 50
    - DDL语句 ≈ 字段数 * 30
    - ALTER语句 ≈ 字段数 * 20
    - JSON格式开销 ≈ 500
    """
    enhanced_code_tokens = int(source_lines * 2 + field_count * 50)
    ddl_tokens = field_count * 30
    alter_tokens = field_count * 20
    json_overhead = 500

    total = enhanced_code_tokens + ddl_tokens + alter_tokens + json_overhead
    return total


def analyze_code_structure(source_code: str) -> Dict[str, Any]:
    """
    分析代码结构，定位关键代码块

    Returns:
        包含代码结构信息的字典
    """
    lines = source_code.splitlines() if source_code else []
    structure = {
        'total_lines': len(lines),
        'select_blocks': [],
        'from_blocks': [],
        'where_blocks': [],
        'join_blocks': [],
        'create_table_line': -1
    }

    in_select = False
    select_start = -1

    for i, line in enumerate(lines):
        line_upper = line.strip().upper()

        # 检测SELECT块
        if 'SELECT' in line_upper and not in_select:
            in_select = True
            select_start = i
        elif in_select and ('FROM' in line_upper or 'WHERE' in line_upper):
            if select_start != -1:
                structure['select_blocks'].append((select_start, i - 1))
            in_select = False
            select_start = -1

        # 检测其他关键字
        if 'FROM' in line_upper:
            structure['from_blocks'].append(i)
        if 'WHERE' in line_upper:
            structure['where_blocks'].append(i)
        if 'JOIN' in line_upper:
            structure['join_blocks'].append(i)
        if 'CREATE' in line_upper and 'TABLE' in line_upper:
            structure['create_table_line'] = i

    # 处理最后一个SELECT块
    if in_select and select_start != -1:
        structure['select_blocks'].append((select_start, len(lines) - 1))

    return structure




async def execute_single_phase_enhancement(enhancement_mode: str, task_message: str,
                                           enhancement_agent: Any, config: Dict,
                                           table_name: str, **kwargs) -> Dict:
    """
    传统的单次生成方法（保持原有逻辑）
    """
    # 调用全局智能体执行增强任务
    result = await enhancement_agent.ainvoke(
        {"messages": [HumanMessage(task_message)]},
        config,
    )

    # 解析智能体的响应
    response_content = result["messages"][-1].content
    enhancement_result = parse_agent_response(response_content)

    if enhancement_result.get("enhanced_code"):
        logger.info(f"代码增强成功 ({enhancement_mode}): {table_name}")

        return {
            "success": True,
            "enhanced_code": enhancement_result.get("enhanced_code"),
            "new_table_ddl": enhancement_result.get("new_table_ddl"),
            "alter_statements": enhancement_result.get("alter_statements"),
            "table_comment": enhancement_result.get("table_comment"),
            "optimization_summary": enhancement_result.get("optimization_summary", ""),
            "field_mappings": kwargs.get("fields", []),
            "generation_method": "single"  # 标记使用了单次生成
        }
    else:
        error_msg = f"智能体未能生成有效的增强代码 ({enhancement_mode})"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }


async def execute_code_enhancement_task(state: "EDWState", enhancement_mode: str) -> dict:
    """
    统一的代码增强执行引擎 - 优化版本

    直接从state获取所有参数，使用策略执行器模式：
    1. 策略选择：根据代码量和字段数选择最优策略
    2. 执行器创建：为选定的策略创建相应的执行器
    3. 执行增强：执行器自行处理提示词构建和执行逻辑
    """
    try:
        logger.info(f"开始执行代码增强任务: {enhancement_mode}")

        # 1. 智能策略选择（只在这里判断一次）
        source_code = state.get("source_code", "")
        fields = state.get("fields", [])

        if enhancement_mode in ["initial_enhancement", "review_improvement"]:
            strategy = choose_enhancement_strategy(source_code, fields, enhancement_mode)

            # Review模式的特殊处理：对于已有的大代码量，更倾向于使用Git diff
            if enhancement_mode == "review_improvement":
                current_code = state.get("enhance_code", "")  # review模式使用enhance_code作为current_code
                current_lines = len(current_code.splitlines()) if current_code else 0

                # 如果当前代码很大，但原始代码较小，仍使用Git diff方案
                if current_lines > 500 and strategy == "traditional":
                    strategy = "single_git_diff"
                    logger.info(f"Review模式特殊处理: 当前代码{current_lines}行，调整策略为single_git_diff")
        else:
            # 其他模式（如refinement）使用传统策略
            strategy = "traditional"

        table_name = state.get("table_name", "unknown")
        logger.info(f"选择增强策略: {strategy} (模式={enhancement_mode}, 表={table_name})")

        # 2. 创建并执行策略执行器
        enhancer = create_enhancer(strategy, enhancement_mode, state)
        result = await enhancer.execute()

        # 3. 如果Git diff策略失败，降级到传统策略
        if not result.get("success") and strategy == "single_git_diff":
            logger.warning(f"Git diff策略失败，降级到传统策略: {table_name}")

            # 创建传统策略执行器并重新执行
            fallback_enhancer = create_enhancer("traditional", enhancement_mode, state)
            result = await fallback_enhancer.execute()

        if result.get("success"):
            logger.info(f"代码增强成功: {enhancement_mode} - {table_name}")
        else:
            logger.error(f"代码增强失败: {enhancement_mode} - {table_name}")

        return result

    except Exception as e:
        error_msg = f"执行代码增强时发生异常 ({enhancement_mode}): {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg
        }
    finally:
        logger.debug(f"代码增强任务完成 ({enhancement_mode})")


def build_initial_enhancement_prompt(table_name: str, source_code: str, adb_code_path: str,
                                     fields: list, logic_detail: str, code_path: str = "", **kwargs) -> str:
    """构建初始模型增强的提示词 - 完整流程"""

    # 判断代码类型
    file_path = code_path or adb_code_path or ""
    if file_path.endswith('.sql'):
        code_language = "sql"
        code_type_desc = "SQL"
    else:
        code_language = "python"
        code_type_desc = "Python"

    # 构造字段信息字符串
    fields_info = []
    source_names = []  # 收集源字段名用于查询
    source_names_lower = []  # 收集小写的源字段名用于大小写不敏感查询
    for field in fields:
        if isinstance(field, dict):
            source_name = field.get('source_name', '')
            physical_name = field.get('physical_name', '')
            attribute_name = field.get('attribute_name', '')
        else:
            source_name = getattr(field, 'source_name', '')
            physical_name = getattr(field, 'physical_name', '')
            attribute_name = getattr(field, 'attribute_name', '')

        # 显示格式：标准化字段名 (属性描述) <- 源字段名
        fields_info.append(f"{physical_name} ({attribute_name}) <- 源字段: {source_name}")
        if source_name:
            source_names.append(f"'{source_name}'")
            source_names_lower.append(f"'{source_name.lower()}'")

    return f"""你是一个Databricks代码增强专家，负责为数据模型添加新字段。

**任务目标**: 为表 {table_name} 创建增强版本的{code_type_desc}代码，
注意根据用户提出的需求修改，添加字段的顺序需要符合用户要求，
尽可能在修改的地方加上注释，标注为AI修改以及修改时间和大致修改内容，
注意代码如果是python代码，那么可能引用了其他增量处理框架，你需要根据表中目前的字段加工的位置，结合用户需求，推断新增字段在代码中添加字段的位置

**增强需求**: {logic_detail}

**新增字段**:
{chr(10).join(fields_info)}

**原始源代码**:
```
{source_code}
```

**执行步骤**:
1. 查询源字段在底表的数据类型，结合用户逻辑来推断新字段的数据类型
    源字段列表：{', '.join(source_names) if source_names else '无'}
    你可以使用如下类似sql查询（请根据实际底表调整table_schema和table_name）：
         SELECT column_name, full_data_type
         FROM `system`.information_schema.columns
         WHERE table_schema = '相应的schema'
         AND table_name = '相应的底表名'
         AND LOWER(column_name) IN ('')
2. 获取当前表建表语句
    你可以使用如下类似sql查询：
         SHOW CREATE TABLE {table_name};
3. 基于原始代码结合用户逻辑生成增强版本，使用标准化后的physical_name作为新字段名
4. 生成完整的CREATE TABLE和ALTER TABLE语句

**输出要求**: 严格按JSON格式返回
{{
    "enhanced_code": "完整的增强后{code_type_desc}代码",
    "new_table_ddl": "包含新字段的CREATE TABLE语句",
    "alter_statements": "ADD COLUMN的ALTER语句"
}}"""


def build_refinement_prompt(current_code: str, user_feedback: str, table_name: str,
                            original_context: dict, **kwargs) -> str:
    """构建代码微调的提示词 - 针对性优化"""

    return f"""你是一个代码优化专家，负责根据用户反馈修改AI生成的代码。
**用户反馈**: "{user_feedback}"

**优化指导原则**:
1. 重点关注用户的具体反馈，精准响应用户需求
2. 如需查询额外信息，可使用工具
3. 优化可能包括：性能改进、代码可读性、异常处理、注释补充等、属性名称修改、字段顺序修改

**注意事项**:
- 不要重新设计整体架构，只做针对性改进
- 保持与原代码的语言风格一致
- 确保修改后的代码逻辑正确且可执行
- ALTER语句如果有需要请重新生成，需满足alter table ** add column ** comment '' after '';

**输出格式**: 严格按JSON格式返回
{{
    "enhanced_code": "优化后的代码",
    "new_table_ddl": "CREATE TABLE语句（如有需要）",
    "alter_statements": "ALTER语句（如有需要）",
    "optimization_summary": "本次优化的具体改进点说明"
}}"""


def build_review_improvement_prompt(improvement_prompt: str, **kwargs) -> str:
    """构建基于review反馈的代码改进提示词"""
    # 如果已经提供了完整的improvement_prompt，直接使用
    if improvement_prompt:
        return improvement_prompt

    # 否则构建默认的改进提示词
    current_code = kwargs.get("current_code", "")
    review_feedback = kwargs.get("review_feedback", "")
    review_suggestions = kwargs.get("review_suggestions", [])
    table_name = kwargs.get("table_name", "")

    suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"

    return f"""你是一个代码质量改进专家，负责根据代码review反馈改进代码。

**Review反馈**: {review_feedback}

**改进建议**:
{suggestions_text}

**表名**: {table_name}

**当前代码**:
```python
{current_code}
```

**改进要求**:
1. 根据review反馈修复所有问题
2. 实施所有合理的改进建议
3. 保持代码功能不变
4. 提升代码质量和可维护性
5. 如需查询额外信息，可使用工具

**输出格式**: 严格按JSON格式返回
{{
    "enhanced_code": "改进后的完整代码",
    "new_table_ddl": "CREATE TABLE语句（如有变化）",
    "alter_statements": "ALTER语句（如有变化）",
    "optimization_summary": "本次改进的具体内容说明"
}}"""


def format_fields_info(fields: list) -> str:
    """格式化字段信息为字符串"""
    if not fields:
        return "无字段信息"

    fields_info = []
    for field in fields:
        if isinstance(field, dict):
            name = field.get('physical_name', '')
            attr = field.get('attribute_name', '')
        else:
            name = getattr(field, 'physical_name', '')
            attr = getattr(field, 'attribute_name', '')

        if name and attr:
            fields_info.append(f"{name} ({attr})")
        elif name:
            fields_info.append(name)

    return ', '.join(fields_info) if fields_info else "无字段信息"


# ===== Git diff工具函数 =====

def parse_git_diff_chunk_with_unidiff(chunk: str) -> Optional[Dict[str, Any]]:
    """
    使用unidiff库解析Git diff chunk（更可靠的解析）

    Args:
        chunk: Git diff格式的字符串

    Returns:
        包含解析结果的字典或None
    """
    if not UNIDIFF_AVAILABLE:
        return None

    try:
        # 为unidiff构建完整的patch格式
        # unidiff需要完整的文件头部信息
        full_patch = f"""--- a/file
+++ b/file
{chunk}"""

        patch_set = PatchSet(full_patch)

        if not patch_set or not patch_set[0].hunks:
            return None

        # 获取第一个hunk（我们处理的是单个chunk）
        hunk = patch_set[0].hunks[0]

        # 提取修改内容
        context_lines = []
        removed_lines = []
        added_lines = []

        for line in hunk:
            if line.is_context:
                context_lines.append(line.value.rstrip('\n'))
            elif line.is_removed:
                removed_lines.append(line.value.rstrip('\n'))
            elif line.is_added:
                added_lines.append(line.value.rstrip('\n'))

        return {
            'old_start': hunk.source_start,
            'old_count': hunk.source_length,
            'new_start': hunk.target_start,
            'new_count': hunk.target_length,
            'context_lines': context_lines,
            'removed_lines': removed_lines,
            'added_lines': added_lines,
            'raw_chunk': chunk,
            'parsed_by': 'unidiff'
        }

    except Exception as e:
        logger.debug(f"unidiff解析失败: {e}")
        return None


def parse_git_diff_chunk_with_regex(chunk: str) -> Optional[Dict[str, Any]]:
    """
    使用正则表达式解析Git diff chunk（降级方案）

    Args:
        chunk: Git diff格式的字符串

    Returns:
        包含解析结果的字典或None
    """
    lines = chunk.strip().split('\n')
    if not lines or not lines[0].startswith('@@'):
        return None

    # 解析头部信息 @@ -old_start,old_count +new_start,new_count @@
    header = lines[0]
    header_match = re.match(r'@@ -(\d+),(\d+) \+(\d+),(\d+) @@', header)
    if not header_match:
        return None

    old_start, old_count, new_start, new_count = map(int, header_match.groups())

    # 解析修改内容
    context_lines = []
    removed_lines = []
    added_lines = []

    for line in lines[1:]:
        if line.startswith(' '):  # 上下文行
            context_lines.append(line[1:])  # 去掉前缀空格
        elif line.startswith('-'):  # 删除的行
            removed_lines.append(line[1:])  # 去掉前缀-
        elif line.startswith('+'):  # 新增的行
            added_lines.append(line[1:])  # 去掉前缀+

    return {
        'old_start': old_start,
        'old_count': old_count,
        'new_start': new_start,
        'new_count': new_count,
        'context_lines': context_lines,
        'removed_lines': removed_lines,
        'added_lines': added_lines,
        'raw_chunk': chunk,
        'parsed_by': 'regex'
    }


def parse_git_diff_chunk(chunk: str) -> Optional[Dict[str, Any]]:
    """
    解析单个Git diff chunk - 智能选择解析方法

    优先使用unidiff库解析，失败时降级到正则表达式

    Args:
        chunk: Git diff格式的字符串，如：
               "@@ -15,4 +15,7 @@\n context\n-old\n+new\n context"

    Returns:
        包含解析结果的字典或None
    """
    # 优先尝试unidiff解析
    if UNIDIFF_AVAILABLE:
        result = parse_git_diff_chunk_with_unidiff(chunk)
        if result:
            logger.debug("使用unidiff成功解析Git diff chunk")
            return result
        else:
            logger.debug("unidiff解析失败，降级到正则表达式")

    # 降级到正则表达式解析
    result = parse_git_diff_chunk_with_regex(chunk)
    if result:
        logger.debug("使用正则表达式成功解析Git diff chunk")
        return result

    logger.warning("Git diff chunk解析完全失败")
    return None


def apply_git_diff_to_code_with_unidiff(source_code: str, diff_chunks: List[str]) -> Optional[str]:
    """
    使用unidiff库应用Git diff修改（更精确的应用）

    Args:
        source_code: 原始源代码
        diff_chunks: Git diff格式的修改列表

    Returns:
        修改后的完整代码或None（如果应用失败）
    """
    if not UNIDIFF_AVAILABLE or not diff_chunks:
        return None

    try:
        lines = source_code.splitlines()
        applied_count = 0

        # 逐个应用diff chunks
        for i, chunk in enumerate(diff_chunks):
            try:
                # 解析chunk
                parsed = parse_git_diff_chunk_with_unidiff(chunk)
                if not parsed:
                    logger.warning(f"unidiff无法解析chunk {i + 1}/{len(diff_chunks)}")
                    continue

                # 应用修改
                old_lines_count = len(lines)
                lines = apply_single_diff_chunk(lines, parsed)

                if len(lines) != old_lines_count or any(parsed['added_lines']):
                    applied_count += 1
                    logger.debug(f"unidiff成功应用chunk {i + 1}/{len(diff_chunks)}")

            except Exception as e:
                logger.warning(f"unidiff应用chunk {i + 1}失败: {e}")
                continue

        if applied_count > 0:
            logger.info(f"unidiff成功应用{applied_count}/{len(diff_chunks)}个chunk")
            return '\n'.join(lines)
        else:
            logger.warning("unidiff未能应用任何chunk")
            return None

    except Exception as e:
        logger.warning(f"unidiff应用失败: {e}")
        return None


def create_smart_patch_content(source_code: str, diff_content: str) -> Optional[str]:
    """
    智能创建符合标准的patch内容

    Args:
        source_code: 原始源代码
        diff_content: Git diff chunk内容

    Returns:
        标准的patch内容或None
    """
    import re

    try:
        lines = source_code.splitlines()

        # 解析chunk头部信息
        chunk_match = re.match(r'@@\s+-(\d+),(\d+)\s+\+(\d+),(\d+)\s+@@', diff_content)
        if not chunk_match:
            logger.warning("无法解析chunk头部信息")
            return None

        old_start, old_count, new_start, new_count = map(int, chunk_match.groups())

        # 调整行号为0-based索引
        old_start -= 1
        new_start -= 1

        # 确保行号在有效范围内
        if old_start < 0 or old_start >= len(lines):
            logger.warning(f"行号{old_start + 1}超出范围(1-{len(lines)})")
            # 尝试查找相似内容
            chunk_lines = diff_content.split('\n')
            for line in chunk_lines:
                if line.startswith('-'):
                    target_line = line[1:].strip()
                    for i, source_line in enumerate(lines):
                        if target_line in source_line or source_line.strip() == target_line:
                            old_start = max(0, i - 2)  # 提供一些上下文
                            logger.info(f"找到相似内容，调整起始行号为{old_start + 1}")
                            break
                    break

        # 生成更精确的上下文
        context_before = max(0, old_start - 3)
        context_after = min(len(lines), old_start + old_count + 3)

        # 构建完整的patch内容
        patch_lines = [
            "--- a/source.sql",
            "+++ b/source.sql"
        ]

        # 重新计算chunk头部
        context_size = (old_start - context_before) + (context_after - old_start - old_count)
        patch_lines.append(f"@@ -{context_before + 1},{context_after - context_before} +{context_before + 1},{context_after - context_before + new_count - old_count} @@")

        # 添加前置上下文
        for i in range(context_before, old_start):
            if i < len(lines):
                patch_lines.append(f" {lines[i]}")

        # 添加实际的diff内容
        diff_body_lines = diff_content.split('\n')[1:]  # 跳过@@行
        for line in diff_body_lines:
            if line.strip():  # 跳过空行
                patch_lines.append(line)

        # 添加后置上下文
        for i in range(old_start + old_count, context_after):
            if i < len(lines):
                patch_lines.append(f" {lines[i]}")

        return '\n'.join(patch_lines)

    except Exception as e:
        logger.error(f"创建智能patch内容失败: {e}")
        return None


def apply_diff_with_standard_tools(source_code: str, diff_content: str) -> Optional[str]:
    """
    使用标准系统工具应用Git diff修改 - 改进版

    Args:
        source_code: 原始源代码
        diff_content: 完整的Git diff内容

    Returns:
        修改后的代码或None（如果失败）
    """
    import tempfile
    import subprocess
    import os

    try:
        # 创建临时目录
        with tempfile.TemporaryDirectory() as temp_dir:
            # 1. 保存原始代码到临时文件
            source_file = os.path.join(temp_dir, "source.sql")
            with open(source_file, 'w', encoding='utf-8') as f:
                f.write(source_code)

            # 2. 智能创建patch内容
            smart_patch_content = create_smart_patch_content(source_code, diff_content)

            if not smart_patch_content:
                logger.warning("无法创建智能patch内容，使用原始内容")
                smart_patch_content = f"--- a/source.sql\n+++ b/source.sql\n{diff_content}"

            # 保存patch文件
            patch_file = os.path.join(temp_dir, "changes.patch")
            with open(patch_file, 'w', encoding='utf-8') as f:
                f.write(smart_patch_content)

            # 3. 尝试使用patch命令
            patch_success = False
            try:
                # 应用patch，使用更宽松的选项
                result = subprocess.run(
                    ['patch', '--force', '--ignore-whitespace', '--no-backup-if-mismatch', source_file, patch_file],
                    capture_output=True, text=True, timeout=30
                )

                if result.returncode == 0:
                    patch_success = True
                    logger.info("使用patch命令成功应用Git diff")
                else:
                    logger.debug(f"patch命令失败: {result.stderr}")

            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
                logger.debug(f"patch命令不可用或失败: {e}")

            # 4. 如果patch失败，尝试git apply
            if not patch_success:
                try:
                    # 使用git apply，添加更多容错选项
                    result = subprocess.run(
                        ['git', 'apply', '--ignore-space-change', '--ignore-whitespace', '--3way', patch_file],
                        cwd=temp_dir, capture_output=True, text=True, timeout=30
                    )

                    if result.returncode == 0:
                        patch_success = True
                        logger.info("使用git apply成功应用Git diff")
                    else:
                        logger.debug(f"git apply失败: {result.stderr}")

                except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
                    logger.debug(f"git apply不可用或失败: {e}")

            # 5. 读取修改后的文件内容
            if patch_success and os.path.exists(source_file):
                try:
                    with open(source_file, 'r', encoding='utf-8') as f:
                        modified_code = f.read()

                    # 验证修改结果
                    if modified_code and len(modified_code) >= len(source_code):
                        logger.info("标准工具应用diff成功")
                        return modified_code
                    else:
                        logger.warning("标准工具应用后代码内容异常")

                except Exception as read_error:
                    logger.error(f"读取修改后文件失败: {read_error}")

            return None

    except Exception as e:
        logger.error(f"标准工具应用diff异常: {e}")
        return None


def apply_git_diff_to_code(source_code: str, diff_chunks: List[str]) -> str:
    """
    将Git diff修改应用到源代码 - 优先使用标准工具

    Args:
        source_code: 原始源代码
        diff_chunks: Git diff格式的修改列表

    Returns:
        修改后的完整代码
    """
    if not diff_chunks:
        logger.info("没有diff chunks需要应用")
        return source_code

    logger.info(f"开始应用{len(diff_chunks)}个Git diff修改")

    # 合并所有diff chunks为完整的diff内容
    full_diff_content = '\n'.join(diff_chunks)

    # 优先尝试标准工具方法
    standard_result = apply_diff_with_standard_tools(source_code, full_diff_content)
    if standard_result is not None:
        logger.info("使用标准工具成功应用Git diff修改")
        return standard_result
    else:
        logger.info("标准工具应用失败，降级到unidiff方法")

    # 降级到unidiff方法
    if UNIDIFF_AVAILABLE:
        unidiff_result = apply_git_diff_to_code_with_unidiff(source_code, diff_chunks)
        if unidiff_result is not None:
            logger.info("使用unidiff成功应用Git diff修改")
            return unidiff_result
        else:
            logger.info("unidiff应用失败，降级到传统方法")

    # 降级到传统方法
    try:
        lines = source_code.splitlines()

        # 解析所有diff chunks
        parsed_chunks = []
        for i, chunk in enumerate(diff_chunks):
            parsed = parse_git_diff_chunk(chunk)
            if parsed:
                parsed_chunks.append(parsed)
            else:
                logger.warning(f"无法解析chunk {i + 1}/{len(diff_chunks)}")

        if not parsed_chunks:
            logger.warning("没有有效的diff chunks，返回原始代码")
            return source_code

        # 按照起始行号排序（从后往前应用，避免行号偏移）
        parsed_chunks.sort(key=lambda x: x['old_start'], reverse=True)

        # 应用每个修改
        applied_count = 0
        for i, chunk in enumerate(parsed_chunks):
            try:
                old_lines_count = len(lines)
                lines = apply_single_diff_chunk(lines, chunk)

                if len(lines) != old_lines_count or chunk.get('added_lines'):
                    applied_count += 1
                    logger.debug(f"传统方法成功应用chunk {i + 1}/{len(parsed_chunks)}")

            except Exception as e:
                logger.warning(f"传统方法应用chunk {i + 1}失败: {e}")
                continue

        logger.info(f"传统方法成功应用{applied_count}/{len(parsed_chunks)}个chunk")
        return '\n'.join(lines)

    except Exception as e:
        logger.error(f"传统方法应用Git diff失败: {e}")
        # 返回原始代码
        return source_code


def normalize_line_for_comparison(line: str) -> str:
    """标准化行内容用于比较，移除多余空白字符"""
    import re
    # 移除行首行尾空白，将多个空白字符压缩为单个空格
    return re.sub(r'\s+', ' ', line.strip())


def find_best_match_position(lines: List[str], target_lines: List[str], start_hint: int) -> Optional[int]:
    """
    智能查找最佳匹配位置

    Args:
        lines: 源代码行列表
        target_lines: 要匹配的行列表
        start_hint: 建议开始位置

    Returns:
        匹配位置或None
    """
    if not target_lines:
        return None

    # 标准化目标行
    normalized_targets = [normalize_line_for_comparison(line) for line in target_lines]

    # 扩大搜索范围
    search_start = max(0, start_hint - 50)
    search_end = min(len(lines), start_hint + 50)

    # 1. 尝试精确匹配整个序列
    for i in range(search_start, search_end - len(target_lines) + 1):
        if i + len(target_lines) <= len(lines):
            normalized_current = [normalize_line_for_comparison(lines[i + j]) for j in range(len(target_lines))]
            if normalized_current == normalized_targets:
                logger.debug(f"找到精确匹配位置: {i}")
                return i

    # 2. 尝试匹配第一行
    first_target = normalized_targets[0]
    for i in range(search_start, search_end):
        if i < len(lines):
            current_line = normalize_line_for_comparison(lines[i])
            if current_line == first_target:
                logger.debug(f"找到第一行匹配位置: {i}")
                return i

    # 3. 尝试部分匹配（包含关系）
    for i in range(search_start, search_end):
        if i < len(lines):
            current_line = normalize_line_for_comparison(lines[i])
            if first_target in current_line or current_line in first_target:
                logger.debug(f"找到部分匹配位置: {i}")
                return i

    logger.warning(f"无法找到匹配位置，目标内容: {first_target[:50]}...")
    return None


def apply_single_diff_chunk(lines: List[str], chunk: Dict[str, Any]) -> List[str]:
    """
    应用单个Git diff chunk到代码行列表 - 增强版

    使用智能匹配算法和详细诊断信息
    """
    try:
        old_start = chunk['old_start'] - 1  # 转为0-based索引
        removed_lines = chunk['removed_lines']
        added_lines = chunk['added_lines']

        logger.debug(f"应用chunk: 起始行{old_start + 1}, 删除{len(removed_lines)}行, 添加{len(added_lines)}行")

        # 如果没有删除行，直接插入新行
        if not removed_lines:
            # 确保插入位置有效
            insert_pos = min(old_start, len(lines))
            for i, new_line in enumerate(added_lines):
                lines.insert(insert_pos + i, new_line)
            logger.debug(f"在位置{insert_pos}插入{len(added_lines)}行")
            return lines

        # 使用智能匹配查找删除行的位置
        match_index = find_best_match_position(lines, removed_lines, old_start)

        if match_index is not None:
            # 验证匹配的合理性
            if match_index + len(removed_lines) <= len(lines):
                # 删除匹配的行
                for _ in range(len(removed_lines)):
                    if match_index < len(lines):
                        removed_line = lines.pop(match_index)
                        logger.debug(f"删除行: {removed_line.strip()[:50]}...")

                # 在删除位置插入新行
                for i, new_line in enumerate(added_lines):
                    lines.insert(match_index + i, new_line)
                    logger.debug(f"插入行: {new_line.strip()[:50]}...")

                logger.info(f"成功应用chunk: 在位置{match_index}删除{len(removed_lines)}行，插入{len(added_lines)}行")
            else:
                logger.warning(f"匹配位置{match_index}超出边界，使用fallback策略")
                # Fallback: 在原始位置插入新行
                insert_pos = min(old_start, len(lines))
                for i, new_line in enumerate(added_lines):
                    lines.insert(insert_pos + i, new_line)
        else:
            # 如果找不到匹配，提供详细诊断信息
            logger.warning(f"未找到删除行的匹配位置")
            logger.warning(f"目标删除行预览:")
            for i, line in enumerate(removed_lines[:3]):  # 只显示前3行
                logger.warning(f"  -{line.strip()[:80]}")

            logger.warning(f"原始位置{old_start}附近的代码:")
            context_start = max(0, old_start - 3)
            context_end = min(len(lines), old_start + 3)
            for i in range(context_start, context_end):
                marker = ">>>" if i == old_start else "   "
                logger.warning(f"  {marker}{i + 1}: {lines[i].strip()[:80]}")

            # Fallback策略：在原始位置插入新行
            insert_pos = min(old_start, len(lines))
            for i, new_line in enumerate(added_lines):
                lines.insert(insert_pos + i, new_line)
            logger.info(f"Fallback: 在位置{insert_pos}插入{len(added_lines)}行新内容")

        return lines

    except Exception as e:
        logger.error(f"应用单个diff chunk失败: {e}")
        logger.error(f"Chunk详情: 起始行{chunk.get('old_start')}, 删除{len(chunk.get('removed_lines', []))}行")
        return lines


def validate_code_syntax(code: str, language: str) -> Tuple[bool, str]:
    """
    验证代码语法是否正确

    Args:
        code: 要验证的代码
        language: 代码语言（python/sql）

    Returns:
        (是否有效, 错误消息)
    """
    if language.lower() == 'python':
        try:
            import ast
            ast.parse(code)
            return True, ""
        except SyntaxError as e:
            return False, f"Python语法错误: {e}"
        except Exception as e:
            return False, f"代码验证失败: {e}"

    elif language.lower() == 'sql':
        # SQL语法验证相对复杂，这里做简单检查
        # 检查基本的SQL关键词和括号匹配
        try:
            # 简单的括号匹配检查
            open_parens = code.count('(')
            close_parens = code.count(')')
            if open_parens != close_parens:
                return False, "SQL括号不匹配"

            # 检查是否包含基本SQL结构
            code_upper = code.upper()
            if 'SELECT' in code_upper or 'CREATE' in code_upper or 'INSERT' in code_upper:
                return True, ""
            else:
                return False, "未检测到有效的SQL语句"

        except Exception as e:
            return False, f"SQL验证失败: {e}"

    # 其他语言暂时不验证
    return True, ""


def get_unidiff_status() -> Dict[str, Any]:
    """
    获取unidiff集成状态信息

    Returns:
        unidiff状态信息
    """
    status = {
        "unidiff_available": UNIDIFF_AVAILABLE,
        "import_error": None,
        "version": None,
        "features": {
            "parse_git_diff_chunk_with_unidiff": True,
            "apply_git_diff_to_code_with_unidiff": True,
            "intelligent_fallback": True
        }
    }

    if UNIDIFF_AVAILABLE:
        try:
            from unidiff import __version__ as unidiff_version
            status["version"] = unidiff_version
        except BaseException:
            status["version"] = "unknown"
    else:
        status["import_error"] = "unidiff library not installed or import failed"

    return status


def build_git_diff_only_prompt(table_name: str, source_code: str, fields: List[Dict],
                               logic_detail: str, code_language: str = "sql",
                               enhancement_mode: str = "initial_enhancement") -> str:
    """
    构建专门生成Git diff的prompt - 第一步：作为记忆锚点的代码增强

    这是三步增强流程的第一步，充当记忆锚点，为后续步骤提供完整上下文。

    Args:
        table_name: 表名
        source_code: 源代码
        fields: 字段列表
        logic_detail: 逻辑详情
        code_language: 代码语言
        enhancement_mode: 增强模式

    Returns:
        专注于Git diff生成的prompt，包含完整上下文信息作为记忆锚点
    """
    # 处理字段信息
    all_fields_info = []
    source_names = []

    for i, field in enumerate(fields, 1):
        if isinstance(field, dict):
            source_name = field.get('source_name', '')
            physical_name = field.get('physical_name', '')
            attribute_name = field.get('attribute_name', '')
        else:
            source_name = getattr(field, 'source_name', '')
            physical_name = getattr(field, 'physical_name', '')
            attribute_name = getattr(field, 'attribute_name', '')

        field_info = f"{i}.{physical_name} ({attribute_name}) <- 源字段: {source_name}"
        all_fields_info.append(field_info)

        if source_name:
            source_names.append(f"'{source_name}'")
    source_preview = source_code  # 显示完整代码
    if enhancement_mode == "review_improvement":
        prompt = f"""**【三步增强流程 - 步骤1/3：Git diff代码修改】** 记忆锚点

你是专业的代码评审改进专家，这是三步增强流程的第一步，请牢记以下完整上下文信息，后续步骤将基于此信息进行：

**改进任务**: 根据评审建议对代码进行改进

**🎯 核心上下文信息（后续步骤会引用）**:
- **目标表**: {table_name}
- **用户逻辑需求**: {logic_detail}
- **所有字段** (共{len(fields)}个):
{chr(10).join(all_fields_info)}

**原始代码**:
```
{source_preview}
```

**任务要求**:
**专注任务**: 只生成Git diff格式的代码修改，不生成其他内容
1. 分析所有{len(fields)}个字段的添加需求
2. 生成精确的Git diff修改格式
3. 确保所有字段都被正确添加到代码中
4. 添加适当的注释标记修改位置

**Git diff格式要求**:
- 使用标准格式: `@@ -old_start,old_count +new_start,new_count @@`
- 包含上下文行(以空格开头)
- 删除行以`-`开头，新增行以`+`开头
- 每个修改包含足够的上下文(3-5行)

**输出格式**:
```json
{{
    "analysis_summary": "对{len(fields)}个字段的代码修改分析(100字内)",
    "git_diffs": [
        {{
            "chunk": "@@ -15,4 +15,7 @@\\n context_line\\n-old_line\\n+new_line1\\n+new_line2\\n context_line",
            "description": "修改描述"
        }}
    ],
    "total_fields_processed": {len(fields)}
}}
```

**记忆锚点提醒**:
这是三步流程的第一步，请确保记住上述完整上下文信息（表名、用户需求、字段详情），因为第二步和第三步将直接引用这些信息，不会重复详细描述。

注意: 只返回Git diff相关内容，确保覆盖所有{len(fields)}个字段。"""
    else:
        prompt = f"""**【三步增强流程 - 步骤1/3：Git diff代码修改】** 🔗记忆锚点

你是专业的Spark代码增强专家，这是三步增强流程的第一步，请牢记以下完整上下文信息，后续步骤将基于此信息进行：

**核心上下文信息（后续步骤会引用）**:
- **用户增强需求**: {logic_detail}
- **所有新增字段** (共{len(fields)}个):
{chr(10).join(all_fields_info)}

**核心任务**: 基于用户需求对代码进行增强，使用Git diff格式输出修改

**源代码类型**: {code_language}
**完整源代码**:
```
{source_preview}
```

**专注任务**: 只生成Git diff格式的代码修改

**执行要求**:
0. 注意代码如果是python代码，那么可能引用了其他增量处理框架，你需要根据表中目前的字段加工的位置，结合用户需求，推断新增字段在代码中添加字段的位置
1. 分析所有{len(fields)}个字段在源代码中的添加位置
2. 生成精确的Git diff修改格式
3. 确保所有字段都被正确添加
4. 添加注释标记: `-- AI添加: 字段描述`
5.确保增强过后的sql或python代码语法正确，达到预期新增字段效果，注意sql嵌套场景，如果有sql子查询，注意sql子查询中也要添加字段，
不能盲目只在最高层查询添加，和最底层添加

**Git diff格式要求**:
- 使用标准格式: `@@ -old_start,old_count +new_start,new_count @@`
- 包含足够的上下文行(3-5行)
- 删除行以`-`开头，新增行以`+`开头
- 多个修改点时，以数组形式返回

**严格JSON输出格式**:
```json
{{
    "analysis_summary": "对{len(fields)}个字段的整体分析和修改策略(100字内)",
    "git_diffs": [
        {{
            "chunk": "@@ -15,4 +15,7 @@\\n context_line\\n-old_line\\n+new_line1\\n+new_line2\\n context_line",
            "description": "修改描述"
        }}
    ],
    "total_fields_processed": {len(fields)}
}}
```
**记忆锚点提醒**:
这是三步流程的第一步，请确保记住上述完整上下文信息（表名、用户需求、字段详情），因为第二步和第三步将直接引用这些信息，不会重复详细描述。
重要: 只专注Git diff生成，确保覆盖所有{len(fields)}个字段的添加。"""

    return prompt


def build_create_table_prompt(table_name: str, fields: List[Dict],
                              logic_detail: str, git_diffs_result: Dict) -> str:
    """
    构建生成CREATE TABLE语句的prompt - 第二步：生成建表语句（记忆优化版）

    利用大模型记忆功能，简化prompt内容，引用前面上下文中的详细信息

    Args:
        table_name: 表名
        fields: 字段列表
        logic_detail: 用户逻辑需求（可能包含字段位置要求等）
        git_diffs_result: 第一步的Git diff结果

    Returns:
        简化的CREATE TABLE生成prompt
    """
    # 构造字段信息字符串
    fields_info = []
    source_names = []  # 收集源字段名用于查询
    source_names_lower = []  # 收集小写的源字段名用于大小写不敏感查询
    for field in fields:
        if isinstance(field, dict):
            source_name = field.get('source_name', '')
            physical_name = field.get('physical_name', '')
            attribute_name = field.get('attribute_name', '')
        else:
            source_name = getattr(field, 'source_name', '')
            physical_name = getattr(field, 'physical_name', '')
            attribute_name = getattr(field, 'attribute_name', '')

        # 显示格式：标准化字段名 (属性描述) <- 源字段名
        fields_info.append(f"{physical_name} ({attribute_name}) <- 源字段: {source_name}")
        if source_name:
            source_names.append(f"'{source_name}'")
    prompt = f"""**【步骤2/3：CREATE TABLE语句生成】**

基于前面分析的用户逻辑需求和{len(fields)}个字段的详细信息，现在专门生成CREATE TABLE语句。

**任务确认**：
- 用户增强需求: {logic_detail}
- 第一步已完成: {git_diffs_result.get('total_fields_processed', len(fields))}个字段的代码修改
- 请参考前面提到的用户位置要求和字段分组需求

**执行要求**:
1. 查询源字段在底表的数据类型，结合用户逻辑来推断新字段的数据类型
    源字段列表：{', '.join(source_names) if source_names else '无'}
    你可以使用如下类似sql查询（请根据实际底表调整table_schema和table_name）：
         SELECT column_name, full_data_type
         FROM `system`.information_schema.columns
         WHERE table_schema = '相应的schema'
         AND table_name = '相应的底表名'
         AND LOWER(column_name) IN ('')
2. 获取当前表建表语句: `SHOW CREATE TABLE {table_name}`
3. 根据前面用户逻辑需求确定新增字段位置
4. 生成完整CREATE TABLE语句（包含原有+新增字段）

**输出格式**:
```json
{{
    "create_table_analysis": "建表语句生成分析(100字内)",
    "new_table_ddl": "完整CREATE TABLE语句",
    "field_positioning": "字段位置处理说明",
    "ddl_validation": "语句正确性验证说明"
}}
```"""

    return prompt


def build_alter_table_prompt(table_name: str, fields: List[Dict],
                             logic_detail: str, create_table_result: Dict) -> str:
    """
    构建生成ALTER TABLE语句的prompt - 第三步：生成ALTER语句（优化版）

    Args:
        table_name: 表名
        fields: 字段列表
        logic_detail: 用户逻辑需求（可能包含字段位置要求等）
        create_table_result: 第二步的CREATE TABLE结果

    Returns:
        专注于ALTER TABLE生成的prompt
    """
    prompt = f"""**【步骤3/3：ALTER TABLE语句生成】**

基于前面的用户需求分析和第二步CREATE TABLE结果，现在生成对应的ALTER TABLE语句。

**任务确认**：
- 目标表: {table_name}
- 第二步已完成: {create_table_result.get('new_fields_count', len(fields))}个字段的CREATE TABLE语句
- 位置策略: {create_table_result.get('field_positioning', '参考前面用户需求')}

**执行要求**:
1. 参考前面分析的用户位置需求和{len(fields)}个字段信息
2. 根据位置要求选择语法：批量添加 `ADD COLUMNS (...)` 或逐个添加 `ADD COLUMN ... AFTER ...`
3. 确保字段数据类型和comment与CREATE TABLE保持一致

**输出格式**:
```json
{{
    "alter_analysis": "ALTER语句生成分析(100字内)",
    "alter_statements": "完整ALTER TABLE语句",
    "positioning_strategy": "位置添加策略说明",
    "alter_validation": "语句正确性验证说明"
}}
```"""

    return prompt


def build_single_git_diff_prompt(table_name: str, source_code: str, fields: List[Dict],
                                 logic_detail: str, code_language: str, code_type_desc: str, **kwargs) -> str:
    """
    构建单次Git diff生成的提示词 - 只生成Git diff版本（与分批次策略保持一致）

    为了与新的分批次策略保持一致，此函数现在只专注于生成Git diff修改，
    不再生成CREATE TABLE和ALTER TABLE语句（这些由专门的函数处理）

    Args:
        table_name: 表名
        source_code: 源代码
        fields: 所有字段列表
        logic_detail: 增强逻辑描述
        code_language: 代码语言
        code_type_desc: 代码类型描述

    Returns:
        专注于Git diff生成的提示词字符串
    """

    # 直接调用专门的Git diff prompt构建函数，保持一致性
    return build_git_diff_only_prompt(
        table_name=table_name,
        source_code=source_code,
        fields=fields,
        logic_detail=logic_detail,
        code_language=code_language,
        enhancement_mode=kwargs.get("enhancement_mode", "initial_enhancement")
    )


def create_git_diff_chunks(original_lines: List[str], modified_lines: List[str],
                           context_lines: int = 3) -> List[str]:
    """
    比较两个代码版本，生成Git diff格式的chunks

    这个函数用于测试和验证diff应用的正确性
    """
    try:
        import difflib

        # 使用difflib生成unified diff
        diff = difflib.unified_diff(
            original_lines,
            modified_lines,
            lineterm='',
            n=context_lines
        )

        # 解析unified diff输出为我们的chunk格式
        diff_lines = list(diff)
        chunks = []
        current_chunk = []

        for line in diff_lines:
            if line.startswith('@@'):
                if current_chunk:
                    chunks.append('\n'.join(current_chunk))
                current_chunk = [line]
            elif line.startswith(' ') or line.startswith('-') or line.startswith('+'):
                current_chunk.append(line)

        if current_chunk:
            chunks.append('\n'.join(current_chunk))

        return chunks

    except Exception as e:
        logger.error(f"生成Git diff失败: {e}")
        return []


# ===== 策略执行器类 =====

class BaseEnhancer:
    """
    基础策略执行器 - 定义统一接口
    """

    def __init__(self, enhancement_mode: str, state: "EDWState"):
        self.mode = enhancement_mode
        self.state = state
        self.table_name = state.get("table_name", "unknown")
        self.user_id = state.get("user_id", "")

    def build_prompt(self) -> str:
        """子类实现具体的提示词构建逻辑"""
        raise NotImplementedError("Subclass must implement build_prompt")

    async def execute(self) -> dict:
        """子类实现具体的执行逻辑"""
        raise NotImplementedError("Subclass must implement execute")


class TraditionalEnhancer(BaseEnhancer):
    """
    传统策略执行器 - 生成完整代码JSON格式
    """

    def build_prompt(self) -> str:
        """根据增强模式构建传统格式提示词"""
        if self.mode == "initial_enhancement":
            return build_initial_enhancement_prompt(
                table_name=self.state.get("table_name", ""),
                source_code=self.state.get("source_code", ""),
                adb_code_path=self.state.get("adb_code_path", ""),
                fields=self.state.get("fields", []),
                logic_detail=self.state.get("logic_detail", ""),
                code_path=self.state.get("code_path", "")
            )
        elif self.mode == "review_improvement":
            return self._build_traditional_review_prompt()
        elif self.mode == "refinement":
            return self._build_traditional_refinement_prompt()
        else:
            raise ValueError(f"Unsupported enhancement mode: {self.mode}")

    def _build_traditional_review_prompt(self) -> str:
        """构建传统格式的review改进提示词"""
        current_code = self.state.get("enhance_code", "")  # review模式使用enhance_code作为当前代码
        review_feedback = self.state.get("review_feedback", "")
        review_suggestions = self.state.get("review_suggestions", [])
        requirement_report = self.state.get("requirement_fulfillment_report", {})
        code_language = self.state.get("code_language", "sql")

        # 🔍 调试：检查执行器中review结果的获取情况
        logger.info(f"🔍 TraditionalEnhancer review提示词构建调试:")
        logger.info(f"  - review_feedback存在: {bool(review_feedback)}, 长度: {len(review_feedback) if review_feedback else 0}")
        logger.info(f"  - review_suggestions存在: {bool(review_suggestions)}, 数量: {len(review_suggestions) if review_suggestions else 0}")
        if not review_feedback:
            logger.warning("⚠️ review_feedback为空，可能导致提示词不包含review结果")
        if not review_suggestions:
            logger.warning("⚠️ review_suggestions为空，可能导致提示词不包含改进建议")

        # 直接从state获取
        table_name = self.state.get("table_name", "")
        logic_detail = self.state.get("logic_detail", "")

        suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"

        # 强调需求不符
        requirement_focus = ""
        if requirement_report and not requirement_report.get("is_fulfilled", True):
            summary = requirement_report.get("summary", "")
            if summary:
                requirement_focus = f"\n**需求问题**：{summary}\n"

        return f"""根据review反馈改进代码。

**Review反馈**：
{review_feedback}

**改进建议**：
{suggestions_text}
{requirement_focus}
**原始需求**：
- 表名: {table_name}
- 逻辑: {logic_detail}

**当前代码**：
```{code_language}
{current_code}
```

**任务**：
1. 修复所有问题
2. 确保满足用户需求
3. 提升代码质量

**输出要求**：严格按JSON格式返回
{{
    "enhanced_code": "改进后的完整代码",
    "new_table_ddl": "CREATE TABLE语句（如有变化）",
    "alter_statements": "ALTER语句（如有变化）",
    "optimization_summary": "本次改进的说明"
}}"""

    def _build_traditional_refinement_prompt(self) -> str:
        """构建传统格式的微调提示词"""
        # 直接从state获取参数
        current_code = self.state.get("enhance_code", "")
        user_feedback = self.state.get("refinement_requirements", "")
        original_context = {
            "logic_detail": self.state.get("logic_detail", ""),
            "fields_info": format_fields_info(self.state.get("fields", []))
        }
        return build_refinement_prompt(
            current_code=current_code,
            user_feedback=user_feedback,
            table_name=self.table_name,
            original_context=original_context
        )

    async def execute(self) -> dict:
        """执行传统策略"""
        try:
            # 构建提示词
            prompt = self.build_prompt()

            # 获取智能体和配置
            from src.agent.edw_agents import get_code_enhancement_agent
            enhancement_agent = get_code_enhancement_agent()

            from src.graph.utils.session import SessionManager
            config = SessionManager.get_config_with_monitor(
                user_id=self.user_id,
                agent_type=f"enhancement_{self.table_name}",
                state=self.state,
                node_name="code_enhancement_traditional",
                enhanced_monitoring=True
            )

            # 执行传统单次生成 - 传递所需的参数
            return await execute_single_phase_enhancement(
                enhancement_mode=self.mode,
                task_message=prompt,
                enhancement_agent=enhancement_agent,
                config=config,
                table_name=self.table_name,
                fields=self.state.get("fields", [])  # 只传递execute_single_phase_enhancement需要的参数
            )

        except Exception as e:
            logger.error(f"传统策略执行失败: {e}")
            return {
                "success": False,
                "error": f"传统策略执行失败: {str(e)}"
            }


class GitDiffEnhancer(BaseEnhancer):
    """
    Git Diff策略执行器 - 生成Git diff格式JSON
    """

    def build_prompt(self) -> str:
        """根据增强模式构建Git diff格式提示词"""
        if self.mode == "initial_enhancement":
            return self._build_git_diff_initial_prompt()
        elif self.mode == "review_improvement":
            return self._build_git_diff_review_prompt()
        else:
            raise ValueError(f"Git diff strategy does not support mode: {self.mode}")

    def _build_git_diff_initial_prompt(self) -> str:
        """构建Git diff格式的初始增强提示词"""
        table_name = self.state.get("table_name", "")
        source_code = self.state.get("source_code", "")
        fields = self.state.get("fields", [])
        logic_detail = self.state.get("logic_detail", "")
        code_path = self.state.get("code_path", "")
        adb_code_path = self.state.get("adb_code_path", "")

        # 判断代码类型
        file_path = code_path or adb_code_path or ""
        code_language = "sql" if file_path.endswith('.sql') else "python"

        # 直接调用build_git_diff_only_prompt，避免通过build_single_git_diff_prompt造成循环
        return build_git_diff_only_prompt(
            table_name=table_name,
            source_code=source_code,
            fields=fields,
            logic_detail=logic_detail,
            code_language=code_language,
            enhancement_mode="initial_enhancement"
        )

    def _build_git_diff_review_prompt(self) -> str:
        """构建Git diff格式的review改进提示词"""
        current_code = self.state.get("enhance_code", "")  # review模式使用enhance_code作为当前代码
        review_feedback = self.state.get("review_feedback", "")
        review_suggestions = self.state.get("review_suggestions", [])
        requirement_report = self.state.get("requirement_fulfillment_report", {})
        code_language = self.state.get("code_language", "sql")

        # 🔍 调试：检查Git diff执行器中review结果的获取情况
        logger.info(f"🔍 GitDiffEnhancer review提示词构建调试:")
        logger.info(f"  - review_feedback存在: {bool(review_feedback)}, 长度: {len(review_feedback) if review_feedback else 0}")
        logger.info(f"  - review_suggestions存在: {bool(review_suggestions)}, 数量: {len(review_suggestions) if review_suggestions else 0}")
        if not review_feedback:
            logger.warning("⚠️ review_feedback为空，可能导致提示词不包含review结果")
        if not review_suggestions:
            logger.warning("⚠️ review_suggestions为空，可能导致提示词不包含改进建议")

        # 直接从state获取
        table_name = self.state.get("table_name", "")
        logic_detail = self.state.get("logic_detail", "")
        fields = self.state.get("fields", [])

        suggestions_text = "\n".join([f"- {s}" for s in review_suggestions]) if review_suggestions else "无"

        # 强调需求不符
        requirement_focus = ""
        if requirement_report and not requirement_report.get("is_fulfilled", True):
            summary = requirement_report.get("summary", "")
            if summary:
                requirement_focus = f"\n**需求问题**：{summary}\n"

        # 获取字段信息
        fields_info = []
        for i, field in enumerate(fields, 1):
            if isinstance(field, dict):
                source_name = field.get('source_name', '')
                physical_name = field.get('physical_name', '')
                attribute_name = field.get('attribute_name', '')
            else:
                source_name = getattr(field, 'source_name', '')
                physical_name = getattr(field, 'physical_name', '')
                attribute_name = getattr(field, 'attribute_name', '')

            field_info = f"{i}. {physical_name} ({attribute_name}) <- 源字段: {source_name}"
            fields_info.append(field_info)

        return f"""你是一个专业的代码质量改进专家，使用Git diff格式进行精确的代码修改。

**任务目标**: 根据review反馈改进表 {table_name} 的{code_language.upper()}代码，使用Git diff格式输出所有修改。

**Review反馈**: {review_feedback}

**改进建议**:
{suggestions_text}
{requirement_focus}
**原始需求**:
- 表名: {table_name}
- 逻辑: {logic_detail}

**新增字段** (共{len(fields)}个):
{chr(10).join(fields_info)}

**当前代码**:
```{code_language}
{current_code}
```

**改进要求**:
1. 根据review反馈修复所有问题
2. 确保满足用户需求
3. 提升代码质量
4. 如需查询额外信息，可使用工具

**Git diff格式要求**:
- 使用标准格式: `@@ -old_start,old_count +new_start,new_count @@`
- 包含上下文行(以空格开头)
- 删除行以`-`开头，新增行以`+`开头
- 每个修改包含足够的上下文(3-5行)

**严格按以下JSON格式输出** (总长度控制在2000字内):
{{
    "analysis_summary": "对review反馈的整体分析和改进策略(100字内)",
    "git_diffs": [
        {{
            "chunk": "@@ -15,4 +15,7 @@\\n context_line\\n-old_line\\n+new_line1\\n+new_line2\\n context_line",
            "description": "根据review反馈的具体改进描述"
        }}
    ],
    "new_table_ddl": "包含改进后字段的完整CREATE TABLE语句(如果需要)",
    "alter_statements": "批量ADD COLUMNS语句: ALTER TABLE {table_name} ADD COLUMNS (...)",
    "table_comment": "表注释更新(如果需要)",
    "optimization_summary": "本次改进的具体内容说明"
}}

注意: git_diffs数组应包含所有必要的改进修改，确保解决review中提到的所有问题。"""

    async def execute(self) -> dict:
        """执行Git diff策略 - 分批次生成版本"""
        try:
            # Review模式下使用current_code作为基础代码
            source_code = self.state.get("enhance_code", "") if self.mode == "review_improvement" else self.state.get("source_code", "")
            fields = self.state.get("fields", [])
            logic_detail = self.state.get("logic_detail", "")
            code_path = self.state.get("code_path", "")
            adb_code_path = self.state.get("adb_code_path", "")

            # 判断代码类型
            file_path = code_path or adb_code_path or ""
            code_language = "sql" if file_path.endswith('.sql') else "python"

            logger.info(f"开始分批次Git diff增强: 表={self.table_name}, 字段数={len(fields)}, 模式={self.mode}")

            # 获取智能体和配置
            from src.agent.edw_agents import get_code_enhancement_agent
            enhancement_agent = get_code_enhancement_agent()

            from src.graph.utils.session import SessionManager
            config = SessionManager.get_config_with_monitor(
                user_id=self.user_id,
                agent_type=f"enhancement_{self.table_name}",
                state=self.state,
                node_name="code_enhancement_git_diff",
                enhanced_monitoring=True
            )

            # 第一步：生成Git diff代码修改
            logger.info("步骤1/3: 生成Git diff代码修改")
            git_diff_prompt = build_git_diff_only_prompt(
                table_name=self.table_name,
                source_code=source_code,
                fields=fields,
                logic_detail=logic_detail,
                code_language=code_language,
                enhancement_mode=self.mode
            )

            git_diff_result = await enhancement_agent.ainvoke(
                {"messages": [HumanMessage(git_diff_prompt)]},
                config
            )

            git_diff_content = git_diff_result["messages"][-1].content
            git_diff_data = extract_json_from_response(git_diff_content)

            if not git_diff_data or not git_diff_data.get("git_diffs"):
                logger.warning("步骤1失败：未能生成有效的Git diff")
                return {"success": False, "error": "Git diff生成失败", "step": 1}

            logger.info(f"步骤1完成：生成了{len(git_diff_data.get('git_diffs', []))}个Git diff修改")

            # 第二步：生成CREATE TABLE语句
            logger.info("步骤2/3: 生成CREATE TABLE语句")
            create_table_prompt = build_create_table_prompt(
                table_name=self.table_name,
                fields=fields,
                logic_detail=logic_detail,
                git_diffs_result=git_diff_data
            )

            create_table_result = await enhancement_agent.ainvoke(
                {"messages": [HumanMessage(create_table_prompt)]},
                config
            )

            create_table_content = create_table_result["messages"][-1].content
            create_table_data = extract_json_from_response(create_table_content)

            if not create_table_data or not create_table_data.get("new_table_ddl"):
                logger.warning("步骤2失败：未能生成有效的CREATE TABLE语句")
                return {"success": False, "error": "CREATE TABLE生成失败", "step": 2}

            logger.info("步骤2完成：生成CREATE TABLE语句")

            # 第三步：生成ALTER TABLE语句
            logger.info("步骤3/3: 生成ALTER TABLE语句")
            alter_table_prompt = build_alter_table_prompt(
                table_name=self.table_name,
                fields=fields,
                logic_detail=logic_detail,
                create_table_result=create_table_data
            )

            alter_table_result = await enhancement_agent.ainvoke(
                {"messages": [HumanMessage(alter_table_prompt)]},
                config
            )

            alter_table_content = alter_table_result["messages"][-1].content
            alter_table_data = extract_json_from_response(alter_table_content)

            if not alter_table_data or not alter_table_data.get("alter_statements"):
                logger.warning("步骤3失败：未能生成有效的ALTER TABLE语句")
                return {"success": False, "error": "ALTER TABLE生成失败", "step": 3}

            logger.info("步骤3完成：生成ALTER TABLE语句")

            # 应用Git diff修改到源代码
            logger.info("应用Git diff修改到源代码")
            git_diffs = git_diff_data.get("git_diffs", [])
            diff_chunks = [chunk.get("chunk", "") for chunk in git_diffs if chunk.get("chunk")]

            if diff_chunks:
                enhanced_code = apply_git_diff_to_code(source_code, diff_chunks)
            else:
                logger.warning("没有有效的Git diff chunks，使用原始代码")
                enhanced_code = source_code

            # 合并所有结果
            final_result = {
                "success": True,
                "enhanced_code": enhanced_code,
                "new_table_ddl": create_table_data.get("new_table_ddl", ""),
                "alter_statements": alter_table_data.get("alter_statements", ""),
                "optimization_summary": git_diff_data.get("analysis_summary", "分批次生成完成"),
                "field_mappings": fields,
                "generation_method": "batch_git_diff",  # 标记使用了分批次生成
                "git_diffs": git_diffs,
                "batch_details": {
                    "step1_git_diff": {
                        "total_fields_processed": git_diff_data.get("total_fields_processed", len(fields)),
                        "diff_count": len(git_diffs)
                    },
                    "step2_create_table": {
                        "new_fields_count": create_table_data.get("new_fields_count", len(fields)),
                        "ddl_validation": create_table_data.get("ddl_validation", "")
                    },
                    "step3_alter_table": {
                        "fields_in_alter": alter_table_data.get("fields_in_alter", len(fields)),
                        "alter_validation": alter_table_data.get("alter_validation", "")
                    }
                }
            }

            logger.info(f"分批次Git diff增强完成: 表={self.table_name}, 方法=batch_git_diff")
            return final_result

        except Exception as e:
            logger.error(f"Git diff策略执行失败: {e}")
            return {
                "success": False,
                "error": f"Git diff策略执行异常: {str(e)}"
            }


def create_enhancer(strategy: str, enhancement_mode: str, state: "EDWState") -> BaseEnhancer:
    """
    工厂函数：根据策略创建相应的执行器

    Args:
        strategy: 增强策略 ('traditional', 'single_git_diff')
        enhancement_mode: 增强模式 ('initial_enhancement', 'review_improvement', 'refinement')
        state: EDW状态对象

    Returns:
        相应的策略执行器实例
    """
    if strategy == "traditional":
        return TraditionalEnhancer(enhancement_mode, state)
    elif strategy == "single_git_diff":
        return GitDiffEnhancer(enhancement_mode, state)
    else:
        raise ValueError(f"Unknown enhancement strategy: {strategy}")


# ===== 策略执行器类结束 =====
