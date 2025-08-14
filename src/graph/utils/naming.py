"""
命名转换工具模块
实现属性名称到物理字段名称的标准化转换
"""

import re
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# 全局缓存命名标准数据
_naming_std_cache = None
_naming_std_cache_time = 0
_naming_std_cache_ttl = 3600  # 缓存1小时


def multiple_replace(text: str, idict: Dict[str, str]) -> str:
    """
    批量替换文本中的内容
    
    Args:
        text: 输入文本
        idict: 替换字典 {原文本: 替换文本}
    
    Returns:
        替换后的文本
    """
    # 移除特殊字符
    com = re.compile(r'[?.,()=+!@-]')
    text2 = re.sub(com, '', text)
    
    # 构建正则表达式进行批量替换
    if not idict:
        return text2
        
    rx = re.compile('|'.join(map(re.escape, idict)))
    
    def one_xlat(match):
        return idict[match.group(0)]
    
    return rx.sub(one_xlat, text2)


def capital_to_lower(dict_info: Dict[str, str]) -> Dict[str, str]:
    """
    将字典的键转换为小写
    
    Args:
        dict_info: 原始字典
    
    Returns:
        键为小写的新字典
    """
    new_dict = {}
    for i, j in dict_info.items():
        new_dict[i.lower()] = j
    return new_dict


def blank_to_downline(text: str) -> str:
    """
    将空格替换为下划线
    
    Args:
        text: 输入文本
    
    Returns:
        替换后的文本
    """
    com = re.compile(r'\s')
    return re.sub(com, '_', text)


async def get_naming_standards() -> Dict[str, str]:
    """
    获取命名标准映射表（带缓存）
    
    Returns:
        {logical_name: physical_name} 映射字典
    """
    global _naming_std_cache, _naming_std_cache_time
    
    current_time = time.time()
    
    # 检查缓存是否有效
    if _naming_std_cache is not None and (current_time - _naming_std_cache_time) < _naming_std_cache_ttl:
        logger.debug(f"从缓存获取命名标准数据，包含 {len(_naming_std_cache)} 条记录")
        return _naming_std_cache
    
    # 从数据库查询
    try:
        from src.mcp.mcp_client import execute_sql_via_mcp
        
        query = "SELECT logical, physical FROM common.model_naming_std_abbr_list ORDER BY length(logical) DESC"
        logger.info("正在查询EDW命名标准表...")
        
        result = await execute_sql_via_mcp(query, "full")
        
        if result and "错误" not in result.lower():
            # 解析查询结果
            idict = {}
            lines = result.split('\n')
            
            # 跳过标题行，解析数据
            for line in lines[1:]:
                if line.strip():
                    # 支持多种分隔符
                    if ',' in line:
                        parts = line.split(',')
                    elif '\t' in line:
                        parts = line.split('\t')
                    else:
                        parts = line.split()
                    
                    if len(parts) >= 2:
                        logical = parts[0].strip()
                        physical = parts[1].strip()
                        if logical and physical:
                            idict[logical] = physical
            
            logger.info(f"成功加载 {len(idict)} 条命名标准映射")
            
            # 更新缓存
            if idict:
                _naming_std_cache = idict
                _naming_std_cache_time = current_time
                logger.debug("命名标准数据已缓存")
            
            return idict
        else:
            logger.error(f"查询命名标准表失败: {result}")
            return {}
            
    except Exception as e:
        logger.error(f"获取命名标准时发生异常: {e}")
        return {}


async def attribute_name_translation(text: str) -> str:
    """
    将属性名称转换为标准的物理字段名称
    
    Args:
        text: 属性名称（如：Invoice Number）
    
    Returns:
        物理字段名称（如：invoice_no）
    """
    if not text:
        return ""
    
    # 获取命名标准映射
    idict = await get_naming_standards()
    
    # 如果没有映射数据，使用基础转换
    if not idict:
        logger.warning("未获取到命名标准映射，使用基础转换规则")
        # 基础转换：移除特殊字符，空格转下划线，转小写
        text_clean = re.sub(r'[?.,()=+!@-]', '', text)
        return blank_to_downline(text_clean).lower()
    
    # 执行标准转换
    # 1. 转小写并应用映射替换
    idict_lower = capital_to_lower(idict)
    translation1 = multiple_replace(text.lower(), idict_lower).lower()
    
    # 2. 空格转下划线
    translation2 = blank_to_downline(translation1)
    
    logger.debug(f"属性名称转换: '{text}' -> '{translation2}'")
    
    return translation2


def compare_field_names(user_input: str, system_generated: str) -> Dict[str, any]:
    """
    比较用户输入的物理名称与系统生成的标准名称
    
    Args:
        user_input: 用户输入的物理名称
        system_generated: 系统生成的标准名称
    
    Returns:
        比较结果字典
    """
    # 规范化比较（忽略大小写和前后空格）
    user_normalized = user_input.strip().lower() if user_input else ""
    system_normalized = system_generated.strip().lower() if system_generated else ""
    
    is_match = user_normalized == system_normalized
    
    result = {
        "user_input": user_input,
        "system_generated": system_generated,
        "is_match": is_match,
        "similarity": 0.0
    }
    
    # 计算相似度
    if user_normalized and system_normalized:
        # 简单的相似度计算
        common = set(user_normalized) & set(system_normalized)
        union = set(user_normalized) | set(system_normalized)
        if union:
            result["similarity"] = len(common) / len(union)
    
    return result


async def standardize_field_name(attribute_name: str, user_physical_name: Optional[str] = None) -> Dict[str, any]:
    """
    标准化字段名称
    
    Args:
        attribute_name: 属性名称
        user_physical_name: 用户提供的物理名称（可选）
    
    Returns:
        包含标准化结果的字典
    """
    # 生成标准物理名称
    standard_physical_name = await attribute_name_translation(attribute_name)
    
    result = {
        "attribute_name": attribute_name,
        "standard_physical_name": standard_physical_name,
        "user_physical_name": user_physical_name,
        "use_standard": True  # 强制使用标准名称
    }
    
    # 如果用户提供了物理名称，进行比较
    if user_physical_name:
        comparison = compare_field_names(user_physical_name, standard_physical_name)
        result["comparison"] = comparison
        
        if not comparison["is_match"]:
            result["message"] = f"系统已将 '{user_physical_name}' 标准化为 '{standard_physical_name}'"
    else:
        result["message"] = f"系统自动生成物理名称: '{standard_physical_name}'"
    
    return result


async def batch_standardize_field_names(fields: list) -> list:
    """
    批量标准化字段名称 - 优化版本
    一次性处理所有字段，减少查询和处理开销
    
    Args:
        fields: 字段列表，每个字段包含 attribute_name 和 physical_name
    
    Returns:
        标准化结果列表
    """
    if not fields:
        return []
    
    # 一次性获取命名标准映射（会使用缓存）
    idict = await get_naming_standards()
    
    # 如果没有映射数据，使用基础转换
    use_basic_conversion = not idict
    if use_basic_conversion:
        logger.warning("未获取到命名标准映射，使用基础转换规则")
    
    # 批量处理所有字段
    results = []
    for field in fields:
        # 兼容字典和对象格式
        if isinstance(field, dict):
            attribute_name = field.get("attribute_name", "")
            user_physical_name = field.get("physical_name", "")
        else:
            attribute_name = getattr(field, "attribute_name", "")
            user_physical_name = getattr(field, "physical_name", "")

        # 生成标准物理名称
        if not attribute_name:
            standard_physical_name = ""
        elif use_basic_conversion:
            # 基础转换：移除特殊字符，空格转下划线，转小写
            text_clean = re.sub(r'[?.,()=+!@-]', '', attribute_name)
            standard_physical_name = blank_to_downline(text_clean).lower()
        else:
            # 标准转换
            idict_lower = capital_to_lower(idict)
            translation1 = multiple_replace(attribute_name.lower(), idict_lower).lower()
            standard_physical_name = blank_to_downline(translation1)
        
        logger.debug(f"属性名称转换: '{attribute_name}' -> '{standard_physical_name}'")
        
        # 构建结果
        result = {
            "attribute_name": attribute_name,
            "standard_physical_name": standard_physical_name,
            "user_physical_name": user_physical_name,
            "use_standard": True  # 强制使用标准名称
        }
        
        # 如果用户提供了物理名称，进行比较
        if user_physical_name:
            comparison = compare_field_names(user_physical_name, standard_physical_name)
            result["comparison"] = comparison
            
            if not comparison["is_match"]:
                result["message"] = f"系统已将 '{user_physical_name}' 标准化为 '{standard_physical_name}'"
        else:
            result["message"] = f"系统自动生成物理名称: '{standard_physical_name}'"
        
        results.append(result)
    
    logger.info(f"批量标准化完成，处理了 {len(results)} 个字段")
    return results