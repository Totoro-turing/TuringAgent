"""
字段处理工具函数
"""

import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def validate_english_model_name(name: str) -> tuple[bool, str]:
    """验证英文模型名称格式"""
    if not name or not name.strip():
        return False, "模型名称不能为空"
    
    name = name.strip()
    
    # 检查是否包含中文字符
    if any('\u4e00' <= char <= '\u9fff' for char in name):
        return False, f"模型名称不能包含中文字符，当前值: '{name}'"
    
    # 检查是否符合标准格式（首字母大写，单词间空格分隔）
    words = name.split()
    if not words:
        return False, "模型名称不能为空"
    
    for word in words:
        if not word[0].isupper() or not word.isalpha():
            return False, f"模型名称应采用标准格式（如：Finance Invoice Header），当前值: '{name}'"
    
    return True, ""


async def get_table_fields_info(table_name: str) -> dict:
    """查询表的字段信息（带智能缓存）"""
    try:
        from src.cache import get_cache_manager
        cache_manager = get_cache_manager()
        
        if cache_manager:
            # 使用缓存管理器获取表字段信息
            result = await cache_manager.get_table_fields(table_name, _fetch_table_fields_from_db)
            
            # 添加缓存命中统计到日志
            stats = cache_manager.get_stats()
            logger.debug(f"表字段查询完成: {table_name} | 缓存统计 - 命中率: {stats['hit_rate']}, 总请求: {stats['total_requests']}")
        else:
            # 直接查询（无缓存）
            result = await _fetch_table_fields_from_db(table_name)
        
        return result
        
    except Exception as e:
        logger.error(f"查询表字段失败 {table_name}: {e}")
        return {"status": "error", "message": str(e)}


async def _fetch_table_fields_from_db(table_name: str) -> dict:
    """从数据库直接获取表字段信息（不使用缓存的原始函数）"""
    try:
        from src.mcp.mcp_client import execute_sql_via_mcp
        
        # 查询表结构
        desc_query = f"DESCRIBE {table_name}"
        result = await execute_sql_via_mcp(desc_query)
        logger.info(f"调用mcp 工具 exec sql result: {result}")
        if result and "错误" not in result.lower():
            # 解析字段信息
            fields = []
            lines = result.split('\n')
            for line in lines[1:]:  # 跳过标题行
                if line.strip():
                    # 优先支持CSV格式（逗号分隔），然后是制表符，最后是空格
                    if ',' in line:
                        parts = line.split(',')
                    elif '\t' in line:
                        parts = line.split('\t')
                    else:
                        parts = line.split()
                    
                    if len(parts) >= 2:
                        field_name = parts[0].strip()
                        field_type = parts[1].strip()
                        fields.append({
                            "name": field_name,
                            "type": field_type
                        })
            
            return {"status": "success", "fields": fields}
        else:
            return {"status": "error", "message": result or "查询无返回结果"}
        
    except Exception as e:
        logger.error(f"查询表字段失败 {table_name}: {e}")
        return {"status": "error", "message": str(e)}


def find_similar_fields(input_field: str, available_fields: list, threshold: Optional[float] = None) -> list:
    """查找相似的字段名"""
    from src.config import get_config_manager
    config_manager = get_config_manager()
    
    if threshold is None:
        validation_config = config_manager.get_validation_config()
        threshold = validation_config.similarity_threshold
    
    similar_fields = []
    
    for field in available_fields:
        # 计算字符串相似度
        similarity = SequenceMatcher(None, input_field.lower(), field.lower()).ratio()
        if similarity >= threshold:
            similar_fields.append({
                "field_name": field,
                "similarity": similarity
            })
    
    # 按相似度排序
    similar_fields.sort(key=lambda x: x["similarity"], reverse=True)
    
    validation_config = config_manager.get_validation_config()
    max_suggestions = validation_config.max_suggestions
    return similar_fields[:max_suggestions]


async def validate_fields_against_base_tables(fields: list, base_tables: list, source_code: str) -> dict:
    """验证新增字段是否基于底表中的现有字段"""
    from src.cache import get_cache_manager
    
    validation_result = {
        "valid": True,
        "invalid_fields": [],
        "suggestions": {},
        "base_tables_info": {}
    }
    
    # 获取所有底表的字段信息
    all_base_fields = []
    
    # 记录开始时间和缓存状态
    start_time = datetime.now()
    cache_manager = get_cache_manager()
    initial_stats = cache_manager.get_stats() if cache_manager else {}
    
    for table_name in base_tables:
        logger.info(f"正在查询底表字段信息: {table_name}")
        table_info = await get_table_fields_info(table_name)
        logger.info(f"mcp返回信息: {table_info}")
        if table_info["status"] == "success":
            table_fields = [field["name"] for field in table_info["fields"]]
            all_base_fields.extend(table_fields)
            validation_result["base_tables_info"][table_name] = table_fields
            logger.info(f"底表 {table_name} 包含字段: {table_fields}")
        else:
            logger.warning(f"无法获取底表 {table_name} 的字段信息: {table_info['message']}")
            validation_result["base_tables_info"][table_name] = []
    
    # 记录结束时间和缓存统计
    end_time = datetime.now()
    if cache_manager:
        final_stats = cache_manager.get_stats()
        
        # 计算本次验证的缓存效果
        cache_hits_delta = final_stats['cache_hits'] - initial_stats.get('cache_hits', 0)
        cache_requests_delta = final_stats['total_requests'] - initial_stats.get('total_requests', 0)
    else:
        cache_hits_delta = 0
        cache_requests_delta = 0
    
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"底表查询完成 - 耗时: {duration:.2f}秒, 查询了{len(base_tables)}个表, 缓存命中: {cache_hits_delta}/{cache_requests_delta}")
    validation_result["cache_performance"] = {
        "duration_seconds": round(duration, 2),
        "tables_queried": len(base_tables),
        "cache_hits": cache_hits_delta,
        "cache_requests": cache_requests_delta,
        "overall_hit_rate": cache_manager.get_stats()['hit_rate'] if cache_manager else 0
    }
    
    if not all_base_fields:
        # 检查是否是因为服务问题导致的失败
        failed_tables = []
        for table_name, fields_list in validation_result["base_tables_info"].items():
            if not fields_list:  # 空列表表示查询失败
                failed_tables.append(table_name)
        
        if failed_tables:
            # 如果有表查询失败，返回服务错误
            error_msg = f"无法获取底表字段信息，MCP服务可能存在问题。失败的表：{', '.join(failed_tables)}\n\n请检查数据服务状态，稍后再试。"
            logger.error(f"MCP服务问题导致字段验证失败: {failed_tables}")
            return {
                "valid": False,
                "service_error": True,
                "error_message": error_msg,
                "failed_tables": failed_tables,
                "base_tables_info": validation_result["base_tables_info"],
                "cache_performance": validation_result["cache_performance"]
            }
        else:
            # 如果没有底表需要验证，返回成功
            logger.info("没有底表需要验证字段关联性")
            return validation_result
    
    logger.info(f"所有底表字段: {all_base_fields}")
    
    # 检查每个新增字段
    for field in fields:
        # 兼容字典和对象访问
        if isinstance(field, dict):
            physical_name = field.get("physical_name", "")
        else:
            physical_name = getattr(field, "physical_name", "")
        
        # 检查是否在底表中存在相似字段
        similar_fields = find_similar_fields(physical_name, all_base_fields)
        
        if not similar_fields:
            validation_result["valid"] = False
            validation_result["invalid_fields"].append(physical_name)
            # 提供基于字段名称模式的建议
            pattern_suggestions = _generate_pattern_suggestions(physical_name, all_base_fields)
            if pattern_suggestions:
                validation_result["suggestions"][physical_name] = pattern_suggestions
            logger.warning(f"字段 {physical_name} 在底表中未找到相似字段")
        else:
            # 如果相似度不够高，也提供建议
            if similar_fields[0]["similarity"] < 0.8:
                validation_result["suggestions"][physical_name] = similar_fields
                logger.info(f"字段 {physical_name} 找到相似字段: {[f['field_name'] for f in similar_fields[:3]]}")
    
    return validation_result


def _generate_pattern_suggestions(field_name: str, available_fields: list) -> list:
    """基于字段名称模式生成建议"""
    suggestions = []
    field_parts = field_name.lower().split('_')
    
    for available_field in available_fields:
        available_parts = available_field.lower().split('_')
        
        # 检查是否有共同的词汇
        common_parts = set(field_parts) & set(available_parts)
        if common_parts:
            suggestions.append({
                "field_name": available_field,
                "reason": f"包含相同词汇: {', '.join(common_parts)}"
            })
    
    return suggestions[:3]