"""
模型增强节点
临时从edw_graph导入，后续需要重构
"""

# TODO: 将edw_graph.py中的模型增强相关代码移到这里
# 现在临时从原文件导入
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

# 临时解决方案：直接从edw_graph导入
# 注意：这会导致循环导入问题，需要后续重构
def edw_model_enhance_node(state):
    """模型增强节点 - 临时占位"""
    from src.graph.edw_graph import edw_model_enhance_node as original_node
    return original_node(state)

__all__ = ['edw_model_enhance_node']