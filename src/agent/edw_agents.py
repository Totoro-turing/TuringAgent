"""
EDW系统智能代理管理模块
统一管理所有EDW相关的智能代理，实现共享memory和会话历史
"""

import logging
from typing import Dict, Any
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import InMemorySaver
from langchain.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
from src.agent.mix_agent import LLMFactory
from src.models.edw_models import ModelEnhanceRequest

logger = logging.getLogger(__name__)

class EDWAgentManager:
    """EDW智能代理管理器，统一管理所有代理和共享memory"""
    
    def __init__(self):
        self.llm = None
        self.shared_checkpointer = InMemorySaver()
        self.agents = {}
        self.parser = None
        self._initialized = False
        
    def initialize(self):
        """初始化LLM和所有代理"""
        if self._initialized:
            return
            
        try:
            # 初始化LLM
            self.llm = LLMFactory.create_llm()
            logger.info("LLM初始化成功")
            
            # 创建输出解析器
            self.parser = PydanticOutputParser(pydantic_object=ModelEnhanceRequest)
            
            # 创建所有代理
            self._create_navigation_agent()
            self._create_chat_agent()
            self._create_validation_agent()
            
            self._initialized = True
            logger.info("EDW智能代理管理器初始化完成")
            
        except Exception as e:
            logger.error(f"EDW智能代理管理器初始化失败: {e}")
            raise
    
    def _create_navigation_agent(self):
        """创建导航代理 - 负责任务分类"""
        self.agents['navigation'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt="你是一个专业的任务分类助手，负责理解用户意图并进行准确分类。",
            checkpointer=self.shared_checkpointer
        )
        logger.info("导航代理创建完成")
    
    def _create_chat_agent(self):
        """创建聊天代理 - 负责普通对话"""
        self.agents['chat'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt="你是一个友好且博学的助手，能够与用户进行自然对话，回答各种问题，请使用markdown格式回答。",
            checkpointer=self.shared_checkpointer
        )
        logger.info("聊天代理创建完成")
    
    def _create_validation_agent(self):
        """创建验证代理 - 负责分析用户的模型增强需求并提取关键信息"""
        validation_prompt = f"""你是一名数据开发专家，负责分析用户的模型增强需求并提取关键信息。

请仔细分析用户输入，提取以下信息：
1. 需要增强的表名（必须包含schema，如：dwd_fi.fi_invoice_item）
2. 具体的增强逻辑描述
3. 增强类型（添加字段、修改逻辑、优化查询等）
4. 字段信息：如果是添加字段，请提取所有字段到fields列表中

特别注意：
- 如果用户提到"增加字段"、"新增字段"、"添加字段"等，必须将每个字段的信息提取到fields列表中
- 每个字段必须包含：
  - physical_name: 物理名称（下划线连接的小写英文，如：invoice_doc_no）
  - attribute_name: 属性名称（首字母大写的英文描述，如：Invoice Document Number）
  - data_type: 数据类型（如未指定，默认为STRING）
  - is_nullable: 是否可空（如未指定，默认为true）
  - comment: 备注信息

示例：
用户输入："给表增加invoice_doc_no（Invoice Document Number）和customer_type（Customer Type）两个字段"
应该提取：
- fields: [
    {{"physical_name": "invoice_doc_no", "attribute_name": "Invoice Document Number", "data_type": "STRING", "is_nullable": true, "comment": ""}},
    {{"physical_name": "customer_type", "attribute_name": "Customer Type", "data_type": "STRING", "is_nullable": true, "comment": ""}}
  ]

{self.parser.get_format_instructions()}

请确保返回有效的JSON格式。如果用户输入信息不完整，请在对应字段中标注"信息不完整"。"""

        self.agents['validation'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt=validation_prompt,
            checkpointer=self.shared_checkpointer
        )
        logger.info("验证代理创建完成")
    
    def get_agent(self, agent_name: str):
        """获取指定的代理"""
        if not self._initialized:
            self.initialize()
        
        if agent_name not in self.agents:
            raise ValueError(f"代理 '{agent_name}' 不存在。可用代理: {list(self.agents.keys())}")
        
        return self.agents[agent_name]
    
    def get_llm(self):
        """获取LLM实例"""
        if not self._initialized:
            self.initialize()
        return self.llm
    
    def get_parser(self):
        """获取解析器"""
        if not self._initialized:
            self.initialize()
        return self.parser
    
    def get_checkpointer(self):
        """获取共享的checkpointer"""
        if not self._initialized:
            self.initialize()
        return self.shared_checkpointer
    
    def clear_memory(self, thread_id: str = None):
        """清除指定线程或全部内存"""
        if thread_id:
            # 清除特定线程的内存
            try:
                # InMemorySaver没有直接的清除方法，需要重新创建
                logger.info(f"清除线程 {thread_id} 的会话记录")
            except Exception as e:
                logger.error(f"清除内存失败: {e}")
        else:
            # 重新创建checkpointer来清除所有内存
            self.shared_checkpointer = InMemorySaver()
            # 重新初始化所有代理以使用新的checkpointer
            if self._initialized:
                self._create_navigation_agent()
                self._create_chat_agent()
                self._create_validation_agent()
            logger.info("所有会话记录已清除")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存使用统计"""
        try:
            # 这里可以添加内存统计逻辑
            return {
                "initialized": self._initialized,
                "agents_count": len(self.agents),
                "available_agents": list(self.agents.keys())
            }
        except Exception as e:
            logger.error(f"获取内存统计失败: {e}")
            return {}

# 全局单例实例
_agent_manager = None

def get_agent_manager() -> EDWAgentManager:
    """获取全局智能代理管理器单例"""
    global _agent_manager
    if _agent_manager is None:
        _agent_manager = EDWAgentManager()
    return _agent_manager

def get_navigation_agent():
    """获取导航代理"""
    return get_agent_manager().get_agent('navigation')

def get_chat_agent():
    """获取聊天代理"""
    return get_agent_manager().get_agent('chat')

def get_validation_agent():
    """获取验证代理"""
    return get_agent_manager().get_agent('validation')

def get_shared_llm():
    """获取共享LLM实例"""
    return get_agent_manager().get_llm()

def get_shared_parser():
    """获取共享解析器"""
    return get_agent_manager().get_parser()

def get_shared_checkpointer():
    """获取共享checkpointer"""
    return get_agent_manager().get_checkpointer()