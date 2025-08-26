"""
EDW系统智能代理管理模块
统一管理所有EDW相关的智能代理，实现共享memory和会话历史
"""

import logging
import asyncio
from typing import Dict, Any, List
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
        # 创建两个不同的 checkpointer 用于不同功能组
        self.business_checkpointer = InMemorySaver()  # 业务处理相关：validation + code_enhancement
        self.interaction_checkpointer = InMemorySaver()  # 用户交互相关：chat + navigation
        self.agents = {}
        self.parser = None
        self._initialized = False
        self._async_initialized = False
        self.code_enhancement_tools = []
        
    def initialize(self):
        """初始化LLM和所有代理"""
        if self._initialized:
            return
            
        try:
            # 初始化LLM
            self.llm = LLMFactory.create_llm()
            self.reasoner = LLMFactory.create_reasoner_llm()
            logger.info("LLM初始化成功")
            
            # 创建输出解析器
            self.parser = PydanticOutputParser(pydantic_object=ModelEnhanceRequest)
            
            # 创建所有代理
            self._create_navigation_agent()
            self._create_chat_agent()
            self._create_validation_agent()
            self._create_review_agent()  # 创建review代理
            
            self._initialized = True
            logger.info("EDW智能代理管理器同步初始化完成")
            
        except Exception as e:
            logger.error(f"EDW智能代理管理器初始化失败: {e}")
            raise
    
    def _create_navigation_agent(self):
        """创建导航代理 - 负责任务分类，与聊天代理共享交互记忆"""
        self.agents['navigation'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt="你是一个专业的任务分类助手，负责理解用户意图并进行准确分类。",
            checkpointer=self.interaction_checkpointer
        )
        logger.info("导航代理创建完成（使用交互记忆）")
    
    def _create_chat_agent(self):
        """创建聊天代理 - 负责普通对话，与导航代理共享交互记忆"""
        self.agents['chat'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt="你是一个友好且博学的助手，能够与用户进行自然对话，回答各种问题，请使用markdown格式回答。",
            checkpointer=self.interaction_checkpointer
        )
        logger.info("聊天代理创建完成（使用交互记忆）")
    
    def _create_review_agent(self):
        """创建代码评审代理 - 负责理解用户需求并评估代码质量"""
        review_prompt = """你是一个专业的代码评审专家，负责：
1. 从对话历史中全面理解用户需求
2. 评估代码是否满足这些需求

分析对话历史时请注意：
- 用户可能通过多轮对话逐步明确需求
- 需求可能有修改或补充
- 提取最终确定的完整需求点
- 理解需求的业务背景和技术细节

你有访问完整对话历史的能力，可以看到用户与系统的全部交互过程。"""
        
        self.agents['review'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt=review_prompt,
            checkpointer=self.business_checkpointer  # 共享业务处理记忆
        )
        logger.info("代码评审代理创建完成（使用业务记忆）")
    
    def _create_validation_agent(self):
        """创建验证代理 - 负责分析用户的模型增强需求并提取关键信息"""
        validation_prompt = f"""你是一名数据开发专家，负责分析用户的模型增强需求并提取关键信息。

请仔细分析用户输入，提取以下信息：
1. 需要增强的表名（必须包含schema，如：dwd_fi.fi_invoice_item）
2. 具体的增强逻辑描述，请务必提取完整，比如取哪些字段，做哪些加工，新增字段位置等
3. 增强类型（添加字段、修改逻辑、优化查询等）
4. 字段信息：如果是添加字段，请提取所有字段到fields列表中
5. 代码分支名称（如：main, dev, feature/xxx）
6. JIRA工单号：用于需求跟踪和管理


特别注意：
- 如果从用户的输入中提取不到相关信息，请直接以空字符串填充!!!
- 如果用户提到"增加字段"、"新增字段"、"添加字段"等，必须将每个字段的信息提取到fields列表中
- 每个字段必须包含：
  - source_name: 源字段名称（来自底表的字段名，下划线连接的小写英文，如：invoice_doc_no）
  - physical_name: 留空字符串（这个字段会在后续标准化步骤中生成）
  - attribute_name: 属性名称，即字段的业务含义描述（首字母大写的英文描述，如：Invoice Document Number)，如果用户没有明确提供请置空
  - source_table: 字段来源的底表名称（如果用户明确指出，如："从dwd_fi.fi_invoice表取invoice_doc_no"，则填入"dwd_fi.fi_invoice"，否则留空）
- 分支名称是必需的，用于确定从哪个代码分支获取源代码

示例：
示例1 - 用户输入："给表增加invoice_doc_no（Invoice Document Number）和customer_type（Customer Type）两个字段，分支是feature/add-invoice"
应该提取：
- table_name: "表名"
- branch_name: "feature/add-invoice"
- fields: [
    {{"source_name": "invoice_doc_no", "physical_name": "", "attribute_name": "Invoice Document Number", "source_table": ""}},
    {{"source_name": "customer_type", "physical_name": "", "attribute_name": "Customer Type", "source_table": ""}}
  ]

示例2 - 用户输入："从dwd_fi.fi_invoice表取invoice_doc_no字段，从dwd_customer.customer_info表取customer_type字段添加到目标表"
应该提取：
- fields: [
    {{"source_name": "invoice_doc_no", "physical_name": "", "attribute_name": "", "source_table": "dwd_fi.fi_invoice"}},
    {{"source_name": "customer_type", "physical_name": "", "attribute_name": "", "source_table": "dwd_customer.customer_info"}}
  ]

{self.parser.get_format_instructions()}

请确保返回有效的JSON格式。如果用户输入信息不完整，请在对应字段中标注"信息不完整"。"""

        self.agents['validation'] = create_react_agent(
            model=self.llm,
            tools=[],
            prompt=validation_prompt,
            checkpointer=self.business_checkpointer
        )
        logger.info("验证代理创建完成（使用业务记忆）")
    
    async def async_initialize(self):
        """异步初始化：包含需要异步获取的代码增强智能体和功能智能体"""
        if self._async_initialized:
            return
            
        # 先确保同步部分已初始化
        if not self._initialized:
            self.initialize()
            
        try:
            # 异步创建代码增强智能体
            await self._create_code_enhancement_agent()
            
            # 异步创建功能智能体
            await self._create_function_agent()
            
            self._async_initialized = True
            logger.info("EDW智能代理管理器异步初始化完成")
            
        except Exception as e:
            logger.error(f"EDW智能代理管理器异步初始化失败: {e}")
            raise
    
    async def _create_code_enhancement_agent(self):
        """创建代码增强智能体 - 负责代码增强任务，需要异步获取MCP工具"""
        logger.info("正在初始化代码增强智能体...")
        
        try:
            # 获取MCP工具（异步操作）
            from src.mcp.mcp_client import get_mcp_tools
            from src.agent.code_enhance_agent import CodeAnalysisTool
            
            tools = []
            try:
                async with get_mcp_tools() as mcp_tools:
                    if mcp_tools:
                        tools.extend(mcp_tools)
                        logger.info(f"代码增强智能体获取到 {len(mcp_tools)} 个MCP工具")
            except Exception as e:
                logger.warning(f"代码增强智能体MCP工具获取失败: {e}")
            
            # 添加基础代码分析工具
            # tools.append(CodeAnalysisTool())
            self.code_enhancement_tools = tools
            
            # 获取系统提示词
            from src.config import get_config_manager
            config_manager = get_config_manager()
            try:
                system_prompt = config_manager.get_prompt("code_enhance_system_prompt")
            except Exception as e:
                logger.warning(f"获取代码增强提示词失败，使用默认提示词: {e}")
                system_prompt = "你是一个专业的Databricks代码增强专家，负责为数据模型添加新字段和优化代码。"
            
            # 创建代码增强智能体
            self.agents['code_enhancement'] = create_react_agent(
                model=self.llm,
                tools=tools,
                prompt=system_prompt,
                checkpointer=self.business_checkpointer
            )
            
            logger.info(f"代码增强智能体创建完成，共 {len(tools)} 个工具")
            
        except Exception as e:
            logger.error(f"代码增强智能体创建失败: {e}")
            # 创建最简单的fallback agent
            from src.agent.code_enhance_agent import CodeAnalysisTool
            fallback_tools = []
            # CodeAnalysisTool()
            self.code_enhancement_tools = fallback_tools
            
            self.agents['code_enhancement'] = create_react_agent(
                model=self.llm,
                tools=fallback_tools,
                prompt="你是一个代码增强助手。",
                checkpointer=self.business_checkpointer
            )
            logger.info("代码增强智能体fallback版本创建完成（使用业务记忆）")
    
    async def _create_function_agent(self):
        """创建功能智能体 - 负责处理各种EDW工具性任务"""
        logger.info("正在初始化功能智能体...")
        
        try:
            # 收集所有工具
            all_tools = []
            
            # 1. 获取MCP工具
            try:
                from src.mcp.mcp_client import get_mcp_tools
                async with get_mcp_tools() as mcp_tools:
                    if mcp_tools:
                        all_tools.extend(mcp_tools)
                        logger.info(f"功能智能体获取到 {len(mcp_tools)} 个MCP工具")
            except Exception as e:
                logger.warning(f"功能智能体MCP工具获取失败: {e}")
            
            # 2. 获取系统工具
            try:
                from src.graph.tools.wrappers import create_all_tools
                system_tools = create_all_tools()
                all_tools.extend(system_tools)
                logger.info(f"功能智能体获取到 {len(system_tools)} 个系统工具")
            except Exception as e:
                logger.warning(f"功能智能体系统工具获取失败: {e}")
            
            # 3. 创建功能智能体提示词
            function_prompt = """你是一个专业的EDW功能助手，拥有丰富的工具来帮助用户完成各种EDW相关任务。

你可以使用的工具包括：
1. **命名工具**：
   - suggest_attribute_names: 为物理字段名提供属性名称建议
   - batch_standardize_fields: 批量标准化字段名称
   - evaluate_attribute_name: 评估属性名称质量

2. **数据库工具**（通过MCP）：
   - 执行SQL查询
   - 查看表结构
   - 导出笔记本代码

3. **文档工具**：
   - create_confluence_doc: 创建Confluence文档
   - update_confluence_doc: 更新Confluence文档

4. **ADB工具**：
   - update_adb_notebook: 更新Azure Databricks笔记本
   - read_adb_notebook: 读取Azure Databricks笔记本

5. **邮件工具**：
   - send_review_email: 发送评审邮件
   - build_email_template: 构建邮件模板

请根据用户的需求，选择合适的工具来完成任务。
记住：
- 准确理解用户需求
- 选择最合适的工具
- 提供清晰的执行结果
- 如果任务复杂，可以组合使用多个工具"""
            
            # 创建功能智能体
            self.agents['function'] = create_react_agent(
                model=self.llm,
                tools=all_tools,
                prompt=function_prompt,
                checkpointer=self.business_checkpointer
            )
            
            logger.info(f"功能智能体创建完成，共 {len(all_tools)} 个工具")
            
        except Exception as e:
            logger.error(f"功能智能体创建失败: {e}")
            # 创建fallback版本
            self.agents['function'] = create_react_agent(
                model=self.llm,
                tools=[],
                prompt="你是一个EDW功能助手，但目前工具加载失败，只能提供建议。",
                checkpointer=self.business_checkpointer
            )
            logger.info("功能智能体fallback版本创建完成")
    
    def get_agent(self, agent_name: str):
        """获取指定的代理"""
        if not self._initialized:
            self.initialize()
        
        # 如果请求代码增强智能体或功能智能体但未异步初始化，抛出提示错误
        if agent_name in ['code_enhancement', 'function'] and not self._async_initialized:
            raise ValueError(f"{agent_name}智能体需要异步初始化。请先调用 async_initialize() 方法。")
        
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
    
    def get_checkpointer(self, checkpointer_type: str = "business"):
        """获取指定类型的checkpointer
        
        Args:
            checkpointer_type: "business" 为业务处理记忆, "interaction" 为用户交互记忆
        """
        if not self._initialized:
            self.initialize()
        
        if checkpointer_type == "business":
            return self.business_checkpointer
        elif checkpointer_type == "interaction":
            return self.interaction_checkpointer
        else:
            raise ValueError(f"不支持的checkpointer类型: {checkpointer_type}. 支持: 'business', 'interaction'")
    
    def clear_memory(self, thread_id: str = None, memory_type: str = "all"):
        """清除指定线程或全部内存
        
        Args:
            thread_id: 特定线程ID，如果为None则清除所有
            memory_type: "all", "business", "interaction" 指定清除哪种类型的记忆
        """
        if thread_id:
            # 清除特定线程的内存
            try:
                # InMemorySaver没有直接的清除方法，需要重新创建
                logger.info(f"清除线程 {thread_id} 的会话记录")
            except Exception as e:
                logger.error(f"清除内存失败: {e}")
        else:
            # 重新创建checkpointer来清除所有内存
            if memory_type in ["all", "business"]:
                self.business_checkpointer = InMemorySaver()
                logger.info("业务处理记忆已清除")
            
            if memory_type in ["all", "interaction"]:
                self.interaction_checkpointer = InMemorySaver()
                logger.info("用户交互记忆已清除")
            
            # 重新初始化所有代理以使用新的checkpointer
            if self._initialized:
                if memory_type in ["all", "interaction"]:
                    self._create_navigation_agent()
                    self._create_chat_agent()
                
                if memory_type in ["all", "business"]:
                    self._create_validation_agent()
                    # 如果代码增强智能体已初始化，也需要重新创建
                    if self._async_initialized:
                        # 异步重新创建代码增强智能体（这里需要在异步上下文中调用）
                        logger.warning("代码增强智能体需要在异步上下文中重新创建")
            
            logger.info(f"会话记录已清除 (类型: {memory_type})")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """获取内存使用统计"""
        try:
            # 这里可以添加内存统计逻辑
            return {
                "initialized": self._initialized,
                "async_initialized": self._async_initialized,
                "agents_count": len(self.agents),
                "available_agents": list(self.agents.keys()),
                "memory_groups": {
                    "business": ["validation", "code_enhancement"],
                    "interaction": ["navigation", "chat"]
                },
                "checkpointers": {
                    "business_checkpointer": "business_checkpointer" if hasattr(self, 'business_checkpointer') else None,
                    "interaction_checkpointer": "interaction_checkpointer" if hasattr(self, 'interaction_checkpointer') else None
                }
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

def get_review_agent():
    """获取代码评审代理"""
    return get_agent_manager().get_agent('review')

def get_code_enhancement_agent():
    """获取代码增强智能体"""
    return get_agent_manager().get_agent('code_enhancement')

def get_code_enhancement_tools():
    """获取代码增强工具列表"""
    manager = get_agent_manager()
    if not manager._async_initialized:
        raise ValueError("代码增强智能体尚未异步初始化。请先调用 async_initialize() 方法。")
    return manager.code_enhancement_tools

def get_shared_llm():
    """获取共享LLM实例"""
    return get_agent_manager().get_llm()

def get_shared_parser():
    """获取共享解析器"""
    return get_agent_manager().get_parser()

def get_shared_checkpointer(checkpointer_type: str = "business"):
    """获取共享checkpointer（向后兼容）
    
    Args:
        checkpointer_type: "business" 或 "interaction"，默认为业务处理记忆
    """
    return get_agent_manager().get_checkpointer(checkpointer_type)

def get_business_checkpointer():
    """获取业务处理checkpointer（validation + code_enhancement）"""
    return get_agent_manager().get_checkpointer("business")

def get_interaction_checkpointer():
    """获取用户交互checkpointer（navigation + chat）"""
    return get_agent_manager().get_checkpointer("interaction")

async def async_initialize_agents():
    """异步初始化所有智能体（包括代码增强智能体）"""
    await get_agent_manager().async_initialize()

def create_intent_analysis_agent():
    """
    创建专门用于意图分析的无记忆agent
    
    该agent专门用于分析用户在代码微调阶段的意图，
    不使用checkpointer以避免记忆干扰
    """
    from langgraph.prebuilt import create_react_agent
    
    # 专门的意图分析提示词
    intent_system_prompt = """你是一个专业的用户意图分析专家。

你的职责是深度理解用户对代码增强结果的真实想法和需求，准确识别用户的意图类型。

分析时请考虑：
1. 用户的真实情感倾向和实际需求
2. 语境和上下文，不要只看字面意思
3. 对于模糊或间接的表达，要推断其深层含义
4. 如果用户表达含糊，倾向于理解为需要进一步沟通

请严格按照要求的JSON格式输出分析结果。"""
    
    return create_react_agent(
        model=get_shared_llm(),
        tools=[],  # 意图分析不需要工具
        prompt=intent_system_prompt,
        checkpointer=None  # 无记忆，避免干扰
    )