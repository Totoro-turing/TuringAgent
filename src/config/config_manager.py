import os
import json
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class MCPServerConfig:
    """MCP服务器配置 - 支持SSE传输"""
    url: str                                    # SSE服务URL
    transport: str = "sse"                     # 传输方式，固定为sse
    env: Dict[str, str] = field(default_factory=dict)
    timeout: int = 300
    retry_count: int = 3

@dataclass
class CacheConfig:
    """缓存配置"""
    ttl_seconds: int = 3600
    max_entries: int = 1000
    cleanup_interval: int = 300
    enabled: bool = True

@dataclass
class ValidationConfig:
    """验证配置"""
    similarity_threshold: float = 0.6
    max_suggestions: int = 5
    enable_pattern_matching: bool = True

@dataclass
class SystemConfig:
    """系统配置"""
    log_level: str = "INFO"
    thread_id_length: int = 16
    max_retry_attempts: int = 3
    request_timeout: int = 120

@dataclass
class MessageManagementConfig:
    """消息管理配置"""
    summary_enabled: bool = True
    summary_threshold: int = 20
    keep_recent_count: int = 5
    max_context_length: int = 10

@dataclass
class EDWConfig:
    """EDW系统完整配置"""
    mcp_servers: Dict[str, MCPServerConfig] = field(default_factory=dict)
    cache: CacheConfig = field(default_factory=CacheConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    system: SystemConfig = field(default_factory=SystemConfig)
    message_management: MessageManagementConfig = field(default_factory=MessageManagementConfig)
    prompts: Dict[str, str] = field(default_factory=dict)
    
class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: Optional[str] = None):
        self.config_dir = Path(config_dir or self._get_default_config_dir())
        self.config_file = self.config_dir / "edw_config.yaml"
        self.prompts_file = self.config_dir / "prompts.yaml"
        
        # 确保配置目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        self._config: Optional[EDWConfig] = None
        
    def _get_default_config_dir(self) -> str:
        """获取默认配置目录"""
        # 优先使用环境变量
        if env_config_dir := os.getenv("EDW_CONFIG_DIR"):
            return env_config_dir
        
        # 使用项目根目录下的config文件夹
        project_root = Path(__file__).parent.parent.parent
        return str(project_root / "config")
    
    def _create_default_config(self) -> EDWConfig:
        """创建默认配置"""
        return EDWConfig(
            mcp_servers={
                "databricks": MCPServerConfig(
                    url="http://127.0.0.1:8000",
                    transport="sse",
                    timeout=30,
                    retry_count=3,
                    env={
                        "DATABRICKS_HOST": os.getenv("DATABRICKS_HOST", ""),
                        "DATABRICKS_TOKEN": os.getenv("DATABRICKS_TOKEN", "")
                    }
                )
            },
            cache=CacheConfig(
                ttl_seconds=int(os.getenv("EDW_CACHE_TTL", "3600")),
                max_entries=int(os.getenv("EDW_CACHE_MAX_ENTRIES", "1000")),
                cleanup_interval=300,
                enabled=os.getenv("EDW_CACHE_ENABLED", "true").lower() == "true"
            ),
            validation=ValidationConfig(
                similarity_threshold=float(os.getenv("EDW_SIMILARITY_THRESHOLD", "0.6")),
                max_suggestions=int(os.getenv("EDW_MAX_SUGGESTIONS", "5")),
                enable_pattern_matching=True
            ),
            system=SystemConfig(
                log_level=os.getenv("EDW_LOG_LEVEL", "INFO"),
                thread_id_length=16,
                max_retry_attempts=3,
                request_timeout=120
            ),
            message_management=MessageManagementConfig(
                summary_enabled=os.getenv("EDW_SUMMARY_ENABLED", "true").lower() == "true",
                summary_threshold=int(os.getenv("EDW_SUMMARY_THRESHOLD", "20")),
                keep_recent_count=int(os.getenv("EDW_KEEP_RECENT_COUNT", "5")),
                max_context_length=int(os.getenv("EDW_MAX_CONTEXT_LENGTH", "10"))
            ),
            prompts=self._get_default_prompts()
        )
    
    def _get_default_prompts(self) -> Dict[str, str]:
        """获取默认提示词模板"""
        return {
            "navigation_prompt": """你是一个专业的导航助手，负责对用户的问题进行分类。
如果用户的输入与增强模型、新增模型、查询数据、优化代码、给表增加字段、新增一个表相关 则返回model 。
如果是其他的问题，返回other 。
除了这几个选项外，不要返回任何其他的内容。
以下是用户的输入:
{input}""",
            
            "model_classification_prompt": """你是一个专业的导航助手，负责对用户的问题进行分类。
如果用户的输入与增强模型、查询数据、优化代码、给表增加字段相关则返回model_enhance。
如果用户的输入与新增模型、新增表相关，返回model_add 。
除了这几个选项外，不要返回任何其他的内容。
以下是用户的输入:
{input}""",
            "validation_agent_prompt": """你是一个EDW系统需求分析专家，负责提取和验证用户的模型增强需求。

请分析用户输入，提取以下信息：
1. 目标表名（如：dwd_fi.fi_invoice_item）
2. 增强类型（add_field, modify_field, optimize_code等）
3. 具体的增强逻辑描述
4. 新增字段信息（如有）：
   - physical_name: 字段的物理名称
   - attribute_name: 字段的属性名称/业务名称
   - data_type: 字段数据类型（可选）
5. 业务需求描述

请严格按照以下JSON格式返回：
{{
  "table_name": "表名",
  "enhancement_type": "增强类型",
  "logic_detail": "详细的增强逻辑",
  "field_info": "字段相关描述",
  "business_requirement": "业务需求",
  "fields": [
    {{
      "physical_name": "字段物理名称",
      "attribute_name": "字段属性名称",
      "data_type": "数据类型"
    }}
  ]
}}"""
        }
    
    def load_config(self, force_reload: bool = False) -> EDWConfig:
        """加载配置"""
        if self._config is None or force_reload:
            try:
                if self.config_file.exists():
                    logger.info(f"从文件加载配置: {self.config_file}")
                    with open(self.config_file, 'r', encoding='utf-8') as f:
                        config_data = yaml.safe_load(f)
                    
                    # 加载提示词
                    if self.prompts_file.exists():
                        with open(self.prompts_file, 'r', encoding='utf-8') as f:
                            prompts_data = yaml.safe_load(f)
                        config_data['prompts'] = prompts_data.get('prompts', {})
                    
                    self._config = self._parse_config_data(config_data)
                else:
                    logger.info("配置文件不存在，使用默认配置")
                    self._config = self._create_default_config()
                    self.save_config()
                    
            except Exception as e:
                logger.error(f"加载配置失败: {e}，使用默认配置")
                self._config = self._create_default_config()
        
        return self._config
    
    def _parse_config_data(self, data: Dict[str, Any]) -> EDWConfig:
        """解析配置数据"""
        # 解析MCP服务器配置
        mcp_servers = {}
        for name, server_data in data.get('mcp_servers', {}).items():
            mcp_servers[name] = MCPServerConfig(**server_data)
        
        # 解析缓存配置
        cache_data = data.get('cache', {})
        cache_config = CacheConfig(**cache_data)
        
        # 解析验证配置
        validation_data = data.get('validation', {})
        validation_config = ValidationConfig(**validation_data)
        
        # 解析系统配置
        system_data = data.get('system', {})
        system_config = SystemConfig(**system_data)
        
        # 解析提示词
        prompts = data.get('prompts', {})
        
        return EDWConfig(
            mcp_servers=mcp_servers,
            cache=cache_config,
            validation=validation_config,
            system=system_config,
            prompts=prompts
        )
    
    def save_config(self):
        """保存配置到文件"""
        if self._config is None:
            return
        
        try:
            # 构建配置数据结构
            config_data = {
                'mcp_servers': {
                    name: {
                        'url': server.url,
                        'transport': server.transport,
                        'env': server.env,
                        'timeout': server.timeout,
                        'retry_count': server.retry_count
                    }
                    for name, server in self._config.mcp_servers.items()
                },
                'cache': {
                    'ttl_seconds': self._config.cache.ttl_seconds,
                    'max_entries': self._config.cache.max_entries,
                    'cleanup_interval': self._config.cache.cleanup_interval,
                    'enabled': self._config.cache.enabled
                },
                'validation': {
                    'similarity_threshold': self._config.validation.similarity_threshold,
                    'max_suggestions': self._config.validation.max_suggestions,
                    'enable_pattern_matching': self._config.validation.enable_pattern_matching
                },
                'system': {
                    'log_level': self._config.system.log_level,
                    'thread_id_length': self._config.system.thread_id_length,
                    'max_retry_attempts': self._config.system.max_retry_attempts,
                    'request_timeout': self._config.system.request_timeout
                }
            }
            
            # 保存主配置文件
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
            
            # 保存提示词文件
            prompts_data = {'prompts': self._config.prompts}
            with open(self.prompts_file, 'w', encoding='utf-8') as f:
                yaml.dump(prompts_data, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"配置已保存到: {self.config_file}")
            
        except Exception as e:
            logger.error(f"保存配置失败: {e}")
    
    def get_mcp_server_config(self, server_name: str) -> Optional[MCPServerConfig]:
        """获取MCP服务器配置"""
        config = self.load_config()
        return config.mcp_servers.get(server_name)
    
    def get_cache_config(self) -> CacheConfig:
        """获取缓存配置"""
        config = self.load_config()
        return config.cache
    
    def get_validation_config(self) -> ValidationConfig:
        """获取验证配置"""
        config = self.load_config()
        return config.validation
    
    def get_system_config(self) -> SystemConfig:
        """获取系统配置"""
        config = self.load_config()
        return config.system
    
    def get_message_config(self) -> MessageManagementConfig:
        """获取消息管理配置"""
        config = self.load_config()
        return config.message_management
    
    def get_prompt(self, prompt_name: str) -> str:
        """获取提示词模板"""
        config = self.load_config()
        return config.prompts.get(prompt_name, "")
    
    def update_config(self, **kwargs):
        """更新配置"""
        config = self.load_config()
        
        for key, value in kwargs.items():
            if hasattr(config, key):
                setattr(config, key, value)
        
        self.save_config()
    
    def reload_config(self):
        """重新加载配置"""
        self._config = None
        return self.load_config(force_reload=True)


# 全局配置管理器实例
_global_config_manager = None

def get_config_manager() -> ConfigManager:
    """获取全局配置管理器实例"""
    global _global_config_manager
    if _global_config_manager is None:
        _global_config_manager = ConfigManager()
    return _global_config_manager

def init_config_manager(config_dir: Optional[str] = None) -> ConfigManager:
    """初始化配置管理器"""
    global _global_config_manager
    _global_config_manager = ConfigManager(config_dir)
    return _global_config_manager