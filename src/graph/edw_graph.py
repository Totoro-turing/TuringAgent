from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
import os
load_dotenv()

# langgraph 做法
from langgraph.prebuilt import create_react_agent
llm = LLMFactory.create_llm()

llm_agent = create_react_agent(
    model = llm,
    tools = [],
    prompt = "你是一个友好的小帮手",
    checkpointer=InMemorySaver()
)

chat_agent = create_react_agent(
    model = llm,
    tools = [],
    prompt = "你是一个多学多才的高材生，负责跟用户聊天",
    checkpointer=InMemorySaver()
)


from operator import add
from typing import List, TypedDict, Annotated
from langchain_core.messages import AnyMessage, HumanMessage
from langgraph.config import get_stream_writer
from langgraph.graph import StateGraph, MessagesState, START, END
from langgraph.types import interrupt, Command
# langgraph 做法
from langgraph.prebuilt import create_react_agent
from langchain.prompts import PromptTemplate


class State(TypedDict):
    messages: Annotated[List[AnyMessage], add]
    type: str
    # 前端socket 访问预留
    user_id: str

class ModelDevState(TypedDict):
    messages: Annotated[List[AnyMessage], add]
    type: str
    # 前端socket 访问预留
    user_id: str


def navigate_node(state: State):
    print(">>> navigate Node")
    writer = get_stream_writer()
    writer({"node": ">>> navigate"})
    prompt = PromptTemplate.from_template("""
    你是一个专业的导航助手，负责对用户的问题进行分类，并将任务分给其他Agent执行。
如果用户的输入与增强模型、新增模型、查询数据、优化代码、给表增加字段、新增一个表相关 则返回model 。
如果是其他的问题，返回other 。
除了这几个选项外，不要返回任何其他的内容。
以下是用户的输入:
    {input}
    """)
    config = {"configurable": {"thread_id": "1"}}
    response = llm_agent.invoke(
        {"messages": [{"role": "user", "content": prompt.format(input = state["messages"][-1])}]},
        config
    )
    re = response["messages"][-1].content
    if re == "other":
        return {"type": "other", "user_id": state.get("user_id", "")}
    # 这里可以根据state的type来决定跳转到哪个节点
    # 模拟搞到 模型增强节点
    return {"type": "model_enhance", "user_id": state.get("user_id", "")}


def chat_node(state: State):
    print(">>> chat Node")
    writer = get_stream_writer()
    writer({"node": ">>> chat"})
    config = {"configurable": {"thread_id": "1"}}
    response = chat_agent.invoke(
        {"messages": [{"role": "user", "content": state["messages"][-1]}]},
        config
    )
    print(response)
    print("state: messages", state["messages"])
    return {"messages": response["messages"]}

# 主要分配模型增强等相关工作
def edw_model_node(state: State):
    print(">>> edw_model Node")
    print(f">>> {state['messages']}")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model"})
    return {}


# 模型增强前针对数据进行校验验证
def edw_model_enhance_data_validation_node(state: ModelDevState):
    print(">>> edw_model_enhance_data_validation Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_enhance_data_validation"})
    writer({"supervisor_step": "问题已经交由其他节点处理完成"})
    return {}


# 新增模型前主要针对数据进行校验验证
def edw_model_addtion_data_validation_node(state: ModelDevState):
    print(">>> edw_model_addtion_data_validation Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_addtion_data"})
    return {}


# 主要进行模型增强等相关工作
def edw_model_enhance_node(state: ModelDevState):
    print(">>> edw_model_enhance Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_enhance"})
    return {}

# 主要进行新增模型等相关工作
def edw_model_addition_node(state: ModelDevState):
    print(">>> edw_model_addition Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_model_addition"})
    return {}


# 负责发送邮件
def edw_email_node(state: ModelDevState):
    print(">>> edw_email Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_email"})
    return {}

# 负责更新confulence page
def edw_confluence_node(state: ModelDevState):
    print(">>> edw_confluence Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_confluence"})
    return {}

def edw_adb_update_node(state: ModelDevState):
    print(">>> edw_adb_update Node")
    writer = get_stream_writer()
    writer({"node": ">>> edw_adb_update"})
    return {}

def model_routing_fun(state: ModelDevState):
    if state["type"] == "model_enhance":
        return "model_enhance_data_validation_node"
    elif state["type"] == "model_addition":
        return "model_addtion_data_validation_node"
    else:
        return END

model_dev_graph = (
StateGraph(ModelDevState)
    .add_node("model_enhance_data_validation_node", edw_model_enhance_data_validation_node)
    .add_node("model_addtion_data_validation_node", edw_model_addtion_data_validation_node)
    .add_node("model_enhance_node", edw_model_enhance_node)
    .add_node("model_addition_node", edw_model_addition_node)
    .add_node("adb_update_node", edw_adb_update_node)
    .add_node("email_node", edw_email_node)
    .add_node("confluence_node", edw_confluence_node)
    .add_conditional_edges(START, model_routing_fun, ["model_enhance_data_validation_node", "model_addtion_data_validation_node"])
    # .add_edge("model_enhance_data_validation_node", "model_enhance_node")
    # .add_edge("model_addtion_data_validation_node", "model_addition_node")
    # .add_edge("model_enhance_node", "adb_update_node")
    # .add_edge("model_addition_node", "adb_update_node")
    # .add_edge("adb_update_node", "confluence_node")
    # .add_edge("confluence_node", "email_node")
    # .add_edge("email_node", END)
)

model_dev = model_dev_graph.compile()

def routing_fun(state: State):
    if state["type"] == "model_enhance":
        return "model_node"
    return "chat_node"

# 一级导航图
guid_graph = (
StateGraph(State)
    .add_node("navigate_node", navigate_node)
    .add_node("chat_node", chat_node)
    .add_node("model_node", edw_model_node)
    .add_node("model_dev_node", model_dev)
    .add_edge(START, "navigate_node")
    .add_conditional_edges("navigate_node", routing_fun, ["chat_node", "model_node"])
    .add_edge("model_node", "model_dev_node")
    .add_edge("model_dev_node", END)
    .add_edge("chat_node", END)
)

guid = guid_graph.compile()


