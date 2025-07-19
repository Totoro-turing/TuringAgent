from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint import InMemorySaver
import os
load_dotenv()

checkpointer = InMemorySaver()

class LLMFactory:
    @staticmethod
    def create_llm() -> ChatOpenAI:
        return ChatOpenAI(
            temperature=0,
            model="deepseek-chat",
            api_key=os.getenv('DEEPSEEK_API_KEY'),
            base_url=os.getenv('DEEPSEEK_BASE_URL')
        )

# langgraph 做法
from langgraph.prebuilt import create_react_agent
llm = LLMFactory.create_llm()

agent = create_react_agent(
    model = llm,
    tools = [],
    prompt = "你是一个友好的小帮手",
    checkpointer=checkpointer
)