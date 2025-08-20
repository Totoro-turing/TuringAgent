from langgraph.prebuilt import create_react_agent
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
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
            base_url=os.getenv('DEEPSEEK_BASE_URL'),
            max_tokens=8000
        )


# langgraph 做法
llm = LLMFactory.create_llm()