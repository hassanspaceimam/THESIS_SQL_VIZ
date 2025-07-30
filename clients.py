# clients.py
from functools import lru_cache
from sqlalchemy import create_engine
from langchain_openai import AzureChatOpenAI
from config import get_settings

@lru_cache(maxsize=1)
def get_engine():
    s = get_settings()
    return create_engine(s.DATABASE_URL)

@lru_cache(maxsize=1)
def get_llm():
    s = get_settings()
    # One LLM client reused across modules
    return AzureChatOpenAI(
        azure_endpoint=s.AZURE_OPENAI_ENDPOINT,
        azure_deployment=s.AZURE_OPENAI_DEPLOYMENT,
        api_version=s.AZURE_OPENAI_API_VERSION,
        api_key=s.AZURE_OPENAI_API_KEY
    )

def get_knowledgebase_path() -> str:
    return get_settings().KNOWLEDGEBASE_PATH
