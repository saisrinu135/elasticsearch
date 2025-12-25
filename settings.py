from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()



class Settings(BaseSettings):
    index_name: str = os.getenv("INDEX_NAME", "")
    host_url: str = os.getenv("HOST_URL", "")
    ngram_index: str = os.getenv("NGRAM_INDEX", "")
    vector_index: str = os.getenv("VECTOR_INDEX", "")



def get_settings():
    return Settings()