from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()



class Settings(BaseSettings):
    index_name: str = os.getenv("INDEX_NAME", "default_index")
    host_url: str = os.getenv("HOST_URL", "http://localhost:8000")



def get_settings():
    return Settings()