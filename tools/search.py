from langchain_core.tools import tool
from langchain_openai import OpenAIEmbeddings
from pinecone import Pinecone
import os
from dotenv import load_dotenv
from langchain_pinecone import PineconeVectorStore

load_dotenv()


def get_vectorstore() -> PineconeVectorStore:
    """Get the vectorstore."""
    pinecone_api_key = os.environ.get("PINECONE_API_KEY")
    pc = Pinecone(api_key=pinecone_api_key)
    index_name = "aces"

    embedding = OpenAIEmbeddings(
        api_key=os.environ.get("OPENAI_API_KEY"),
        model="text-embedding-3-large",
    )

    print("Connecting to Pinecone vectorstore...")
    return PineconeVectorStore(index_name=index_name, embedding=embedding)


vectorstore = get_vectorstore()
