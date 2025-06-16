import os
from typing import Literal

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from langchain_community.document_loaders import PyPDFLoader, UnstructuredPDFLoader
from pydantic import BaseModel, Field

from sw_minio_service import DocumentTypeEnum


class AiServiceConfig(BaseModel):
    pdf_loader: Literal["pypdf", "unstructured"] = "unstructured"


class AiService:
    def __init__(self, config: AiServiceConfig):
        self.config = config

    def extract_text_and_type(self, file_bytes: bytes, file_name: str) -> tuple[str, str]:
        temp_file_path = f"/tmp/{file_name}"  # noqa: S108

        with open(temp_file_path, "wb") as f:
            f.write(file_bytes)

        if self.config.pdf_loader == "pypdf":
            loader = PyPDFLoader(temp_file_path, mode="single")
        elif self.config.pdf_loader == "unstructured":
            loader = UnstructuredPDFLoader(temp_file_path)

        docs = loader.load()
        text = "\n".join([doc.page_content for doc in docs])

        class Trial(BaseModel):
            document_type: DocumentTypeEnum
            score: int = Field(description="describes how confident the model is about the document type", ge=0, le=100)
            rationale: str = Field(description="sence bu döküman neden senin seçtiğin türe ait")

        agent = Agent(
            model=OpenAIChat(id="gpt-4o-mini"),
            description="Sen bir classification uzmanısın, sana bir dökümanın alabileceği farklı türleri veriyorum, lütfen bunlar arasından en uygun olanı seç ve score ver, Cevaplarının türkçe olması gerekiyor.",
            instructions="Sen bir classification uzmanısın, sana bir dökümanın alabileceği farklı türleri veriyorum, lütfen bunlar arasından en uygun olanı seç ve score ver, Cevaplarının türkçe olması gerekiyor.",
            response_model=Trial,
            markdown=True,
            debug_mode=False,
        )
        result: Trial = agent.run(message=text).content
        print("result", result)

        os.remove(temp_file_path)
        if result.score < 50:
            return text, None
        else:
            return text, result.document_type.name
