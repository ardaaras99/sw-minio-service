from enum import StrEnum

from pydantic import BaseModel, Field

from sw_minio_service import DocumentTypeEnum


class DocumentTypeResponse(BaseModel):
    document_type: DocumentTypeEnum
    score: int = Field(description="describes how confident the model is about the document type", ge=0, le=100)
    rationale: str = Field(description="sence bu döküman neden senin seçtiğin türe ait")


class PDFLoaderEnum(StrEnum):
    PYPDF = "pypdf"
    UNSTRUCTURED = "unstructured"


class LLMOptions(StrEnum):
    GPT_4O_MINI = "gpt-4o-mini"
    GPT_4O = "gpt-4o"
    GPT_4O_2024_08_06 = "gpt-4o-2024-08-06"
    GPT_4O_2024_05_16 = "gpt-4o-2024-05-16"


class AgentConfig(BaseModel):
    response_model: type[BaseModel] = Field(default=DocumentTypeResponse)
    model: LLMOptions = Field(default=LLMOptions.GPT_4O_MINI)
    markdown: bool = Field(default=True)
    debug_mode: bool = Field(default=False)
    description: str = Field(
        default="Sen bir classification uzmanısın, sana bir dökümanın alabileceği farklı türleri veriyorum, lütfen bunlar arasından en uygun olanı seç ve score ver, Cevaplarının türkçe olması gerekiyor."
    )
    instructions: str = Field(
        default="Sen bir classification uzmanısın, sana bir dökümanın alabileceği farklı türleri veriyorum, lütfen bunlar arasından en uygun olanı seç ve score ver, Cevaplarının türkçe olması gerekiyor.",
    )


class PDFLoaderConfig(BaseModel):
    model: PDFLoaderEnum = Field(default=PDFLoaderEnum.UNSTRUCTURED)
    mode: str = Field(default="single")


class AiServiceConfig(BaseModel):
    pdf_loader: PDFLoaderConfig = Field(default=PDFLoaderConfig())
    agent_config: AgentConfig = Field(default=AgentConfig())


class MinioConfig(BaseModel):
    endpoint: str = Field(default="localhost:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")
    bucket_name: str = Field(default="test")
    secure: bool = Field(default=False)


class EngineConfig(BaseModel):
    minio_config: MinioConfig
    ai_service_config: AiServiceConfig
