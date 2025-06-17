import logging
import os
from pathlib import Path

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from langchain_community.document_loaders import PyPDFLoader, UnstructuredPDFLoader

from sw_minio_service import DocumentTypeEnum
from sw_minio_service.configs import AiServiceConfig, DocumentTypeResponse, PDFLoaderEnum

logger = logging.getLogger("sw_minio_service")


def get_loader(config: AiServiceConfig, file_path: Path) -> PyPDFLoader | UnstructuredPDFLoader:
    if config.pdf_loader.model == PDFLoaderEnum.PYPDF:
        logger.debug(f"Using PyPDFLoader with mode: {config.pdf_loader.mode}")
        return PyPDFLoader(file_path=file_path, mode=config.pdf_loader.mode)
    elif config.pdf_loader.model == PDFLoaderEnum.UNSTRUCTURED:
        logger.debug(f"Using UnstructuredPDFLoader with mode: {config.pdf_loader.mode}")
        return UnstructuredPDFLoader(file_path=file_path, mode=config.pdf_loader.mode)


class AiService:
    def __init__(self, config: AiServiceConfig):
        self.config = config
        logger.debug(f"Initialized AiService with PDF loader: {config.pdf_loader.model}, model: {config.agent_config.model}")

    def extract_text_and_type(self, file_bytes: bytes, file_name: str) -> tuple[str, DocumentTypeEnum]:
        logger.info(f"Extracting text and determining document type for: {file_name}")
        text = self.get_text(file_bytes, file_name)
        agent = self.create_agent()

        result: DocumentTypeResponse = agent.run(message=text).content

        if result.score < 50:
            logger.info(f"Document type determination: UNKNOWN (score: {result.score})")
            return text, DocumentTypeEnum.UNKNOWN
        else:
            logger.info(f"Document type determination: {result.document_type} (score: {result.score}), type of result: {type(result.document_type)}")
            return text, result.document_type

    def create_agent(self) -> Agent:
        return Agent(
            model=OpenAIChat(id=self.config.agent_config.model),
            description=self.config.agent_config.description,
            instructions=self.config.agent_config.instructions,
            response_model=self.config.agent_config.response_model,
            markdown=self.config.agent_config.markdown,
            debug_mode=self.config.agent_config.debug_mode,
        )

    def get_text(self, file_bytes: bytes, file_name: str) -> str:
        Path("tmp").mkdir(exist_ok=True)
        temp_file_path = Path("tmp") / file_name

        with open(temp_file_path, "wb") as f:
            f.write(file_bytes)

        loader = get_loader(self.config, temp_file_path)
        docs = loader.load()
        text = "\n".join([doc.page_content for doc in docs])

        os.remove(temp_file_path)
        return text
