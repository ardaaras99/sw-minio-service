# %%
import logging

from sw_minio_service import setup_logging
from sw_minio_service.configs import AiServiceConfig, EngineConfig, MinioConfig, PDFLoaderConfig, PDFLoaderEnum
from sw_minio_service.engine import MinioEngine

# %%


def main():
    # Setup logging
    setup_logging(level=logging.INFO, log_file="minio_service.log")
    logger = logging.getLogger("sw_minio_service")

    # Initialize MinioEngine
    logger.info("Initializing MinioEngine")
    engine = MinioEngine(
        config=EngineConfig(
            minio_config=MinioConfig(bucket_name="test-newest3"),
            ai_service_config=AiServiceConfig(pdf_loader=PDFLoaderConfig(model=PDFLoaderEnum.PYPDF)),
        )
    )

    # Upload file
    file_name = "yenal333.pdf"
    try:
        logger.info(f"Writing file to minio: {file_name}")
        with open(f"data/somewhere/{file_name}", "rb") as f:
            file_bytes = f.read()
            result = engine.upload_file(file_bytes=file_bytes, file_name=file_name)
            logger.info(f"Upload complete. Document type: {result}")
    except FileNotFoundError:
        logger.error(f"File not found: data/somewhere/{file_name}")
    except Exception as e:
        logger.error(f"Error during file upload: {e}", exc_info=True)

    # Retrieve text and document type
    try:
        logger.info(f"Getting text and document type from minio: {file_name}")
        text, document_type = engine.get_text_and_type(file_name)
        logger.info(f"Text: {text[:100]}")
        logger.info(f"Document type: {document_type}")
    except Exception as e:
        logger.error(f"Error retrieving file: {e}", exc_info=True)

    engine.clear_all_buckets()


if __name__ == "__main__":
    main()
