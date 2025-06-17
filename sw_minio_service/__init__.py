import enum
import logging

from rich.logging import RichHandler
from sw_onto_generation import ENUM_CLASSES

__version__ = "0.1.0"

# Create a unified DocumentTypeEnum for backward compatibility
possible_document_types = [f"{member}" for enum_class in ENUM_CLASSES for member in enum_class._member_names_]
possible_document_types.append("UNKNOWN")
DocumentTypeEnum = enum.StrEnum("DocumentTypeEnum", possible_document_types)


def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    """Configure Rich logging for the sw_minio_service module.

    Args:
        level: The logging level (default: logging.INFO)
        log_file: Optional path to a log file

    Example:
        from sw_minio_service import setup_logging
        setup_logging(level=logging.DEBUG, log_file="minio_service.log")
    """
    # Configure root logger with WARNING level to suppress third-party logs
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)

    # Configure our specific logger with Rich handler
    logger = logging.getLogger("sw_minio_service")
    logger.setLevel(level)
    logger.propagate = False  # Don't propagate to root logger

    # Create handlers
    rich_handler = RichHandler(rich_tracebacks=True, markup=True)
    rich_handler.setLevel(level)
    logger.addHandler(rich_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_format)
        file_handler.setLevel(level)
        logger.addHandler(file_handler)

    return logger
