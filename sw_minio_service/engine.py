import hashlib
import io
import json
import logging

from minio import Minio
from rich.traceback import install as install_rich_traceback

from sw_minio_service import DocumentTypeEnum
from sw_minio_service.ai_service import AiService
from sw_minio_service.configs import EngineConfig

# Configure Rich logging and traceback
install_rich_traceback(show_locals=False)
logger = logging.getLogger("sw_minio_service")


class MinioEngine:
    def __init__(self, config: EngineConfig):
        self.minio_config = config.minio_config
        self.ai_service_config = config.ai_service_config
        self.minio_client = Minio(endpoint=self.minio_config.endpoint, access_key=self.minio_config.access_key, secret_key=self.minio_config.secret_key, secure=self.minio_config.secure)
        self.ai_service = AiService(config=self.ai_service_config)
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        if not self.minio_client.bucket_exists(self.minio_config.bucket_name):
            self.minio_client.make_bucket(self.minio_config.bucket_name)
            self._create_mappings()
        else:
            logger.info(f"Bucket {self.minio_config.bucket_name} already exists")

    def _create_mappings(self) -> None:
        """Create empty mappings files in the mappings directory."""
        empty_json = json.dumps({}).encode("utf-8")
        self.minio_client.put_object(
            data=io.BytesIO(empty_json),
            bucket_name=self.minio_config.bucket_name,
            object_name="mappings/file_hash_to_filename.json",
            length=len(empty_json),
        )
        self.minio_client.put_object(
            data=io.BytesIO(empty_json),
            bucket_name=self.minio_config.bucket_name,
            object_name="mappings/filename_to_file_hash.json",
            length=len(empty_json),
        )
        logger.info(f"Created empty mapping files in bucket {self.minio_config.bucket_name}")

    def _get_hash_filename_mappings(self) -> tuple[dict[str, str], dict[str, str]]:
        """Read the hash-to-filename and filename-to-hash mappings from MinIO."""
        try:
            response1 = self.minio_client.get_object(self.minio_config.bucket_name, "mappings/file_hash_to_filename.json")
            response2 = self.minio_client.get_object(self.minio_config.bucket_name, "mappings/filename_to_file_hash.json")
            hash_to_filename = json.loads(response1.read().decode("utf-8"))
            filename_to_hash = json.loads(response2.read().decode("utf-8"))
            return hash_to_filename, filename_to_hash
        except Exception as e:
            error_msg = f"Error reading mappings: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

    def _store_hash_to_file_mapping(self, file_hash: str, filename: str) -> None:
        hash_to_filename, filename_to_hash = self._get_hash_filename_mappings()

        hash_to_filename[file_hash] = filename
        filename_to_hash[filename] = file_hash

        hash_to_filename_json = json.dumps(hash_to_filename).encode("utf-8")
        filename_to_hash_json = json.dumps(filename_to_hash).encode("utf-8")

        self.minio_client.put_object(
            data=io.BytesIO(hash_to_filename_json),
            bucket_name=self.minio_config.bucket_name,
            object_name="mappings/file_hash_to_filename.json",
            length=len(hash_to_filename_json),
        )
        self.minio_client.put_object(
            data=io.BytesIO(filename_to_hash_json),
            bucket_name=self.minio_config.bucket_name,
            object_name="mappings/filename_to_file_hash.json",
            length=len(filename_to_hash_json),
        )

    def _find_hash_from_filename(self, filename: str) -> str:
        """Find the hash associated with a given filename."""
        try:
            _, filename_to_hash = self._get_hash_filename_mappings()
            return filename_to_hash.get(filename, "")
        except Exception as e:
            logger.error(f"Error finding hash from filename: {e}")
            return ""

    def upload_file(self, file_bytes: bytes, file_name: str) -> DocumentTypeEnum:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        hash_to_filename, _ = self._get_hash_filename_mappings()

        if file_hash in hash_to_filename:
            existing_filename = hash_to_filename[file_hash]
            error_msg = f"File with identical content already exists as '{existing_filename}' (hash: {file_hash})"
            logger.warning(error_msg)
            # raise ValueError(error_msg)

        # Upload the raw file
        self.minio_client.put_object(
            data=io.BytesIO(file_bytes),
            bucket_name=self.minio_config.bucket_name,
            object_name=f"{file_hash}/{file_name}",
            length=len(file_bytes),
        )

        # Extract text and determine document type
        file_name_without_extension = file_name.split(".")[0]
        text, text_type = self.ai_service.extract_text_and_type(file_bytes, file_name)

        # Upload the extracted text
        self.minio_client.put_object(
            data=io.BytesIO(text.encode("utf-8")),
            bucket_name=self.minio_config.bucket_name,
            object_name=f"{file_hash}/{file_name_without_extension}_extracted.txt",
            length=len(text.encode("utf-8")),
        )

        # Store document type metadata
        metadata = {"document_type": text_type}
        metadata_json = json.dumps(metadata).encode("utf-8")
        self.minio_client.put_object(
            data=io.BytesIO(metadata_json),
            bucket_name=self.minio_config.bucket_name,
            object_name=f"{file_hash}/{file_name_without_extension}_metadata.json",
            length=len(metadata_json),
        )

        self._store_hash_to_file_mapping(file_hash, file_name)
        logger.info(f"Successfully uploaded and processed file: {file_name} (type: {text_type})")
        return text_type

    def get_text_and_type(self, file_name: str) -> tuple[str, DocumentTypeEnum]:
        file_hash = self._find_hash_from_filename(file_name)
        if not file_hash:
            raise ValueError(f"No file found with name {file_name}")

        file_name_without_extension = file_name.split(".")[0]

        # Get document type from metadata
        document_type = DocumentTypeEnum.UNKNOWN
        try:
            metadata_path = f"{file_hash}/{file_name_without_extension}_metadata.json"
            response = self.minio_client.get_object(bucket_name=self.minio_config.bucket_name, object_name=metadata_path)
            metadata = json.loads(response.read().decode("utf-8"))
            doc_type_str = metadata.get("document_type")
            if doc_type_str:
                try:
                    document_type = DocumentTypeEnum(doc_type_str)
                except (ValueError, KeyError):
                    logger.warning(f"Invalid document type in metadata: {doc_type_str}")
            response.close()
            response.release_conn()
        except Exception as e:
            logger.warning(f"Metadata file not found or invalid: {e}")

        # Get extracted text
        try:
            text_path = f"{file_hash}/{file_name_without_extension}_extracted.txt"
            response = self.minio_client.get_object(bucket_name=self.minio_config.bucket_name, object_name=text_path)
            extracted_text = response.read().decode("utf-8")
            response.close()
            response.release_conn()
            return extracted_text, document_type
        except Exception as e:
            raise FileNotFoundError(f"No extracted text file found for {file_name}: {e}")  # noqa: B904

    def clear_all_buckets(self) -> None:
        """Delete all objects from all buckets and then remove the buckets."""
        buckets = self.minio_client.list_buckets()
        for bucket in buckets:
            objects = self.minio_client.list_objects(bucket.name, recursive=True)
            for obj in objects:
                self.minio_client.remove_object(bucket.name, obj.object_name)
            self.minio_client.remove_bucket(bucket.name)
            logger.warning(f"Bucket {bucket.name} has been cleared and removed")
