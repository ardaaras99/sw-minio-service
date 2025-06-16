import hashlib
import io
import json
from typing import Literal

from minio import Minio
from pydantic import BaseModel
from rich import print as rprint

from sw_minio_service.ai_service import AiService, AiServiceConfig


class MinioConfig(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str
    secure: bool = False
    ai_flag: bool = False
    loader: Literal["pypdf", "unstructured"] = "unstructured"


class MinioEngine:
    def __init__(self, config: MinioConfig, clear_flag: bool = False):
        self.config = config
        self.minio_client = Minio(endpoint=config.endpoint, access_key=config.access_key, secret_key=config.secret_key, secure=config.secure)
        if clear_flag:
            self.clear_all_buckets()
        # Ensure bucket exists
        if not self.minio_client.bucket_exists(self.config.bucket_name):
            self.minio_client.make_bucket(self.config.bucket_name)
            self.create_mappings()
        else:
            print(f"Bucket {self.config.bucket_name} already exists")

    def get_hash_filename_mappings(self) -> tuple[dict[str, str], dict[str, str]]:
        """Read the hash-to-filename and filename-to-hash mappings from MinIO."""
        try:
            response1 = self.minio_client.get_object(self.config.bucket_name, "mappings/file_hash_to_filename.json")
            response2 = self.minio_client.get_object(self.config.bucket_name, "mappings/filename_to_file_hash.json")
            hash_to_filename = json.loads(response1.read().decode("utf-8"))
            filename_to_hash = json.loads(response2.read().decode("utf-8"))
            return hash_to_filename, filename_to_hash
        except Exception as e:
            print(f"Error reading mappings: {e}")
            return {}, {}

    def upload_file(self, file_bytes: bytes, file_name: str) -> None:
        """Upload a file to MinIO and store its hash-to-filename mapping.

        Args:
            file_bytes: The bytes of the file to upload
            file_name: The name of the file

        Raises:
            ValueError: If a file with the same content hash already exists
        """
        file_hash = hashlib.sha256(file_bytes).hexdigest()

        # Check if file with same hash already exists by reading the mappings file
        hash_to_filename, _ = self.get_hash_filename_mappings()

        if file_hash in hash_to_filename:
            existing_filename = hash_to_filename[file_hash]
            raise ValueError(f"File with identical content already exists as '{existing_filename}' (hash: {file_hash})")

        bucket_dir = f"{file_hash}/{file_name}"

        # Upload the raw pdf file
        self.minio_client.put_object(
            data=io.BytesIO(file_bytes),
            bucket_name=self.config.bucket_name,
            object_name=bucket_dir,
            length=len(file_bytes),
        )

        # Upload the the extracted content from Unstructured as txt file
        # pop extension from file_name
        if self.config.ai_flag:
            file_name_without_extension = file_name.split(".")[0]

            ai_service = AiService(config=AiServiceConfig(pdf_loader=self.config.loader))

            text, text_type = ai_service.extract_text_and_type(file_bytes, file_name)

            self.minio_client.put_object(
                data=io.BytesIO(text.encode("utf-8")),  # text
                bucket_name=self.config.bucket_name,
                object_name=f"{file_hash}/{file_name_without_extension}_{text_type}_extracted.txt",
                length=len(text.encode("utf-8")),
            )

        # Store a mapping from hash to filename
        self.store_hash_to_file_mapping(file_hash, file_name)

    def store_hash_to_file_mapping(self, file_hash: str, filename: str) -> None:
        # Read the existing mappings using the helper method
        hash_to_filename, filename_to_hash = self.get_hash_filename_mappings()

        # Update the mappings
        hash_to_filename[file_hash] = filename
        filename_to_hash[filename] = file_hash

        # Store the mappings
        hash_to_filename_json = json.dumps(hash_to_filename).encode("utf-8")
        filename_to_hash_json = json.dumps(filename_to_hash).encode("utf-8")

        self.minio_client.put_object(
            data=io.BytesIO(hash_to_filename_json),
            bucket_name=self.config.bucket_name,
            object_name="mappings/file_hash_to_filename.json",
            length=len(hash_to_filename_json),
        )
        self.minio_client.put_object(
            data=io.BytesIO(filename_to_hash_json),
            bucket_name=self.config.bucket_name,
            object_name="mappings/filename_to_file_hash.json",
            length=len(filename_to_hash_json),
        )

    def download_file(self, file_identifier: str, is_hash: bool = False) -> tuple[bytes, str]:
        """Download a file from MinIO using either its hash or filename.

        Args:
            file_identifier: Either the file hash or filename
            is_hash: If True, file_identifier is a hash; otherwise, it's a filename

        Returns:
            A tuple of (file_bytes, filename)

        Raises:
            ValueError: If the file doesn't exist
        """
        if is_hash:
            file_hash = file_identifier
            filename = self.find_filename_from_hash(file_hash)
            if not filename:
                raise ValueError(f"No file found with hash {file_hash}")
        else:
            filename = file_identifier
            file_hash = self.find_hash_from_filename(filename)
            if not file_hash:
                raise ValueError(f"No file found with name {filename}")

        object_name = f"{file_hash}/{filename}"

        try:
            response = self.minio_client.get_object(bucket_name=self.config.bucket_name, object_name=object_name)
            file_bytes = response.read()
            return file_bytes, filename
        except Exception as e:
            raise ValueError(f"Error downloading file: {e}") from e

    def create_mappings(self) -> None:
        """Create empty mappings files in the mappings directory."""
        empty_json = json.dumps({}).encode("utf-8")
        self.minio_client.put_object(
            data=io.BytesIO(empty_json),
            bucket_name=self.config.bucket_name,
            object_name="mappings/file_hash_to_filename.json",
            length=len(empty_json),
        )
        self.minio_client.put_object(
            data=io.BytesIO(empty_json),
            bucket_name=self.config.bucket_name,
            object_name="mappings/filename_to_file_hash.json",
            length=len(empty_json),
        )

    def clear_all_buckets(self) -> None:
        """Delete all objects from all buckets and then remove the buckets."""
        buckets = self.minio_client.list_buckets()
        for bucket in buckets:
            objects = self.minio_client.list_objects(bucket.name, recursive=True)
            for obj in objects:
                self.minio_client.remove_object(bucket.name, obj.object_name)
            self.minio_client.remove_bucket(bucket.name)
            rprint(f"Bucket {bucket.name} has been cleared and removed")
