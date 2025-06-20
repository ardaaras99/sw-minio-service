import io

from minio import Minio
from pydantic import BaseModel

from sw_minio_service.mapper import Mapper


class EngineConfig(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str
    secure: bool


class Engine:
    def __init__(self, config: EngineConfig):
        self.config = config
        self.minio_client = Minio(endpoint=self.config.endpoint, access_key=self.config.access_key, secret_key=self.config.secret_key, secure=self.config.secure)
        self.mapper = Mapper(minio_client=self.minio_client, bucket_name=self.config.bucket_name)
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        if not self.minio_client.bucket_exists(self.config.bucket_name):
            self.minio_client.make_bucket(self.config.bucket_name)
            self.mapper.create_empty_mappings()

    def check_if_file_exists(self, pdf_file_hash: str) -> bool:
        hash_to_filename, _ = self.mapper.get_mappings()

        if pdf_file_hash in hash_to_filename:
            existing_filename = hash_to_filename[pdf_file_hash]
            raise ValueError(f"File with identical content already exists as '{existing_filename}' (hash: {pdf_file_hash})")
        return True

    def upload_pdf_file(self, pdf_file_bytes: bytes, pdf_file_name: str, pdf_txt_format: str, pdf_file_hash: str) -> None:
        # Upload the raw file
        self.minio_client.put_object(
            data=io.BytesIO(pdf_file_bytes),
            bucket_name=self.config.bucket_name,
            object_name=f"{pdf_file_hash}/{pdf_file_name}",
            length=len(pdf_file_bytes),
        )

        # Extract text and determine document type
        file_name_without_extension = pdf_file_name.split(".")[0]

        # Upload the extracted text
        self.minio_client.put_object(
            data=io.BytesIO(pdf_txt_format.encode("utf-8")),
            bucket_name=self.config.bucket_name,
            object_name=f"{pdf_file_hash}/{file_name_without_extension}_extracted.txt",
            length=len(pdf_txt_format.encode("utf-8")),
        )

        self.mapper.add_instance_to_mappings(pdf_file_hash, pdf_file_name)

    def delete_pdf_file(self, pdf_file_name: str) -> None:
        _, filename_to_hash = self.mapper.get_mappings()
        pdf_file_hash = filename_to_hash[pdf_file_name]

        self.minio_client.remove_object(bucket_name=self.config.bucket_name, object_name=f"{pdf_file_hash}/{pdf_file_name}")
        self.minio_client.remove_object(bucket_name=self.config.bucket_name, object_name=f"{pdf_file_hash}/{pdf_file_name}_extracted.txt")

        self.mapper.delete_instance_from_mappings(pdf_file_name)

    def clear_all_buckets(self) -> None:
        """Delete all objects from all buckets and then remove the buckets."""
        buckets = self.minio_client.list_buckets()
        for bucket in buckets:
            objects = self.minio_client.list_objects(bucket.name, recursive=True)
            for obj in objects:
                self.minio_client.remove_object(bucket.name, obj.object_name)
            self.minio_client.remove_bucket(bucket.name)
