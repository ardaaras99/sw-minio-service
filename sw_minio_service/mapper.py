import io
import json

from minio import Minio


class Mapper:
    def __init__(self, minio_client: Minio, bucket_name: str):
        self.minio_client = minio_client
        self.bucket_name = bucket_name
        self.hash_to_filename_path = "mappings/hash_to_filename.json"
        self.filename_to_hash_path = "mappings/filename_to_hash.json"

    def create_empty_mappings(self) -> None:
        self.put_mappings({}, {})

    def get_mappings(self) -> tuple[dict[str, str], dict[str, str]]:
        res1 = self.minio_client.get_object(bucket_name=self.bucket_name, object_name=self.hash_to_filename_path)
        res2 = self.minio_client.get_object(bucket_name=self.bucket_name, object_name=self.filename_to_hash_path)
        hash_to_filename = json.loads(res1.read().decode("utf-8"))
        filename_to_hash = json.loads(res2.read().decode("utf-8"))
        return hash_to_filename, filename_to_hash

    def put_mappings(self, hash_to_filename: dict[str, str], filename_to_hash: dict[str, str]) -> None:
        hash_to_filename_json = json.dumps(hash_to_filename).encode("utf-8")
        filename_to_hash_json = json.dumps(filename_to_hash).encode("utf-8")
        self.minio_client.put_object(bucket_name=self.bucket_name, object_name=self.hash_to_filename_path, data=io.BytesIO(hash_to_filename_json), length=len(hash_to_filename_json))
        self.minio_client.put_object(bucket_name=self.bucket_name, object_name=self.filename_to_hash_path, data=io.BytesIO(filename_to_hash_json), length=len(filename_to_hash_json))

    def delete_mappings(self) -> None:
        self.minio_client.remove_object(bucket_name=self.bucket_name, object_name=self.hash_to_filename_path)
        self.minio_client.remove_object(bucket_name=self.bucket_name, object_name=self.filename_to_hash_path)

    def add_instance_to_mappings(self, file_hash: str, filename: str) -> None:
        hash_to_filename, filename_to_hash = self.get_mappings()
        hash_to_filename[file_hash] = filename
        filename_to_hash[filename] = file_hash
        self.put_mappings(hash_to_filename, filename_to_hash)

    def delete_instance_from_mappings(self, file_hash: str, filename: str) -> None:
        hash_to_filename, filename_to_hash = self.get_mappings()
        hash_to_filename.pop(file_hash)
        filename_to_hash.pop(filename)
        self.put_mappings(hash_to_filename, filename_to_hash)
