from dataclasses import dataclass

from airflow.models import Variable


@dataclass
class DataPipelineConfig:
    bucket_name: str
    path_prefix: str
    schema_registry_url: str
    schema_subject: str
    batch_size: int

    def __post_init__(self):
        """Validate the configuration"""
        if not self.bucket_name:
            raise ValueError("bucket_name cannot be empty")
        if not self.path_prefix:
            raise ValueError("path_prefix cannot be empty")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

    @classmethod
    def from_airflow_variables(cls):
        """Load configuration from Airflow Variables"""
        return cls(
            bucket_name=Variable.get(
                "MINIO_BUCKET_NAME", default_var="validated-events-bucket"
            ),
            path_prefix=Variable.get(
                "MINIO_PATH_PREFIX",
                default_var="topics/validated-events-topic/year=2024/month=12",
            ),
            schema_registry_url=Variable.get(
                "SCHEMA_REGISTRY_URL", default_var="http://schema-registry:8081"
            ),
            schema_subject=Variable.get(
                "SCHEMA_SUBJECT", default_var="raw-events-topic-schema"
            ),
            batch_size=int(Variable.get("BATCH_SIZE", default_var="1000")),
        )