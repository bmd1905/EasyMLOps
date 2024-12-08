class PipelineError(Exception):
    """Base exception for pipeline errors"""

    pass


class MinioError(PipelineError):
    """Raised when MinIO operations fail"""

    pass


class TransformError(PipelineError):
    """Raised when data transformation fails"""

    pass


class PostgresError(PipelineError):
    """Raised when PostgreSQL operations fail"""

    pass


class SchemaError(PipelineError):
    """Raised when schema validation or conversion fails"""

    pass
