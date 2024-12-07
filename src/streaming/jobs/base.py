from abc import ABC, abstractmethod

from pyflink.datastream import StreamExecutionEnvironment


class FlinkJob(ABC):
    """Base class for all Flink jobs"""

    @abstractmethod
    def create_pipeline(self, env: StreamExecutionEnvironment):
        """Create and return the job pipeline"""
        pass

    @property
    @abstractmethod
    def job_name(self) -> str:
        """Return the name of the job"""
        pass
