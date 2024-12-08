import argparse
from typing import Dict, Type

from dotenv import find_dotenv, load_dotenv
from pyflink.datastream import StreamExecutionEnvironment

from .jobs.alert_invalid_events_job import AlertInvalidEventsJob
from .jobs.base import FlinkJob
from .jobs.schema_validation_job import SchemaValidationJob

load_dotenv(find_dotenv())


def get_available_jobs() -> Dict[str, Type[FlinkJob]]:
    """Return a dictionary of available jobs"""
    return {
        "schema_validation": SchemaValidationJob,
        "alert_invalid_events": AlertInvalidEventsJob,
    }


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Flink job")
    parser.add_argument(
        "job_name", choices=get_available_jobs().keys(), help="Name of the job to run"
    )
    args = parser.parse_args()

    # Get the job class and create an instance
    job_class = get_available_jobs()[args.job_name]
    job = job_class()

    # Create and execute the pipeline
    env = StreamExecutionEnvironment.get_execution_environment()
    job.create_pipeline(env)
    env.execute(f"{job.job_name} Pipeline")
    print(f"Job {job.job_name} has been started successfully!")


if __name__ == "__main__":
    main()
