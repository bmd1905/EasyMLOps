import argparse
import sys
from typing import Dict, Type

from dotenv import find_dotenv, load_dotenv
from pyflink.datastream import StreamExecutionEnvironment

from .jobs.base import FlinkJob

load_dotenv(find_dotenv())


def get_available_jobs() -> Dict[str, Type[FlinkJob]]:
    """Return a dictionary of available jobs"""
    jobs = {}

    # Lazy import jobs based on command line argument
    job_name = sys.argv[1] if len(sys.argv) > 1 else None

    if job_name == "schema_validation" or job_name is None:
        from .jobs.schema_validation_job import SchemaValidationJob

        jobs["schema_validation"] = SchemaValidationJob

    if job_name == "alert_invalid_events" or job_name is None:
        from .jobs.alert_invalid_events_job import AlertInvalidEventsJob

        jobs["alert_invalid_events"] = AlertInvalidEventsJob

    if job_name == "validated_events_to_features" or job_name is None:
        from .jobs.validated_events_to_features_job import ValidatedEventsToFeaturesJob

        jobs["validated_events_to_features"] = ValidatedEventsToFeaturesJob

    return jobs


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
