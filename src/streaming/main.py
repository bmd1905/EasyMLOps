import argparse
from typing import Dict, Type

from dotenv import find_dotenv, load_dotenv
from pyflink.datastream import StreamExecutionEnvironment

from .jobs.base import FlinkJob

load_dotenv(find_dotenv())


def get_available_jobs() -> Dict[str, Type[FlinkJob]]:
    """Return a dictionary of available jobs with lazy loading"""
    return {
        "schema_validation": lambda: __import__(
            "streaming.jobs.schema_validation_job", fromlist=["SchemaValidationJob"]
        ).SchemaValidationJob,
        "alert_invalid_events": lambda: __import__(
            "streaming.jobs.alert_invalid_events_job",
            fromlist=["AlertInvalidEventsJob"],
        ).AlertInvalidEventsJob,
        "validated_events_to_features": lambda: __import__(
            "streaming.jobs.validated_events_to_features_job",
            fromlist=["ValidatedEventsToFeaturesJob"],
        ).ValidatedEventsToFeaturesJob,
    }


def get_job_class(job_name: str) -> Type[FlinkJob]:
    """Get the job class, loading it only when requested"""
    jobs = get_available_jobs()
    if job_name not in jobs:
        raise ValueError(f"Unknown job: {job_name}")
    return jobs[job_name]()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run a Flink job")
    parser.add_argument(
        "job_name", choices=get_available_jobs().keys(), help="Name of the job to run"
    )
    args = parser.parse_args()

    try:
        # Get the job class and create an instance
        job_class = get_job_class(args.job_name)
        job = job_class()

        # Create and execute the pipeline
        env = StreamExecutionEnvironment.get_execution_environment()
        job.create_pipeline(env)
        env.execute(f"{job.job_name} Pipeline")
        print(f"Job {job.job_name} has been started successfully!")
    except Exception as e:
        print(f"Error running job {args.job_name}: {str(e)}")
        raise


if __name__ == "__main__":
    main()
