from include.utils.ray_setup import patch_logging_proxy

# Apply patch before importing Ray-related modules
patch_logging_proxy()

from training_pipeline.data_loader import load_training_data  # noqa: E402
from training_pipeline.hyperparameter_tuning import tune_hyperparameters  # noqa: E402
from training_pipeline.model_trainer import train_final_model  # noqa: E402
from training_pipeline.results_saver import save_results  # noqa: E402

from airflow.decorators import dag  # noqa: E402
from include.config.tune_config import DEFAULT_ARGS  # noqa: E402


@dag(
    dag_id="training_pipeline",
    default_args=DEFAULT_ARGS,
    description="ML training pipeline with hyperparameter tuning",
    schedule="@weekly",
    catchup=False,
    tags=["training", "ml"],
    max_active_runs=5,
)
def training_pipeline():
    """
    ### ML Training Pipeline with Hyperparameter Tuning
    This DAG handles the end-to-end training process:
    1. Load training data
    2. Tune hyperparameters using Ray Tune
    3. Train final model with best parameters
    4. Save results and metrics
    """
    # Load data
    data = load_training_data()

    # Tune hyperparameters
    best_params = tune_hyperparameters(data)

    # Train final model with dependencies
    results = train_final_model(data, best_params)

    # Save results (no need to set explicit dependencies)
    save_results(results)


# Create DAG instance
training_pipeline_dag = training_pipeline()
