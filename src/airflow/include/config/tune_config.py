from datetime import timedelta
from pathlib import Path

import pendulum

# Ray configuration
CONN_ID = "ray_conn"
FOLDER_PATH = Path(__file__).parent.parent / "ray_scripts"
DAGS_PATH = Path(__file__).parent.parent

RAY_TASK_CONFIG = {
    "conn_id": CONN_ID,
    "runtime_env": {
        "working_dir": str(FOLDER_PATH),
        "py_modules": [str(DAGS_PATH)],
        "pip": [
            "ray[train,tune]==2.9.0",
            "xgboost_ray==0.1.19",
            "xgboost==2.0.3",
            "pandas==1.3.0",
            "astro-provider-ray==0.3.0",
            "boto3>=1.34.90",
            "pyOpenSSL==23.2.0",
            "cryptography==41.0.7",
            "urllib3<2.0.0",
            "tensorboardX==2.6.2",
            "pyarrow",
        ],
    },
    "num_cpus": 3,
    "num_gpus": 0,
    "poll_interval": 5,
    "xcom_task_key": "dashboard",
}

# MinIO Configuration
MINIO_CONFIG = {
    "endpoint_override": "http://minio:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "allow_bucket_creation": True,
}

# Training Configuration
TRAINING_CONFIG = {
    "model_path": "model-checkpoints-bucket/xgb_model",
    "test_size": 0.3,
    "num_workers": 2,
    "resources_per_worker": {"CPU": 2},
    "use_gpu": False,
    "num_boost_round": 5,
}

# XGBoost Parameters
XGBOOST_PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": ["logloss", "error", "rmse", "mae", "auc"],
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.3,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
}

# Feature Configuration
FEATURE_COLUMNS = [
    "brand",
    "price",
    "event_weekday",
    "category_code_level1",
    "category_code_level2",
    "activity_count",
    "is_purchased",
]

CATEGORICAL_COLUMNS = [
    "brand",
    "event_weekday",
    "category_code_level1",
    "category_code_level2",
]

# DAG Configuration
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "start_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
}
