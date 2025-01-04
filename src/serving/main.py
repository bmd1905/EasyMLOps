import json
import os
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from online_feature_service import OnlineFeatureService
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import SpanKind
from prediction_service import PredictionService
from pydantic import BaseModel

import ray
from ray import serve

app = FastAPI()


working_dir = os.path.dirname(os.path.abspath(__file__))

ray.init(
    address="auto",
    namespace="serving",
    runtime_env={"working_dir": working_dir},
    log_to_driver=True,
)


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    class PredictionRequest(BaseModel):
        user_id: int = 530834332
        product_id: int = 1005073
        user_session: str = "040d0e0b-0a40-4d40-bdc9-c9252e877d9c"

    def __init__(
        self,
        feature_service: OnlineFeatureService,
        prediction_service: PredictionService,
    ):
        self._feature_service = feature_service
        self._prediction_service = prediction_service
        self._FEATURE_COLUMNS = [
            "brand",
            "price",
            "event_weekday",
            "category_code_level1",
            "category_code_level2",
            "activity_count",
        ]

        # Initialize telemetry
        self._setup_telemetry()

    def _setup_telemetry(self):
        # Initialize Resource
        resource = Resource.create({ResourceAttributes.SERVICE_NAME: "serving"})

        # Initialize TracerProvider
        trace.set_tracer_provider(TracerProvider(resource=resource))
        otlp_span_exporter = OTLPSpanExporter(
            endpoint="http://otel-collector:4317", insecure=True
        )
        span_processor = BatchSpanProcessor(otlp_span_exporter)
        trace.get_tracer_provider().add_span_processor(span_processor)

        # Initialize MeterProvider
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint="http://otel-collector:4317", insecure=True)
        )
        metrics.set_meter_provider(
            MeterProvider(resource=resource, metric_readers=[metric_reader])
        )

        # Get tracer and meter
        self.tracer = trace.get_tracer(__name__)
        self.meter = metrics.get_meter(__name__)

        # Create metrics
        self.request_counter = self.meter.create_counter(
            name="request_counter",
            description="Counts the number of requests",
            unit="1",
        )

        self.request_duration = self.meter.create_histogram(
            name="request_duration",
            description="Duration of requests",
            unit="ms",
        )

    @app.post("/predict")
    async def predict(self, requests: List[PredictionRequest]):
        # Start the main request span
        with self.tracer.start_as_current_span(
            "predict_request", kind=SpanKind.SERVER
        ) as request_span:
            # Add request metadata
            request_span.set_attribute("request_count", len(requests))

            # Increment request counter
            self.request_counter.add(1, {"endpoint": "root"})

            features = []

            # Get features from online store for each request
            with self.tracer.start_span("process_requests") as process_span:  # noqa: F841
                for request in requests:
                    # Get online features with propagated context
                    feature_result = await self._feature_service.remote(
                        user_id=request.user_id, product_id=request.product_id
                    )

                    if not feature_result["success"]:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Failed to get features for user_id={request.user_id}, product_id={request.product_id}: {feature_result['error']}",
                        )

                    # Convert feature lists to single values
                    feature_dict = {}
                    for key, value in feature_result["features"].items():
                        feature_dict[key] = (
                            value[0] if isinstance(value, list) else value
                        )

                    features.append(feature_dict)

                # Filter features
                features = [
                    {
                        key: feature[key]
                        for key in self._FEATURE_COLUMNS
                        if key in feature
                    }
                    for feature in features
                ]

                # Get predictions with propagated context
                result = await self._prediction_service.remote(features)

                # Load to json
                result_json = json.dumps(result)

            return Response(
                content=result_json,
                media_type="application/json",
            )


feature_service = OnlineFeatureService.bind(
    feature_retrieval_url="http://feature-retrieval:8001"
)
prediction_service = PredictionService.bind(
    model_name="purchase_prediction_model", mlflow_uri="http://mlflow_server:5000"
)
entrypoint = APIIngress.bind(feature_service, prediction_service)

serve.start(http_options={"host": "0.0.0.0", "port": 8000})
