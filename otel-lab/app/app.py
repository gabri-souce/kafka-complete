# app.py
from flask import Flask, request
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor

# --- Configurazione Tracer OTLP ---
resource = Resource(attributes={
    "service.name": "otel-python-app"
})

tracer_provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(
    endpoint="http://otel-collector:4317"  # porta gRPC del collector
)
span_processor = BatchSpanProcessor(otlp_exporter)
tracer_provider.add_span_processor(span_processor)
trace.set_tracer_provider(tracer_provider)

# --- Applicazione Flask ---
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

@app.route("/")
def hello():
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("hello-request"):
        return "Hello, OpenTelemetry!\n"

@app.route("/greet/<name>")
def greet(name):
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("greet-request"):
        return f"Hello, {name}!\n"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


