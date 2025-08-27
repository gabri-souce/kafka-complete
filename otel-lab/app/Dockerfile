FROM python:3.11-slim
WORKDIR /app
COPY app.py /app
RUN pip install flask opentelemetry-sdk opentelemetry-exporter-otlp opentelemetry-instrumentation-flask
EXPOSE 5000
CMD ["python", "app.py"]

