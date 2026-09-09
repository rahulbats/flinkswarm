# One image for every role; the k8s command/env picks worker vs orchestrator.
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY flinkswarm ./flinkswarm
RUN pip install --no-cache-dir .

COPY agent-spec.yaml ./agent-spec.yaml
ENV SWARM_SPEC=/app/agent-spec.yaml

# Default: run as a worker. Override `command` for the orchestrator.
CMD ["python", "-m", "flinkswarm.worker"]
