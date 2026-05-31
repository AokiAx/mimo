# MiMo claw-worker — runs the region-gated MiMo parts on a mainland-IP host.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mimo_client.py worker.py ./

# No secrets baked in; everything comes from env + the panel at runtime.
CMD ["python", "worker.py"]
