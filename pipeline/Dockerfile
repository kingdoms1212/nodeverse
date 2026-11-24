FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY topics.py contract.py collector.py ./
COPY common ./common
COPY remote ./remote
COPY tools/__init__.py tools/dlq_peek.py ./tools/
CMD ["uvicorn", "collector:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
