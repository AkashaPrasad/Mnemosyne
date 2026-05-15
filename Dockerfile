FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download the sentence-transformer model so it works offline
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

COPY . .

RUN pip install -e .

ENV PYTHONPATH=/app

CMD ["python", "-m", "pytest", "tests/", "-v", "--tb=short"]
