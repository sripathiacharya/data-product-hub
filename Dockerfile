# Use a slim Python image
FROM python:3.12-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system deps (optional but useful for pandas, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# If you move to pyproject.toml later, we can switch this to `pip install .`
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and configuration
COPY src/ ./src/
COPY config/ ./config/
COPY sample-data/ ./sample-data/

# Expose API port
EXPOSE 8000

# Run the FastAPI app with uvicorn
CMD ["uvicorn", "data_product_hub.main:app", "--host", "0.0.0.0", "--port", "8000", "--app-dir", "src"]
