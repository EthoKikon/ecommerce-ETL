# Use a slim official Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffer stdout/stderr (helps logs)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install minimal OS packages needed for some Python packages/builds
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy only requirements first to leverage Docker layer caching
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the project
COPY . /app

# Create outputs dir and make a non-root user to run the app
RUN mkdir -p /app/outputs \
    && useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app

USER appuser

# Default command: run the CLI (overrideable)
CMD ["python", "-m", "src.cli", "--pages", "1"]
