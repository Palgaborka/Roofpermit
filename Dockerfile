# Playwright official image includes Chromium + OS deps already
FROM mcr.microsoft.com/playwright/python:v1.49.1-jammy

# Set working directory
WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Render sets PORT automatically; default to 10000 if not set
EXPOSE 10000

# Start FastAPI using root app.py
# Uses Render's PORT if available
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-10000} --proxy-headers --log-level info"]
