FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY .env .

# Create data and logs directories
RUN mkdir -p data logs

# Set environment variable to tell code it's running in Docker
ENV RUNNING_IN_DOCKER=true

# Set Python to unbuffered mode for better logging in containers
ENV PYTHONUNBUFFERED=1

# Default command will run the streaming component
CMD ["python", "src/stream_matrix.py"]