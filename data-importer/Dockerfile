FROM python:3.9-slim

WORKDIR /app

# Install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the import script
COPY import_data.py .

# Log message about streaming configuration
RUN echo "Configured import_data.py with streaming processing to handle large datasets efficiently"
RUN echo "Memory usage will be monitored and optimized during import process"

# Set Python memory optimization environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV MALLOC_TRIM_THRESHOLD_=100000

# Create script to run with proper exit handling
RUN echo '#!/bin/bash\n\
echo "Starting memory-optimized DVF data import..."\n\
echo "Available memory: $(free -h | head -2)"\n\
python import_data.py\n\
exit_code=$?\n\
echo "Import process completed with exit code: $exit_code"\n\
echo "Final memory usage: $(free -h | head -2)"\n\
exit $exit_code' > /app/start_import.sh && chmod +x /app/start_import.sh

# Set the entry point to the wrapper script
CMD ["/app/start_import.sh"] 