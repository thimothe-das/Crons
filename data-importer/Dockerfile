FROM python:3.9-slim

WORKDIR /app

# Install required packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the import script
COPY import_data.py .

# Log message about address fields
RUN echo "Configured import_data.py to include adresse_nom_voie and nom_commune fields"

# Set the entry point
CMD ["python", "import_data.py"] 