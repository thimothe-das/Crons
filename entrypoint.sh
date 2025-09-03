#!/bin/bash
set -e

echo "🚀 Starting DVF Data Import Process"
echo "Configuration:"
echo "  Start Year: ${DVF_START_YEAR:-2020}"
echo "  End Year: ${DVF_END_YEAR:-2024}" 
echo "  Chunk Size: ${DVF_CHUNK_SIZE:-10000}"
echo "  Max Memory: ${DVF_MAX_MEMORY:-128}MB"

# Wait a moment for database to be fully ready
sleep 5

# Run the import with environment variables
python dvf_importer.py \
    --start-year ${DVF_START_YEAR:-2020} \
    --end-year ${DVF_END_YEAR:-2024} \
    --chunk-size ${DVF_CHUNK_SIZE:-10000} \
    --max-memory ${DVF_MAX_MEMORY:-128} \
    --init-db

echo "✅ DVF Import Process Completed"
