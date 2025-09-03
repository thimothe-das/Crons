# DVF Data Import Module

This module provides efficient import of French property transaction data (Demande de Valeurs Foncières) from multiple years into a PostgreSQL database, optimized for low-resource servers.

## 🏗️ Architecture

- **Database**: PostgreSQL with PostGIS in Docker (512MB memory limit)
- **Import Strategy**: Streaming downloads + chunked processing (10K rows/chunk)
- **Memory Optimization**: 128MB max usage, garbage collection after each chunk
- **Data Source**: Annual CSV.gz files from government URLs
- **Duplicate Handling**: Automatic deduplication using unique constraints

## 📁 Files Created

- `dvf_importer.py` - Main import module with streaming and chunked processing
- `db_schema.sql` - Optimized database schema with indexes
- `Dockerfile.importer` - Lightweight container for import service
- `import_dvf_data.sh` - Convenient shell script for common operations
- `test_import.py` - Test script using local example data

## 🚀 Quick Start

### 1. Start the Database
```bash
./import_dvf_data.sh start-db
```

### 2. Initialize Database Schema
```bash
./import_dvf_data.sh init
```

### 3. Import Data for Specific Years
```bash
# Import single year
./import_dvf_data.sh import-year 2023

# Import year range
./import_dvf_data.sh import 2020 2024
```

### 4. Check Status and Statistics
```bash
# Check import status by year
./import_dvf_data.sh status

# Get database statistics
./import_dvf_data.sh stats
```

## 🔧 Configuration

### Environment Variables
```bash
export DVF_BASE_URL="https://your-actual-url/{year}"  # Replace placeholder URL
export DVF_CHUNK_SIZE=10000                          # Rows per processing chunk
export DVF_MAX_MEMORY=128                            # Max memory usage (MB)
```

### Database Connection
The module uses the same PostgreSQL configuration as your existing API:
- Host: `postgres` (Docker) or `localhost`
- User: `dvf_user`
- Password: `dvf_password`
- Database: `dvf_data`

## 📊 Database Schema

The `dvf_data` table includes:
- **All 39 original CSV columns** (id_mutation, date_mutation, valeur_fonciere, etc.)
- **Metadata columns**: `import_year`, `import_date`
- **Computed column**: `prix_m2` (automatically calculated price per m²)
- **Optimized indexes** for common query patterns (apartments, price ranges, locations)

### Key Indexes
- `idx_dvf_type_local` - Filtered index for apartments
- `idx_dvf_apparts_complete` - Composite index for apartment queries
- `idx_dvf_unique_mutation` - Prevents duplicate imports

## 💾 Memory Optimization Features

1. **Streaming Downloads**: Files are not stored locally, processed in memory
2. **Chunked Processing**: 10,000 rows processed at a time
3. **Garbage Collection**: Explicit cleanup after each chunk
4. **Selective Columns**: Only necessary columns loaded during processing
5. **Batch Inserts**: Efficient bulk operations with 1,000-row pages

## 🔄 Usage Examples

### Complete Setup from Scratch
```bash
# Start database
./import_dvf_data.sh start-db

# Initialize schema
./import_dvf_data.sh init

# Import recent years
./import_dvf_data.sh import 2022 2024

# Check results
./import_dvf_data.sh stats
```

### Updating with New Data
```bash
# Import only the latest year
./import_dvf_data.sh import-year 2024

# Or clear and re-import if data was updated
./import_dvf_data.sh clear 2024
./import_dvf_data.sh import-year 2024
```

### Direct Python Usage
```python
from dvf_importer import DVFImporter

# Create importer with custom settings
importer = DVFImporter(
    base_url_template="https://your-url/{year}",
    chunk_size=5000,  # Smaller chunks for very low memory
    max_memory_mb=64
)

# Import specific years
results = importer.import_year_range(2020, 2024)
print(results)
```

## 🧪 Testing

Test the import process with your local example data:
```bash
python test_import.py
```

This will:
1. Initialize the database schema
2. Import your `example.csv` file
3. Validate data integrity
4. Show sample results

## 🔍 Monitoring and Logs

### View Real-time Logs
```bash
./import_dvf_data.sh logs
```

### Check Import Progress
The import process shows:
- Download progress with file sizes
- Processing progress by chunk
- Memory usage statistics
- Success/failure rates per chunk

### Log Files
- `dvf_import.log` - Detailed import logs with timestamps
- Console output - Real-time progress and status

## ⚠️ Troubleshooting

### Database Connection Issues
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# View PostgreSQL logs
docker-compose logs postgres

# Test database connection
docker-compose exec postgres psql -U dvf_user -d dvf_data -c "SELECT 1;"
```

### Memory Issues
If you encounter memory problems:
```bash
# Reduce chunk size
export DVF_CHUNK_SIZE=5000

# Reduce max memory
export DVF_MAX_MEMORY=64

# Re-run import
./import_dvf_data.sh import-year 2023
```

### Network Issues
```bash
# Test URL accessibility
curl -I https://your-actual-url/2023/full.csv.gz

# Use different base URL
export DVF_BASE_URL="https://alternative-url/{year}"
```

## 🔗 Integration with Existing API

Your existing `prix_moyen_appartements.py` API will automatically work with the imported data:

1. The API's `find_dvf_table()` function looks for `dvf_data` table ✅
2. All required columns are present in the schema ✅
3. The computed `prix_m2` column improves query performance ✅
4. Indexes optimize the API's common query patterns ✅

## 📈 Performance Expectations

On a 2GB RAM server:
- **2020 data (~3.5M records)**: ~45-60 minutes
- **Memory usage**: Stays under 256MB total
- **Database size**: ~2-3GB per year of data
- **Query performance**: Sub-second for filtered apartment searches

## 🛠️ Advanced Usage

### Custom URL Template
```bash
export DVF_BASE_URL="https://files.data.gouv.fr/geo-dvf/latest/csv/{year}"
./import_dvf_data.sh import 2023 2024
```

### Import with Different Settings
```bash
# Low-memory mode for very constrained servers
python dvf_importer.py --start-year 2023 --end-year 2023 --chunk-size 5000 --max-memory 64

# High-performance mode for servers with more resources
python dvf_importer.py --start-year 2020 --end-year 2024 --chunk-size 20000 --max-memory 256
```

### Maintenance Operations
```bash
# Clear all data and start fresh
./import_dvf_data.sh clear 2020
./import_dvf_data.sh clear 2021
./import_dvf_data.sh clear 2022
./import_dvf_data.sh import 2020 2022
```

## 🔒 Security Features

- Non-root Docker user for importer service
- SQL injection prevention in query building
- Environment variable configuration (no hardcoded credentials)
- Graceful shutdown handling (SIGINT/SIGTERM)

## 🎯 Next Steps

1. **Update the placeholder URL** in your environment:
   ```bash
   export DVF_BASE_URL="https://your-actual-data-url/{year}"
   ```

2. **Test with a single year** first:
   ```bash
   ./import_dvf_data.sh import-year 2023
   ```

3. **Monitor performance** and adjust chunk sizes if needed

4. **Set up automated imports** using cron jobs for regular updates
