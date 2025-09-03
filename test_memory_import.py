#!/usr/bin/env python3
"""
Test script for memory-optimized DVF data import
This script helps verify the streaming import process and monitor memory usage
"""

import os
import time
import subprocess
import psutil
from datetime import datetime

def get_memory_info():
    """Get system memory information"""
    memory = psutil.virtual_memory()
    return {
        'total': memory.total / (1024**3),  # GB
        'available': memory.available / (1024**3),  # GB
        'used': memory.used / (1024**3),  # GB
        'percent': memory.percent
    }

def get_docker_stats():
    """Get Docker container stats if available"""
    try:
        result = subprocess.run(['docker', 'stats', '--no-stream', '--format', 
                               'table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}'],
                              capture_output=True, text=True, timeout=10)
        return result.stdout
    except:
        return "Docker stats not available"

def test_postgres_connection():
    """Test PostgreSQL connection"""
    try:
        import psycopg2
        
        # Use the same connection parameters as the import script
        conn = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST', 'localhost'),
            database=os.environ.get('POSTGRES_DB', 'dvf_data'),
            user=os.environ.get('POSTGRES_USER', 'dvf_user'),
            password=os.environ.get('POSTGRES_PASSWORD', 'dvf_password'),
            port=os.environ.get('POSTGRES_PORT', '5432')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'dvf_data';")
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM dvf_data;")
            row_count = cursor.fetchone()[0]
        else:
            row_count = 0
        
        conn.close()
        
        return {
            'status': 'connected',
            'version': version[0],
            'table_exists': table_exists,
            'row_count': row_count
        }
    except Exception as e:
        return {
            'status': 'failed',
            'error': str(e)
        }

def monitor_import_process():
    """Monitor the import process if it's running"""
    try:
        # Look for Python processes running import_data.py
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info']):
            try:
                if proc.info['name'] == 'python' and 'import_data.py' in ' '.join(proc.info['cmdline']):
                    memory_mb = proc.info['memory_info'].rss / (1024**2)
                    return {
                        'pid': proc.info['pid'],
                        'memory_mb': memory_mb,
                        'status': 'running'
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return {'status': 'not_running'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def main():
    print("DVF Import Memory Test & Monitor")
    print("=" * 50)
    print(f"Test started at: {datetime.now()}")
    
    # System memory info
    print("\n1. SYSTEM MEMORY INFO:")
    memory_info = get_memory_info()
    print(f"   Total: {memory_info['total']:.1f} GB")
    print(f"   Available: {memory_info['available']:.1f} GB")
    print(f"   Used: {memory_info['used']:.1f} GB ({memory_info['percent']:.1f}%)")
    
    # Docker stats
    print("\n2. DOCKER CONTAINER STATS:")
    docker_stats = get_docker_stats()
    print(docker_stats)
    
    # PostgreSQL connection test
    print("\n3. POSTGRESQL CONNECTION TEST:")
    pg_info = test_postgres_connection()
    if pg_info['status'] == 'connected':
        print(f"   ✓ Connected to PostgreSQL")
        print(f"   Version: {pg_info['version']}")
        print(f"   Table exists: {pg_info['table_exists']}")
        print(f"   Row count: {pg_info['row_count']:,}")
    else:
        print(f"   ✗ Connection failed: {pg_info.get('error', 'Unknown error')}")
    
    # Import process monitoring
    print("\n4. IMPORT PROCESS STATUS:")
    proc_info = monitor_import_process()
    if proc_info['status'] == 'running':
        print(f"   ✓ Import process running (PID: {proc_info['pid']})")
        print(f"   Memory usage: {proc_info['memory_mb']:.1f} MB")
    elif proc_info['status'] == 'not_running':
        print("   - Import process not currently running")
    else:
        print(f"   ✗ Error monitoring process: {proc_info.get('error', 'Unknown error')}")
    
    # Configuration recommendations
    print("\n5. CONFIGURATION RECOMMENDATIONS:")
    if memory_info['available'] < 1.0:
        print("   ⚠ WARNING: Less than 1GB available memory")
        print("   Recommendation: Reduce CSV_CHUNK_SIZE to 2000 or lower")
    elif memory_info['available'] < 2.0:
        print("   ⚠ CAUTION: Less than 2GB available memory")
        print("   Recommendation: Keep CSV_CHUNK_SIZE at 3000 or lower")
    else:
        print("   ✓ Sufficient memory available")
        print("   Suggestion: Can increase CSV_CHUNK_SIZE to 5000 for faster processing")
    
    print(f"\nTest completed at: {datetime.now()}")

if __name__ == "__main__":
    main()
