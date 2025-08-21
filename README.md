# Process Optimization with MinIO & In-Memory Caching

This application supports loading configuration and model files from MinIO object storage with intelligent in-memory caching for optimal performance.

## Features

- **MinIO Integration**: Configuration and model files are loaded from MinIO object storage
- **In-Memory Caching**: Smart caching system that avoids redundant downloads from MinIO
- **Version-Aware Caching**: Automatic cache invalidation when config version changes
- **Version Management**: Config files are versioned (e.g., config-1.0.0.yaml)
- **Automatic Model Loading**: PyTorch models (.pth), scalers (.pkl), and metadata (.json) are loaded from MinIO
- **Performance Optimization**: Models and configs are cached in memory and reused across optimization cycles
- **Cache Management**: TTL-based expiration, statistics tracking, and cleanup utilities
- **Deployment Safety**: Cache automatically refreshes when new model versions are deployed
- **Fallback Support**: Can still run with local files if needed

## Quick Start

1. **Start MinIO and setup**:
   ```bash
   ./scripts/setup_and_run.sh
   ```

2. **Upload your files** (if not done through the setup script):
   ```bash
   # Upload config file
   python scripts/upload_to_minio.py upload-config --config config.yaml --version 1.0.0
   
   # Upload model files  
   python scripts/upload_to_minio.py upload-models --data-dir ../process-optimization/data
   ```

3. **Run the application**:
   ```bash
   python main.py
   ```

## Cache Management

### Cache Performance Testing
Test the caching system performance:
```bash
python test_cache_performance.py
```

### Version-Aware Cache Testing
Test automatic cache invalidation on config version changes:
```bash
python test_version_invalidation.py
```

### Cache Management CLI
Manage cache operations:
```bash
# Show cache statistics
python cache_manager_cli.py --stats

# Test cache functionality
python cache_manager_cli.py --test

# Clean up expired files
python cache_manager_cli.py --cleanup

# Clear all caches
python cache_manager_cli.py --clear

# Run all operations
python cache_manager_cli.py --all
```

## Detailed Setup

See [scripts/README.md](scripts/README.md) for detailed setup instructions.

## Configuration

The application reads the deployed config version from `metadata/deployed_config_version.yaml` and downloads the corresponding config file from MinIO.

## Architecture

- **ConfigManager**: Reads config version and loads config from MinIO
- **MinIOClient**: Handles all MinIO operations with intelligent caching
- **CacheManager**: Singleton cache manager with TTL-based expiration
- **InferenceModel**: Modified to load PyTorch models from MinIO with caching
- **OptimizationStrategy**: Uses MinIO-based config loading by default

## Performance Benefits

The caching system provides significant performance improvements:

- **🚀 Speed**: 3-10x faster model/config loading after first access
- **💾 Memory Efficient**: Intelligent TTL-based cache expiration
- **🔄 Consistency**: Same artifacts used across optimization cycles
- **🔄 Version-Aware**: Automatic cache invalidation on config version changes
- **🚀 Deployment Ready**: Safe cache updates when new models are deployed
- **📊 Monitoring**: Built-in cache statistics and version tracking
- **🧹 Cleanup**: Automatic cleanup of expired temporary files