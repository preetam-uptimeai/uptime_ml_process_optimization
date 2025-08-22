# Process Optimization Application

A professional process optimization application with MinIO integration, intelligent caching, and flexible deployment options.

## Architecture Overview

This application follows a clean, modular architecture with proper separation of concerns:

- **Single Entry Point**: `app.py` - handles all startup modes
- **Service-Based Architecture**: Separate services for optimization and API
- **Intelligent Caching**: In-memory caching with version-aware invalidation
- **Flexible Deployment**: Continuous optimization, API server, or hybrid mode


## Architecture Diagram

<img width="2654" height="1232" alt="image" src="https://github.com/user-attachments/assets/b429af56-7192-4ccc-baaa-ccc87d3cb2ca" />

## Features

- **MinIO Integration**: Configuration and model files are loaded from MinIO object storage
- **In-Memory Caching**: Smart caching system that avoids redundant downloads from MinIO
- **Version-Aware Caching**: Automatic cache invalidation when config version changes
- **Version Management**: Config files are versioned (e.g., config-1.0.0.yaml)
- **Automatic Model Loading**: PyTorch models (.pth), scalers (.pkl), and metadata (.json) are loaded from MinIO
- **Performance Optimization**: Models and configs are cached in memory and reused across optimization cycles
- **Cache Management**: TTL-based expiration, statistics tracking, and cleanup utilities
- **Timestamp Caching**: Last run timestamps are cached in memory for faster access
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
   
   The application has a single entry point with multiple modes:
   
   ```bash
   # Run both continuous optimization and API server (default)
   python app.py
   
   # Run only continuous optimization
   python app.py --mode continuous
   
   # Run only API server
   python app.py --mode api --port 8080
   
   # Run with debug logging
   python app.py --debug
   
   # Show all options
   python app.py --help
   ```

## Cache Management

## Project Structure

```
process_optimization/
├── app.py                          # 🚀 Single entry point (all modes)
├── requirements.txt                # 📦 Python dependencies
├── config.yaml                     # ⚙️ Default configuration
├── docker-compose.yml              # 🐳 Container setup
│
├── src/                           # 📁 Source code
│   ├── core/
│   │   └── logging_config.py       # 📝 Centralized logging
│   ├── services/
│   │   ├── optimization_service.py  # 🔄 Continuous optimization
│   │   └── api_service.py          # 🌐 REST API service
│   └── tests/
│       ├── test_api.py             # 🧪 API endpoint tests
│       └── test_timestamp_caching.py # 🧪 Cache tests
│
├── examples/
│   └── api_client_example.py       # 📖 API usage examples
│
├── scripts/
│   └── cache_manager_cli.py        # 🎛️ Cache management CLI
│
├── utils/                          # 🛠️ Shared utilities
│   ├── cache_manager.py            # 💾 Memory caching
│   ├── config_manager.py           # ⚙️ Config handling
│   ├── minio_client.py             # ☁️ MinIO integration
│   └── db/                         # 🗄️ Database utilities
│
├── rto/                           # 🧠 Optimization engine
│   ├── strategy.py                 # 📋 Main strategy
│   ├── data_context.py            # 📊 Data handling
│   └── skills/                     # 🎯 Optimization skills
│
├── metadata/                      # 📄 Configuration files
│   ├── deployed_config_version.yaml
│   └── last_run_timestamp.yaml
│
└── logs/                          # 📜 Application logs
    └── app_YYYYMMDD.log
```

## Testing & Tools

### Application Testing
```bash
# Test timestamp caching functionality
python src/tests/test_timestamp_caching.py

# Test API endpoints
python src/tests/test_api.py
```

### Cache Management CLI
```bash
# Show cache statistics
python scripts/cache_manager_cli.py --stats

# Test cache functionality
python scripts/cache_manager_cli.py --test

# Clean up expired files
python scripts/cache_manager_cli.py --cleanup

# Clear all caches
python scripts/cache_manager_cli.py --clear

# Run all operations
python scripts/cache_manager_cli.py --all
```

## API Endpoints

The application provides a REST API for on-demand optimization:

### Start API Server
```bash
python app.py --mode api --host 0.0.0.0 --port 5000
```

### Available Endpoints

**POST /optimize** - Run single optimization cycle
```bash
curl -X POST http://localhost:5000/optimize \
  -H "Content-Type: application/json" \
  -d '{
    "input_data": {
      "Kiln_Feed_SFF_1_Feed_rate": 85.0,
      "Kiln_Coal_PV": 8.5,
      "Calciner_temperature_PV": 875.0,
      ...
    },
    "config": {
      "variables": {...},
      "skills": {...},
      "tasks": [...]
    }
  }'
```

**GET /health** - Health check
```bash
curl http://localhost:5000/health
```

**GET /cache/stats** - Get cache statistics
```bash
curl http://localhost:5000/cache/stats
```

**POST /cache/clear** - Clear all caches
```bash
curl -X POST http://localhost:5000/cache/clear
```

### API Testing
```bash
# Test all API endpoints
python src/tests/test_api.py

# Example API client usage
python examples/api_client_example.py
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
- **API Server**: Flask-based REST API for on-demand optimization cycles

## Performance Benefits

The caching system provides significant performance improvements:

- **🚀 Speed**: 3-10x faster model/config loading after first access
- **💾 Memory Efficient**: Intelligent TTL-based cache expiration
- **🔄 Consistency**: Same artifacts used across optimization cycles
- **🔄 Version-Aware**: Automatic cache invalidation on config version changes
- **🕒 Timestamp Caching**: Fast in-memory access to last run timestamps
- **🌐 REST API**: On-demand optimization via HTTP endpoints
- **🚀 Deployment Ready**: Safe cache updates when new models are deployed
- **📊 Monitoring**: Built-in cache statistics and version tracking
- **🧹 Cleanup**: Automatic cleanup of expired temporary files
