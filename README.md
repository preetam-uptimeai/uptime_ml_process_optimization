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

A high-performance process optimization service that uses machine learning models to continuously optimize industrial processes. Built with intelligent caching, MinIO integration, and flexible deployment modes.

## 🚀 Quick Start

### Prerequisites
- Python 3.8+

- MinIO (for model storage)
- PostgreSQL (for data storage)

### Installation & Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure the service
cp config.yaml.example config.yaml
# Edit config.yaml with your database and storage settings

# 3. Start the service
python -m src
```

## 🏗️ Architecture

This service follows a clean, modular architecture:

- **🎯 Strategy-Based Optimization**: Configurable optimization strategies with multiple skill types
- **🧠 ML Model Integration**: PyTorch models with intelligent caching from MinIO
- **⚡ In-Memory Caching**: Fast in-memory caching for models, configs, and data
- **🔄 Multiple Run Modes**: Continuous optimization, API server, or hybrid mode
- **📊 Real-time Data**: PostgreSQL integration for live process data

## 📋 Features

### Core Capabilities
- **Continuous Optimization**: Automated optimization cycles every 5 minutes
- **REST API**: On-demand optimization via HTTP endpoints
- **Strategy Management**: Version-controlled optimization strategies from MinIO
- **Intelligent Caching**: In-memory caching with version awareness
- **Multi-Modal Deployment**: Run as continuous service, API server, or both

### Optimization Skills
- **🤖 ML Models**: PyTorch inference models for predictions
- **🧮 Math Functions**: Custom mathematical calculations
- **⚖️ Constraints**: Operational and safety constraints
- **🎯 Optimization**: IPOPT-based nonlinear optimization
- **🔧 Composition**: Chain multiple skills together

## ⚙️ Configuration

### Basic Configuration (`config.yaml`)
```yaml
app:
  mode: hybrid  # continuous, api, or hybrid

api:
  host: 0.0.0.0
  port: 5013

optimization:
  interval_seconds: 300
  config_file: process-optimization-strategy-config.yaml

storage:
  minio:
    endpoint: localhost:9002
    bucket: process-optimization

database:
  host: localhost
  port: 5432
  dbname: process_db
```

### Strategy Configuration
The optimization strategy is defined in a separate YAML file that includes:
- **Variables**: Operative, informative, calculated, and predicted variables
- **Skills**: ML models, constraints, and optimization components
- **Tasks**: Execution sequence for optimization cycles

## 🚀 Usage

### Run Modes

```bash
# Continuous optimization only
python -m src  # Uses config.yaml mode setting

# API server only
# Set mode: api in config.yaml

# Both continuous and API (hybrid)
# Set mode: hybrid in config.yaml
```

### API Endpoints

Start the API server and use these endpoints:

```bash
# Health check
curl http://localhost:5013/health

# Run optimization
curl -X POST http://localhost:5013/optimize \
  -H "Content-Type: application/json" \
  -d '{"input_data": {...}}'

# Cache statistics
curl http://localhost:5013/cache/stats

# Clear cache
curl -X POST http://localhost:5013/cache/clear
```

### Example API Usage

```python
import requests

# Run optimization
response = requests.post('http://localhost:5013/optimize', json={
    'input_data': {
        'Kiln_Feed_SFF_1_Feed_rate': 85.0,
        'Kiln_Coal_PV': 8.5,
        'Calciner_temperature_PV': 875.0
    }
})

result = response.json()
print(f"Optimization result: {result}")
```

## 📁 Project Structure

```
uptime_ml_process_optimization/
├── src/
│   ├── __main__.py              # Application entry point
│   ├── service/
│   │   ├── optimization.py     # Continuous optimization service
│   │   └── api.py              # REST API service
│   ├── strategy/
│   │   ├── strategy.py         # Main optimization strategy
│   │   ├── data_context.py     # Data management
│   │   └── skills/             # Optimization skills
│   ├── storage/
│   │   ├── minio.py           # MinIO integration

│   │   └── psql.py            # PostgreSQL connector
│   └── strategy-manager/
│       └── strategy_manager.py # Strategy version management
├── config.yaml                 # Main configuration
├── process-optimization-strategy-config.yaml  # Strategy config
└── requirements.txt            # Dependencies
```

## 🔧 Development

### Running Tests
```bash
# Run all tests
python -m pytest src/tests/

# Test specific components
python src/tests/test_api.py
python src/tests/test_timestamp_caching.py
```

### Cache Management
```bash
# View cache statistics
python examples/cache_manager_cli.py --stats

# Clear all caches
python examples/cache_manager_cli.py --clear
```

## 🐳 Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Development mode
docker-compose -f docker-compose.dev.yml up -d
```

## 📊 Monitoring

The service provides comprehensive logging and monitoring:

- **Structured Logging**: JSON-formatted logs with contextual information
- **Cache Statistics**: Real-time cache hit/miss ratios and performance metrics
- **Optimization Metrics**: Cycle completion times and success rates
- **Health Endpoints**: Service status and dependency health checks

## 🔧 Troubleshooting

### Common Issues

1. **Cache Miss Issues**: Check memory usage and restart if needed
2. **Model Loading Errors**: Verify MinIO connection and bucket permissions
3. **Database Timeouts**: Check PostgreSQL connection settings
4. **Optimization Failures**: Review strategy configuration and variable constraints

### Debugging

```bash
# Enable debug logging
# Set log.level: DEBUG in config.yaml

# Check service status
curl http://localhost:5013/health

# View cache statistics
curl http://localhost:5013/cache/stats
```

## 📝 License

This project is proprietary software developed for industrial process optimization.

## 🤝 Contributing
