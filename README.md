# Process Optimization ML Service

A high-performance process optimization service that uses machine learning models to continuously optimize industrial processes. Built with intelligent in-memory caching, MinIO integration, and flexible deployment modes.

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
- **🧠 ML Model Integration**: PyTorch models with intelligent in-memory caching from MinIO
- **⚡ Advanced Caching**: Fast in-memory caching for models, scalers, configs, and timestamps with version-aware invalidation
- **🔄 Multiple Run Modes**: Continuous optimization, API server, or hybrid mode
- **📊 Real-time Data**: PostgreSQL integration for live process data

## 📋 Features

### Core Capabilities
- **Continuous Optimization**: Automated optimization cycles every 5 minutes
- **REST API**: On-demand optimization via HTTP endpoints
- **Strategy Management**: Version-controlled optimization strategies from MinIO with automatic cache invalidation
- **Intelligent Caching**: Advanced in-memory caching system with automatic version-based invalidation
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
  port: 8005

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
curl http://localhost:8005/health

# Run optimization
curl -X POST http://localhost:8005/optimize \
  -H "Content-Type: application/json" \
  -d '{"input_data": {...}}'

# Cache statistics
curl http://localhost:8005/cache/stats

# Clear cache
curl -X POST http://localhost:8005/cache/clear
```

### Example API Usage

```python
import requests

# Run optimization
response = requests.post('http://localhost:8005/optimize', json={
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
│   ├── task/
│   │   └── math_optimizer/     # Math optimization module
│   │       ├── strategy/       # Optimization strategy components
│   │       │   ├── strategy.py         # Main optimization strategy
│   │       │   ├── data_context.py     # Data management
│   │       │   ├── variable.py         # Variable definitions
│   │       │   ├── post_processor.py   # Result post-processing
│   │       │   └── skills/             # Optimization skills
│   │       │       ├── models.py       # ML inference models
│   │       │       ├── functions.py    # Math functions
│   │       │       ├── constraints.py  # Constraint handling
│   │       │       ├── optimizer.py    # IPOPT optimization
│   │       │       └── composition.py  # Skill composition
│   │       └── strategy-manager/       # Strategy management
│   │           ├── strategy_manager.py # Version & config management
│   │           ├── strategy_version.yaml # Version tracking
│   │           └── last_run_timestamp.yaml # Timestamp tracking
│   ├── storage/
│   │   ├── minio.py           # MinIO integration with in-memory caching
│   │   ├── in_memory_cache.py # Advanced caching system
│   │   ├── psql.py            # PostgreSQL connector
│   │   └── interface.py       # Storage interfaces
│   ├── core/
│   │   └── logging_config.py  # Structured logging setup
│   ├── telemetry/
│   │   └── logging.py         # Telemetry and monitoring
│   └── tests/
│       ├── test_api.py        # API endpoint tests
│       └── test_timestamp_caching.py # Cache system tests
├── config.yaml                 # Main configuration
├── config.docker.yaml          # Docker configuration
├── process-optimization-strategy-config.yaml  # Strategy config
├── docker-compose.yml          # Production deployment
├── docker-compose.dev.yml      # Development deployment
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

## 🚀 Performance Features

### Advanced In-Memory Caching
- **Model Caching**: PyTorch models loaded directly into memory (no temporary files)
- **Scaler Caching**: ML scalers cached in memory for fast access
- **Version-Aware Invalidation**: Automatic cache clearing when strategy versions change
- **Cache Statistics**: Real-time monitoring of cache hit/miss ratios and memory usage

### Caching Benefits
- **⚡ Fast Model Loading**: Models cached in memory eliminate file I/O overhead
- **🔄 Version Safety**: Cache automatically invalidates when `strategy_version.yaml` changes
- **📊 Memory Efficient**: No temporary files, all caching done in memory
- **🎯 Smart Invalidation**: Only clears cache when necessary, preserving performance

## 📊 Monitoring

The service provides comprehensive logging and monitoring:

- **Structured Logging**: JSON-formatted logs with contextual information
- **Cache Statistics**: Real-time cache hit/miss ratios and performance metrics
- **Optimization Metrics**: Cycle completion times and success rates
- **Health Endpoints**: Service status and dependency health checks
- **Cache Visibility**: Detailed logging of cached model and scaler locations

## 🔧 Troubleshooting

### Common Issues

1. **Cache Miss Issues**: Check memory usage and restart if needed. Models and scalers are now cached in memory.
2. **Model Loading Errors**: Verify MinIO connection and bucket permissions. Check logs for "Model cached in memory" messages.
3. **Version Mismatch**: If models seem outdated, check `strategy_version.yaml` - cache auto-invalidates on version changes.
4. **Database Timeouts**: Check PostgreSQL connection settings
5. **Optimization Failures**: Review strategy configuration and variable constraints

### Debugging

```bash
# Enable debug logging
# Set log.level: DEBUG in config.yaml

# Check service status
curl http://localhost:8005/health

# View detailed cache statistics (shows models, scalers, configs)
curl http://localhost:8005/cache/stats

# Clear all caches (forces fresh loading from MinIO)
curl -X POST http://localhost:8005/cache/clear

# Check logs for cache activity
tail -f logs/app.log | grep -E "(cached|cache|version)"
```

### Cache Debugging
Look for these log messages to understand cache behavior:
- `"Model cached in memory for: <path>"` - Model successfully cached
- `"Using cached model object for: <path>"` - Model loaded from cache
- `"Version changed from X to Y, invalidating cache"` - Cache cleared due to version change
- `"Cache invalidated due to version change. Cleared N items"` - Shows what was cleared

## 📝 License

This project is proprietary software developed for industrial process optimization.

## 🤝 Contributing

Please follow the established code style and include tests for new features. Contact the development team for contribution guidelines.