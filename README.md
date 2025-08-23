# Process Optimization Application

A professional process optimization application with MinIO integration, intelligent caching, and flexible deployment options.

## Architecture Diagram

<img width="2654" height="1232" alt="image" src="https://github.com/user-attachments/assets/b429af56-7192-4ccc-baaa-ccc87d3cb2ca" />


## Quick Start
**Run the application**:
   
   The application has a single entry point with multiple modes:
   
   ```bash
    - `cd uptime_ml_process_optimization`

    ### Setup

    - `conda create -n uptime_ml_process_optimization python=3.12.0`

    - `conda activate uptime_ml_process_optimization`

    ### Run

    - [Only if dependencies are updated] `pip install -r requirements.txt`

    - `python src`
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

### Strategy Configuration
The optimization strategy is defined in a separate YAML file that includes:
- **Variables**: Operative, informative, calculated, and predicted variables
- **Skills**: ML models, constraints, and optimization components
- **Tasks**: Execution sequence for optimization cycles


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


```bash
# Build and run with Docker Compose
docker-compose up -d

# Development mode
docker-compose -f docker-compose.dev.yml up -d
```


### Cache Debugging
Look for these log messages to understand cache behavior:
- `"Model cached in memory for: <path>"` - Model successfully cached
- `"Using cached model object for: <path>"` - Model loaded from cache
- `"Version changed from X to Y, invalidating cache"` - Cache cleared due to version change
- `"Cache invalidated due to version change. Cleared N items"` - Shows what was cleared
