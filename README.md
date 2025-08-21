# Process Optimization with MinIO

This application now supports loading configuration and model files from MinIO object storage, enabling better deployment flexibility and version management.

## Features

- **MinIO Integration**: Configuration and model files are loaded from MinIO object storage
- **Version Management**: Config files are versioned (e.g., config-1.0.0.yaml)
- **Automatic Model Loading**: PyTorch models (.pth), scalers (.pkl), and metadata (.json) are loaded from MinIO
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

## Detailed Setup

See [scripts/README.md](scripts/README.md) for detailed setup instructions.

## Configuration

The application reads the deployed config version from `metadata/deployed_config_version.yaml` and downloads the corresponding config file from MinIO.

## Architecture

- **ConfigManager**: Reads config version and loads config from MinIO
- **MinIOClient**: Handles all MinIO operations (download models, scalers, metadata)
- **InferenceModel**: Modified to load PyTorch models from MinIO
- **OptimizationStrategy**: Uses MinIO-based config loading by default