# MinIO Setup and Usage Guide

This guide explains how to set up MinIO and upload your configuration and model files.

## 1. Start MinIO Server

First, start the MinIO server using Docker Compose:

```bash
# Start MinIO in the background
docker-compose up -d

# Check if containers are running
docker-compose ps
```

MinIO will be available at:
- **API**: http://localhost:9090
- **Web UI**: http://localhost:9091
- **Credentials**: `minioadmin` / `minioadmin123`

## 2. Upload Configuration File

Upload your config.yaml file with a specific version:

```bash
# Upload current config.yaml as version 1.0.0
python scripts/upload_to_minio.py upload-config --config config.yaml --version 1.0.0

# Upload with different version
python scripts/upload_to_minio.py upload-config --config config.yaml --version 1.1.0
```

## 3. Upload Model Files

Upload all your model files (.pth, .pkl, .json) from the data directory:

```bash
# Upload from the process-optimization data directory
python scripts/upload_to_minio.py upload-models --data-dir ../process-optimization/data

# Or from any other directory containing your models
python scripts/upload_to_minio.py upload-models --data-dir /path/to/your/model/files
```

Expected directory structure for model files:
```
data/
├── saved_models/
│   ├── MA_MinMax_lag0_Kiln_Drive_Current_ann_model_fold_5.pth
│   ├── 4_Calciner_outlet_CO_ann_model_fold_2.pth
│   └── 2_Clinker_temperature_ann_model_fold_2.pth
├── saved_scalers/
│   ├── Kiln_Drive_Current_minmax_scalers.pkl
│   ├── Calciner_outlet_CO_robust_scalers.pkl
│   └── Clinker_temperature_minmax_scalers.pkl
└── saved_metadata/
    ├── Kiln_Drive_Current_metadata.json
    ├── Calciner_outlet_CO_metadata.json
    └── Clinker_temperature_metadata.json
```

## 4. List Files in MinIO

Check what files are uploaded:

```bash
python scripts/upload_to_minio.py list
```

## 5. Update Deployed Config Version

After uploading a new config version, update the deployed version:

```bash
# Edit the metadata/deployed_config_version.yaml file
echo "config.yaml: 1.0.0" > metadata/deployed_config_version.yaml
```

## 6. Run the Application

Once everything is uploaded, you can run the application:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the optimization application
python main.py
```

The application will:
1. Read the deployed config version from `metadata/deployed_config_version.yaml`
2. Download the corresponding config file from MinIO
3. Load model files (.pth, .pkl, .json) from MinIO as needed
4. Run the optimization cycle

## 7. MinIO Web Interface

You can also manage files through the web interface:
1. Go to http://localhost:9091
2. Login with `minioadmin` / `minioadmin123`
3. Navigate to the `process-optimization` bucket
4. Upload/download files manually if needed

## File Organization in MinIO

### Process-Optimization Bucket
```
process-optimization/
├── configs/
│   ├── config-1.0.0.yaml
│   ├── config-1.1.0.yaml
│   └── etc.
└── models/
    ├── saved_models/model_file.pth
    ├── saved_scalers/scaler_file.pkl
    └── saved_metadata/metadata_file.json
```

## Troubleshooting

### MinIO Connection Issues
- Make sure Docker is running: `docker ps`
- Check MinIO logs: `docker-compose logs minio`
- Ensure ports 9090 and 9091 are not in use
- If ports are still in use, modify the ports in `docker-compose.yml`

### Upload Issues
- Check file paths exist
- Ensure MinIO server is running
- Verify credentials in `utils/minio_client.py`

### Model Loading Issues
- Verify files are uploaded correctly: `python scripts/upload_to_minio.py list`
- Check application logs for specific error messages
- Ensure the deployed config version matches an uploaded config file
