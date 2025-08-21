#!/bin/bash

# Setup and Run Script for MinIO-based ML Process Optimization
# This script sets up MinIO and optionally uploads your files

set -e  # Exit on error

echo "🚀 ML Process Optimization MinIO Setup"
echo "========================================"

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ docker-compose.yml not found. Please run this script from the project root."
    exit 1
fi

# Function to check if MinIO is running
check_minio() {
    if curl -f http://localhost:9090/minio/health/live > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Start MinIO
echo "📦 Starting MinIO..."
docker-compose up -d

echo "⏳ Waiting for MinIO to be ready..."
for i in {1..30}; do
    if check_minio; then
        echo "✅ MinIO is ready!"
        break
    fi
    echo "   Waiting... ($i/30)"
    sleep 2
done

if ! check_minio; then
    echo "❌ MinIO failed to start after 60 seconds"
    exit 1
fi

echo ""
echo "🌐 MinIO is now running:"
echo "   API: http://localhost:9090"
echo "   Web UI: http://localhost:9091"
echo "   Username: minioadmin"
echo "   Password: minioadmin123"
echo ""

# Ask user what they want to do
echo "What would you like to do?"
echo "1) Upload config.yaml (version 1.0.0)"
echo "2) Upload model files from ../process-optimization/data"
echo "3) List files in MinIO"
echo "4) Run the optimization application"
echo "5) Just show status and exit"
echo ""
read -p "Enter your choice (1-5): " choice

case $choice in
    1)
        echo "📤 Uploading config.yaml..."
        python scripts/upload_to_minio.py upload-config --config config.yaml --version 1.0.0
        ;;
    2)
        echo "📤 Uploading model files..."
        if [ -d "../process-optimization/data" ]; then
            python scripts/upload_to_minio.py upload-models --data-dir ../process-optimization/data/models
        else
            echo "❌ Directory ../process-optimization/data not found"
            echo "Please specify the correct path to your model files"
        fi
        ;;
    3)
        echo "📁 Listing files in MinIO..."
        python scripts/upload_to_minio.py list
        ;;
    4)
        echo "🏃 Running optimization application..."
        python main.py
        ;;
    5)
        echo "ℹ️ MinIO is running and ready for use"
        ;;
    *)
        echo "❌ Invalid choice"
        exit 1
        ;;
esac

echo ""
echo "✅ Done! MinIO is running in the background."
echo "   Use 'docker-compose down' to stop MinIO when finished."
