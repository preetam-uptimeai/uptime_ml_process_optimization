# main entrypoint module

import sys
import os
import signal
import threading
import time
import yaml
import structlog
import logging
from datetime import datetime
from typing import Optional

# Import application services
from service.optimization import OptimizationService
from service.api import APIService
from core.logging_config import configure_structlog
from storage import init_redis_cache


class ProcessOptimizationApp:
    """Main application class that coordinates all services."""
    
    def __init__(self, configuration: dict):
        """
        Initialize the application.
        
        Args:
            configuration: Configuration dictionary loaded from config.yaml
        """
        self.configuration = configuration
        
        # Get application mode from config
        self.mode = configuration.get('app', {}).get('mode', 'hybrid')
        api_config = configuration.get('api', {})
        self.api_host = api_config.get('host', '0.0.0.0')
        self.api_port = api_config.get('port', 5000)
        self.debug = configuration.get('log', {}).get('level') == 'DEBUG'
        
        # Service instances
        self.optimization_service: Optional[OptimizationService] = None
        self.api_service: Optional[APIService] = None
        
        # Threading
        self.optimization_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        
        # Setup signal handlers for faster shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Set a secondary signal handler for immediate termination (double Ctrl+C)
        self._shutdown_count = 0
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self._shutdown_count += 1
        log = structlog.get_logger()
        
        if self._shutdown_count == 1:
            log.info(f"Received signal {signum}, initiating graceful shutdown...")
            log.info("Press Ctrl+C again for immediate termination")
            self.shutdown()
        else:
            log.warning("Force shutdown requested!")
            log.info("Forcing immediate process termination")
            os._exit(1)  # Immediate termination without cleanup
    
    def _run_continuous_optimization(self):
        """Run continuous optimization in background thread."""
        try:
            log = structlog.get_logger()
            log.info("Starting continuous optimization service...")
            self.optimization_service = OptimizationService(self.shutdown_event, self.configuration)
            self.optimization_service.run_continuous()
        except Exception as e:
            log = structlog.get_logger()
            log.error(f"Error in continuous optimization: {e}")
            import traceback
            log.error(traceback.format_exc())
    
    def start(self):
        """Start the application based on the selected mode."""
        log = structlog.get_logger()
        log.info("Starting Process Optimization Application")
        log.info(f"Mode: {self.mode}")
        log.info(f"Started at: {datetime.now()}")
        
        try:
            if self.mode in ["continuous", "hybrid"]:
                # Start continuous optimization in background thread
                self.optimization_thread = threading.Thread(
                    target=self._run_continuous_optimization,
                    name="OptimizationThread",
                    daemon=True
                )
                self.optimization_thread.start()
                log.info("Continuous optimization started in background")
            
            if self.mode in ["api", "hybrid"]:
                # Start API service (this will block the main thread)
                log.info(f"Starting API service on {self.api_host}:{self.api_port}")
                self.api_service = APIService(
                    host=self.api_host,
                    port=self.api_port,
                    debug=self.debug,
                    configuration=self.configuration
                )
                self.api_service.start()
            
            elif self.mode == "continuous":
                # If only continuous mode, wait for the thread
                log.info("Waiting for continuous optimization (Ctrl+C to stop)...")
                try:
                    while not self.shutdown_event.is_set():
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
        
        except KeyboardInterrupt:
            log.info("Shutdown requested by user")
        except Exception as e:
            log.error(f"Application error: {e}")
            import traceback
            log.error(traceback.format_exc())
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shutdown all services gracefully."""
        log = structlog.get_logger()
        log.info("Shutting down services...")
        
        # Signal shutdown to all services
        self.shutdown_event.set()
        
        # Stop API service
        if self.api_service:
            log.info("Stopping API service...")
            self.api_service.stop()
        
        # Wait for optimization thread to finish (shorter timeout for faster shutdown)
        if self.optimization_thread and self.optimization_thread.is_alive():
            log.info("Waiting for optimization service to stop...")
            self.optimization_thread.join(timeout=3)  # Reduced from 10 to 3 seconds
            if self.optimization_thread.is_alive():
                log.warning("Optimization thread did not stop gracefully, forcing shutdown")
        
        log.info("Application shutdown complete")


if __name__ == "__main__":
    
    # read the configuration file from current working directory
    configuration = {}
    try:
        with open("./config.yaml", "r") as yaml_file:
            configuration = yaml.load(yaml_file, Loader=yaml.FullLoader)
        if configuration is None:
            raise Exception("empty data in configuration file")
        print("configuration loaded from ./config.yaml")
    except Exception as e:
        print(f"error while loading the config.yaml: {e}")
        sys.exit(101)

    # Configure structlog with color-coded output
    log_level_str = configuration.get("log", {}).get("level", "INFO")
    configure_structlog(log_level=log_level_str, enable_file_logging=True)

    log = structlog.get_logger()

    log.debug("configuration loaded")
    log.debug("logger configured for global consumption")
    
    # Initialize Redis cache
    try:
        init_redis_cache(configuration)
        log.debug("Redis cache initialized")
    except Exception as e:
        log.error(f"Failed to initialize Redis cache: {e}")
        sys.exit(102)
    
    log.info(f"<<{configuration['meta']['id']}>> starting process optimization worker ...")

    # Create and start application
    app = ProcessOptimizationApp(configuration=configuration)
    app.start()
