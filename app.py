#!/usr/bin/env python3
"""
Process Optimization Application - Main Entry Point

This is the single entry point for the process optimization application.
It supports multiple modes: continuous optimization, API server, or both.

Usage:
    python app.py --mode continuous    # Run continuous optimization only
    python app.py --mode api          # Run API server only  
    python app.py --mode hybrid       # Run both (default)
    python app.py --help              # Show help
"""

import sys
import os
import signal
import threading
import time
import argparse
from datetime import datetime
from typing import Optional

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from services.optimization_service import OptimizationService
from services.api_service import APIService
from core.logging_config import setup_logging


class ProcessOptimizationApp:
    """Main application class that coordinates all services."""
    
    def __init__(self, mode: str = "hybrid", api_host: str = "0.0.0.0", api_port: int = 5000, debug: bool = False):
        """
        Initialize the application.
        
        Args:
            mode: Application mode ('continuous', 'api', 'hybrid')
            api_host: API server host
            api_port: API server port
            debug: Enable debug mode
        """
        self.mode = mode
        self.api_host = api_host
        self.api_port = api_port
        self.debug = debug
        
        # Service instances
        self.optimization_service: Optional[OptimizationService] = None
        self.api_service: Optional[APIService] = None
        
        # Threading
        self.optimization_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        
        # Setup logging
        self.logger = setup_logging(debug=debug)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown()
    
    def _run_continuous_optimization(self):
        """Run continuous optimization in background thread."""
        try:
            self.logger.info("Starting continuous optimization service...")
            self.optimization_service = OptimizationService(self.shutdown_event)
            self.optimization_service.run_continuous()
        except Exception as e:
            self.logger.error(f"Error in continuous optimization: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    def start(self):
        """Start the application based on the selected mode."""
        self.logger.info("Starting Process Optimization Application")
        self.logger.info(f"Mode: {self.mode}")
        self.logger.info(f"Started at: {datetime.now()}")
        
        try:
            if self.mode in ["continuous", "hybrid"]:
                # Start continuous optimization in background thread
                self.optimization_thread = threading.Thread(
                    target=self._run_continuous_optimization,
                    name="OptimizationThread",
                    daemon=True
                )
                self.optimization_thread.start()
                self.logger.info("Continuous optimization started in background")
            
            if self.mode in ["api", "hybrid"]:
                # Start API service (this will block the main thread)
                self.logger.info(f"Starting API service on {self.api_host}:{self.api_port}")
                self.api_service = APIService(
                    host=self.api_host,
                    port=self.api_port,
                    debug=self.debug
                )
                self.api_service.start()
            
            elif self.mode == "continuous":
                # If only continuous mode, wait for the thread
                self.logger.info("Waiting for continuous optimization (Ctrl+C to stop)...")
                try:
                    while not self.shutdown_event.is_set():
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
        
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")
        except Exception as e:
            self.logger.error(f"Application error: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shutdown all services gracefully."""
        self.logger.info("Shutting down services...")
        
        # Signal shutdown to all services
        self.shutdown_event.set()
        
        # Stop API service
        if self.api_service:
            self.logger.info("Stopping API service...")
            self.api_service.stop()
        
        # Wait for optimization thread to finish
        if self.optimization_thread and self.optimization_thread.is_alive():
            self.logger.info("Waiting for optimization service to stop...")
            self.optimization_thread.join(timeout=10)
            if self.optimization_thread.is_alive():
                self.logger.warning("Optimization thread did not stop gracefully")
        
        self.logger.info("Application shutdown complete")


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Process Optimization Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python app.py                                    # Run both API and continuous optimization
    python app.py --mode continuous                 # Run only continuous optimization
    python app.py --mode api --port 8080           # Run only API server on port 8080
    python app.py --mode hybrid --debug            # Run both with debug logging
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['continuous', 'api', 'hybrid'],
        default='hybrid',
        help='Application mode (default: hybrid)'
    )
    
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='API server host (default: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='API server port (default: 5000)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug mode'
    )
    
    args = parser.parse_args()
    
    # Create and start application
    app = ProcessOptimizationApp(
        mode=args.mode,
        api_host=args.host,
        api_port=args.port,
        debug=args.debug
    )
    
    app.start()


if __name__ == "__main__":
    main()
