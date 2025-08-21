"""
Optimization Service - Handles continuous optimization cycles.
"""

import time
import threading
import logging
from datetime import datetime
from typing import Optional

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from rto.strategy import OptimizationStrategy
from utils import ConfigManager, DatabaseManager, post_process_optimization_result, get_cache_manager


class OptimizationService:
    """Service for running continuous optimization cycles."""
    
    def __init__(self, shutdown_event: threading.Event):
        """
        Initialize the optimization service.
        
        Args:
            shutdown_event: Event to signal shutdown
        """
        self.shutdown_event = shutdown_event
        self.logger = logging.getLogger("process_optimization.optimization_service")
        self.config_manager = ConfigManager()
        self.cache_manager = get_cache_manager()
        self.cycle_count = 0
        
    def run_single_cycle(self) -> bool:
        """
        Run a single optimization cycle.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.cycle_count += 1
            self.logger.info(f"Starting optimization cycle #{self.cycle_count}")
            
            # Load strategy from MinIO
            strategy = OptimizationStrategy(use_minio=True)
            self.logger.debug(f"Model outputs: {strategy.get_predicted_variable_ids()}")
            self.logger.info("Strategy loaded successfully from MinIO")

            # Get required variables
            operative_vars = strategy.get_operative_variable_ids()
            informative_vars = strategy.get_informative_variable_ids()
            calculated_vars = strategy.get_calculated_variable_ids()
            required_vars = operative_vars + informative_vars

            # Get last run timestamp
            last_timestamp = self.config_manager.get_last_run_timestamp()
            if last_timestamp:
                self.logger.debug(f"Last run timestamp from config: {last_timestamp}")

            # Get latest data from database
            db = DatabaseManager()
            result = db.get_latest_data(required_vars, last_timestamp)
            last_timestamp = result['timestamp']
            self.logger.info(f"Running optimization cycle for timestamp: {last_timestamp}")
            latest_data = result['data']

            # Check for missing variables
            missing_vars = []
            for var in required_vars:
                if latest_data.get(var) is None:
                    missing_vars.append(var)
                    self.logger.warning(f"Warning: {var} has None values - dof: None, current: None")

            if missing_vars:
                self.logger.error(f"Missing variables: {missing_vars}")
                return False

            # Run optimization
            self.logger.info("Running optimizer...")
            final_context = strategy.run_cycle(latest_data)

            # Post-process optimization results
            post_process_optimization_result(final_context, strategy)

            # Update last run timestamp in config
            self.config_manager.update_last_run_timestamp(last_timestamp)
            
            self.logger.info(f"Cycle #{self.cycle_count} completed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in optimization cycle #{self.cycle_count}: {str(e)}")
            return False
    
    def run_continuous(self):
        """Run continuous optimization cycles until shutdown is requested."""
        self.logger.info("Starting continuous optimization with in-memory caching")
        self.logger.info("Cache statistics will be shown every 10 cycles")
        
        try:
            while not self.shutdown_event.is_set():
                # Run optimization cycle
                success = self.run_single_cycle()
                
                # Show cache statistics every 10 cycles
                if self.cycle_count % 10 == 0:
                    self._show_cache_statistics()
                
                if success:
                    self.logger.info(f"Cycle #{self.cycle_count} completed. Next cycle in 1 minute...")
                else:
                    self.logger.warning(f"Cycle #{self.cycle_count} failed. Retrying in 1 minute...")
                
                # Wait for 1 minute or until shutdown
                if self.shutdown_event.wait(timeout=60):
                    break
                    
        except Exception as e:
            self.logger.error(f"Critical error in continuous optimization: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            self._show_final_statistics()
    
    def _show_cache_statistics(self):
        """Show cache statistics."""
        try:
            self.logger.info(f"Cache Statistics (Cycle #{self.cycle_count}):")
            stats = self.cache_manager.get_cache_stats()
            
            # Show current config version
            current_version = stats.get('current_config_version')
            if current_version:
                self.logger.info(f"  Current config version: {current_version}")
            else:
                self.logger.info(f"  Current config version: Not set")
            
            # Show cached timestamp
            cached_timestamp = stats.get('cached_last_run_timestamp')
            if cached_timestamp:
                self.logger.info(f"  Cached last run timestamp: {cached_timestamp}")
            else:
                self.logger.info(f"  Cached last run timestamp: Not set")
            
            # Show cache stats for each type
            for cache_type, cache_stats in stats.items():
                if cache_type not in ['current_config_version', 'cached_last_run_timestamp']:
                    active = cache_stats.get('active_items', 0)
                    expired = cache_stats.get('expired_items', 0)
                    self.logger.info(f"  {cache_type}: {active} items active, {expired} expired")
            
            # Clean up expired temp files periodically
            self.logger.info("Cleaning up expired temporary files...")
            self.cache_manager.cleanup_expired_temp_files()
            
        except Exception as e:
            self.logger.error(f"Error showing cache statistics: {e}")
    
    def _show_final_statistics(self):
        """Show final statistics when shutting down."""
        try:
            self.logger.info(f"Optimization stopped after {self.cycle_count} cycles")
            self.logger.info("Final cache statistics:")
            stats = self.cache_manager.get_cache_stats()
            
            current_version = stats.get('current_config_version')
            if current_version:
                self.logger.info(f"  Config version: {current_version}")
            
            cached_timestamp = stats.get('cached_last_run_timestamp')
            if cached_timestamp:
                self.logger.info(f"  Last run timestamp: {cached_timestamp}")
            
            for cache_type, cache_stats in stats.items():
                if cache_type not in ['current_config_version', 'cached_last_run_timestamp']:
                    active = cache_stats.get('active_items', 0)
                    self.logger.info(f"  {cache_type}: {active} items")
                    
        except Exception as e:
            self.logger.error(f"Error showing final statistics: {e}")
