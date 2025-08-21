"""
API Service - Handles REST API endpoints for on-demand optimization.
"""

import logging
import threading
import tempfile
import yaml
import os
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from flask import Flask, request, jsonify
from flask_cors import CORS

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from rto.strategy import OptimizationStrategy
from utils import get_cache_manager, post_process_optimization_result


class APIService:
    """Service for handling REST API requests."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 5000, debug: bool = False):
        """
        Initialize the API service.
        
        Args:
            host: Host to bind to
            port: Port to bind to
            debug: Enable debug mode
        """
        self.host = host
        self.port = port
        self.debug = debug
        self.logger = logging.getLogger("process_optimization.api_service")
        
        # Create Flask app
        self.app = Flask(__name__)
        CORS(self.app)  # Enable CORS for all routes
        
        # Configure Flask logging
        if not debug:
            log = logging.getLogger('werkzeug')
            log.setLevel(logging.WARNING)
        
        # Register routes
        self._register_routes()
        
        # Server instance
        self.server_thread: Optional[threading.Thread] = None
        
    def _register_routes(self):
        """Register all API routes."""
        self.app.add_url_rule('/health', 'health_check', self._health_check, methods=['GET'])
        self.app.add_url_rule('/optimize', 'run_optimization', self._run_optimization, methods=['POST'])
        self.app.add_url_rule('/cache/stats', 'get_cache_stats', self._get_cache_stats, methods=['GET'])
        self.app.add_url_rule('/cache/clear', 'clear_cache', self._clear_cache, methods=['POST'])
        
        # Error handlers
        self.app.errorhandler(404)(self._not_found)
        self.app.errorhandler(500)(self._internal_error)
    
    def _validate_request_data(self, data: Dict[str, Any]) -> tuple[bool, str]:
        """
        Validate the request data structure.
        
        Args:
            data: Request JSON data
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(data, dict):
            return False, "Request body must be a JSON object"
        
        # Check required fields
        required_fields = ['input_data', 'config']
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"
        
        # Validate input_data structure
        input_data = data.get('input_data')
        if not isinstance(input_data, dict):
            return False, "input_data must be a JSON object"
        
        # Validate config structure
        config = data.get('config')
        if not isinstance(config, dict):
            return False, "config must be a JSON object"
        
        # Check if config has required sections
        required_config_sections = ['variables', 'skills', 'tasks']
        for section in required_config_sections:
            if section not in config:
                return False, f"Missing required config section: {section}"
        
        return True, ""
    
    def _run_single_optimization_cycle(self, input_data: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run a single optimization cycle with provided input data and config.
        
        Args:
            input_data: Dictionary containing variable values
            config: Configuration dictionary with variables, skills, and tasks
            
        Returns:
            Dictionary containing optimization results
        """
        try:
            self.logger.info("Starting single optimization cycle via API")
            
            # Create temporary config file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_config:
                yaml.dump(config, temp_config)
                temp_config_path = temp_config.name
            
            try:
                # Create strategy with the provided config
                strategy = OptimizationStrategy(config_path=temp_config_path, use_minio=False)
                self.logger.info(f"Strategy loaded with {len(strategy.get_operative_variable_ids())} operative variables")
                
                # Validate input data contains required variables
                operative_vars = strategy.get_operative_variable_ids()
                informative_vars = strategy.get_informative_variable_ids()
                calculated_vars = strategy.get_calculated_variable_ids()
                required_vars = operative_vars + informative_vars
                
                missing_vars = []
                for var in required_vars:
                    if var not in input_data:
                        missing_vars.append(var)
                
                if missing_vars:
                    raise ValueError(f"Missing required variables in input_data: {missing_vars}")
                
                # Run the optimization cycle
                self.logger.info("Running optimization cycle...")
                final_context = strategy.run_cycle(input_data)
                
                # Post-process results
                post_process_optimization_result(final_context, strategy)
                
                # Extract results
                results = self._extract_optimization_results(final_context, strategy)
                
                self.logger.info(f"Optimization cycle completed successfully. Optimized {results['summary']['total_optimized_variables']} variables.")
                return results
                
            finally:
                # Clean up temporary config file
                try:
                    os.unlink(temp_config_path)
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary config file: {e}")
            
        except Exception as e:
            self.logger.error(f"Optimization cycle failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            return {
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e),
                'error_type': type(e).__name__
            }
    
    def _extract_optimization_results(self, final_context, strategy) -> Dict[str, Any]:
        """Extract and format optimization results."""
        # Get optimized variable values
        optimized_vars = {}
        for var_id in strategy.get_optimizable_variable_ids():
            var = final_context.get_variable(var_id)
            if var.dof_value is not None:
                optimized_vars[var_id] = {
                    'current_value': var.current_value,
                    'optimized_value': var.dof_value,
                    'units': strategy.variables_config.get(var_id, {}).get('units', ''),
                    'threshold': strategy.variables_config.get(var_id, {}).get('threshold', None)
                }
        
        # Get predicted values
        predicted_vars = {}
        for var_id in strategy.get_predicted_variable_ids():
            var = final_context.get_variable(var_id)
            if var.dof_value is not None:
                predicted_vars[var_id] = {
                    'predicted_value': var.dof_value,
                    'units': strategy.variables_config.get(var_id, {}).get('units', ''),
                    'type': strategy.variables_config.get(var_id, {}).get('type', '')
                }
        
        # Get constraint values
        constraint_vars = {}
        for var_id in strategy.get_constraint_variable_ids():
            var = final_context.get_variable(var_id)
            if var.dof_value is not None:
                constraint_vars[var_id] = {
                    'constraint_value': var.dof_value,
                    'units': strategy.variables_config.get(var_id, {}).get('units', '')
                }
        
        # Get cost function value if available
        cost_function_value = None
        cost_var = final_context.get_variable('cost_function_total')
        if cost_var and cost_var.dof_value is not None:
            cost_function_value = cost_var.dof_value
        
        return {
            'status': 'success',
            'timestamp': datetime.now().isoformat(),
            'optimized_variables': optimized_vars,
            'predicted_variables': predicted_vars,
            'constraint_variables': constraint_vars,
            'cost_function_value': cost_function_value,
            'summary': {
                'total_optimized_variables': len(optimized_vars),
                'total_predicted_variables': len(predicted_vars),
                'total_constraints': len(constraint_vars)
            }
        }
    
    def _health_check(self):
        """Health check endpoint."""
        cache_manager = get_cache_manager()
        cache_stats = cache_manager.get_cache_stats()
        
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'cache_stats': {
                'config_version': cache_stats.get('current_config_version'),
                'cached_timestamp': cache_stats.get('cached_last_run_timestamp'),
                'total_cached_items': sum(
                    stats['active_items'] for stats in cache_stats.values() 
                    if isinstance(stats, dict) and 'active_items' in stats
                )
            }
        })
    
    def _run_optimization(self):
        """Run optimization endpoint."""
        try:
            # Validate request
            if not request.is_json:
                return jsonify({
                    'status': 'error',
                    'timestamp': datetime.now().isoformat(),
                    'error': 'Request must be JSON'
                }), 400
            
            data = request.get_json()
            
            # Validate request data
            is_valid, error_message = self._validate_request_data(data)
            if not is_valid:
                return jsonify({
                    'status': 'error',
                    'timestamp': datetime.now().isoformat(),
                    'error': error_message
                }), 400
            
            # Extract input data and config
            input_data = data['input_data']
            config = data['config']
            
            self.logger.info(f"Received optimization request with {len(input_data)} input variables")
            
            # Run optimization
            result = self._run_single_optimization_cycle(input_data, config)
            
            # Return appropriate status code
            status_code = 200 if result['status'] == 'success' else 500
            
            return jsonify(result), status_code
            
        except Exception as e:
            self.logger.error(f"API endpoint error: {str(e)}")
            self.logger.error(traceback.format_exc())
            
            return jsonify({
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': 'Internal server error',
                'error_details': str(e)
            }), 500
    
    def _get_cache_stats(self):
        """Get current cache statistics."""
        try:
            cache_manager = get_cache_manager()
            stats = cache_manager.get_cache_stats()
            
            # Format stats for API response
            formatted_stats = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'current_config_version': stats.get('current_config_version'),
                'cached_last_run_timestamp': stats.get('cached_last_run_timestamp'),
                'cache_details': {}
            }
            
            # Add cache details
            for cache_type, cache_stats in stats.items():
                if isinstance(cache_stats, dict) and 'active_items' in cache_stats:
                    formatted_stats['cache_details'][cache_type] = {
                        'active_items': cache_stats['active_items'],
                        'expired_items': cache_stats['expired_items'],
                        'total_items': cache_stats['total_items']
                    }
            
            return jsonify(formatted_stats)
            
        except Exception as e:
            self.logger.error(f"Cache stats endpoint error: {str(e)}")
            return jsonify({
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }), 500
    
    def _clear_cache(self):
        """Clear all caches."""
        try:
            cache_manager = get_cache_manager()
            cache_manager.clear_all_caches()
            
            return jsonify({
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'message': 'All caches cleared successfully'
            })
            
        except Exception as e:
            self.logger.error(f"Cache clear endpoint error: {str(e)}")
            return jsonify({
                'status': 'error',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }), 500
    
    def _not_found(self, error):
        """Handle 404 errors."""
        return jsonify({
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': 'Endpoint not found'
        }), 404
    
    def _internal_error(self, error):
        """Handle 500 errors."""
        return jsonify({
            'status': 'error',
            'timestamp': datetime.now().isoformat(),
            'error': 'Internal server error'
        }), 500
    
    def start(self):
        """Start the API service."""
        self.logger.info(f"Starting API service on {self.host}:{self.port}")
        self.logger.info("Available endpoints:")
        self.logger.info("   GET  /health          - Health check")
        self.logger.info("   POST /optimize        - Run optimization cycle")
        self.logger.info("   GET  /cache/stats     - Get cache statistics")
        self.logger.info("   POST /cache/clear     - Clear all caches")
        
        try:
            self.app.run(host=self.host, port=self.port, debug=self.debug, use_reloader=False)
        except Exception as e:
            self.logger.error(f"Failed to start API service: {e}")
            raise
    
    def stop(self):
        """Stop the API service."""
        # Flask's development server doesn't have a clean shutdown method
        # In production, this would be handled by the WSGI server
        self.logger.info("API service stop requested")
