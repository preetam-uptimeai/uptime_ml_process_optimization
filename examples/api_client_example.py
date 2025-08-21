#!/usr/bin/env python3
"""
Example client for the Process Optimization API.
Shows how to use the API endpoints programmatically.
"""

import requests
import json
import yaml
from datetime import datetime


class OptimizationAPIClient:
    """Client for the Process Optimization API."""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        """
        Initialize the API client.
        
        Args:
            base_url: Base URL of the API server
        """
        self.base_url = base_url.rstrip('/')
    
    def health_check(self) -> dict:
        """Check API health status."""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": str(e)}
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics."""
        try:
            response = requests.get(f"{self.base_url}/cache/stats", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": str(e)}
    
    def clear_cache(self) -> dict:
        """Clear all caches."""
        try:
            response = requests.post(f"{self.base_url}/cache/clear", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": str(e)}
    
    def optimize(self, input_data: dict, config: dict, timeout: int = 60) -> dict:
        """
        Run optimization with input data and config.
        
        Args:
            input_data: Dictionary of variable values
            config: Configuration dictionary
            timeout: Request timeout in seconds
            
        Returns:
            Optimization results
        """
        try:
            request_data = {
                "input_data": input_data,
                "config": config
            }
            
            response = requests.post(
                f"{self.base_url}/optimize",
                json=request_data,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            return {"status": "error", "error": str(e)}


def load_config_from_file(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config from {config_path}: {e}")
        return {}


def create_example_input_data() -> dict:
    """Create example input data for demonstration."""
    return {
        # Operative Variables (these will be optimized)
        "Kiln_Feed_SFF_1_Feed_rate": 85.0,
        "Kiln_Feed_SFF_2_Feed_rate": 87.0,
        "Kiln_Coal_PV": 8.5,
        "Calciner_temperature_PV": 875.0,
        "PH_Fan_Speed_PV": 1200.0,
        "Kiln_Drive_Speed_PV": 3.2,
        "Under_grate_Average_Pressure": 4.5,
        "Cooler_Fan_3_Flow": 45000.0,
        "Cooler_Fan_4_Flow": 47000.0,
        "Cooler_Fan_5_Flow": 46000.0,
        "Cooler_Fan_6_Flow": 48000.0,
        "Cooler_Fan_7_Flow": 35000.0,
        "Cooler_Fan_8_Flow": 36000.0,
        "Cooler_Fan_9_Flow": 34000.0,
        "Cooler_Fan_A_Flow": 12000.0,
        "Cooler_Fan_2_Flow": 13000.0,
        
        # Informative Variables (current plant state)
        "Kiln_Inlet_NOX": 0.35,
        "Kiln_Drive_Current": 520.0,
        "Clinker_temperature": 95.0,
        "Calciner_outlet_CO": 0.25
    }


def print_optimization_results(result: dict):
    """Print optimization results in a formatted way."""
    if result.get('status') != 'success':
        print(f"ERROR: Optimization failed: {result.get('error', 'Unknown error')}")
        return
    
    print("Optimization completed successfully!")
    print(f"Timestamp: {result.get('timestamp')}")
    
    # Summary
    summary = result.get('summary', {})
    print(f"\nSummary:")
    print(f"   Optimized variables: {summary.get('total_optimized_variables', 0)}")
    print(f"   Predicted variables: {summary.get('total_predicted_variables', 0)}")
    print(f"   Constraints: {summary.get('total_constraints', 0)}")
    
    # Cost function
    cost_value = result.get('cost_function_value')
    if cost_value is not None:
        print(f"   Cost function value: {cost_value:.4f}")
    
    # Optimized variables
    optimized_vars = result.get('optimized_variables', {})
    if optimized_vars:
        print(f"\nOptimized Variables:")
        for var_name, var_data in optimized_vars.items():
            current = var_data.get('current_value')
            optimized = var_data.get('optimized_value')
            units = var_data.get('units', '')
            change = optimized - current if current is not None else 0
            change_str = f"({change:+.2f})" if change != 0 else "(no change)"
            print(f"   {var_name}: {current} → {optimized} {units} {change_str}")
    
    # Predicted variables
    predicted_vars = result.get('predicted_variables', {})
    if predicted_vars:
        print(f"\nFinal: Predicted Variables:")
        for var_name, var_data in predicted_vars.items():
            value = var_data.get('predicted_value')
            units = var_data.get('units', '')
            print(f"   {var_name}: {value} {units}")
    
    # Constraints
    constraint_vars = result.get('constraint_variables', {})
    if constraint_vars:
        print(f"\n⚖️ Constraints:")
        for var_name, var_data in constraint_vars.items():
            value = var_data.get('constraint_value')
            status = "OK" if value >= 0.8 else "WARNING" if value >= 0.5 else "VIOLATED"
            print(f"   {var_name}: {value:.3f} ({status})")


def main():
    """Main example function."""
    print("Process Optimization API Client Example")
    print("=" * 60)
    
    # Create API client
    client = OptimizationAPIClient("http://localhost:5000")
    
    # 1. Check API health
    print("1️⃣ Checking API health...")
    health = client.health_check()
    if health.get('status') == 'healthy':
        print("   API is healthy")
        cache_stats = health.get('cache_stats', {})
        print(f"   Summary: Cached items: {cache_stats.get('total_cached_items', 0)}")
    else:
        print(f"   ERROR: API health check failed: {health.get('error', 'Unknown error')}")
        print("   Make sure the API server is running:")
        print("   python api_server.py")
        return
    
    print()
    
    # 2. Get cache stats
    print("2️⃣ Getting cache statistics...")
    cache_stats = client.get_cache_stats()
    if cache_stats.get('status') == 'success':
        print("   Cache stats retrieved")
        version = cache_stats.get('current_config_version', 'Not set')
        print(f"   Optimized Variables: Config version: {version}")
    else:
        print(f"   WARNING: Failed to get cache stats: {cache_stats.get('error', 'Unknown error')}")
    
    print()
    
    # 3. Load configuration
    print("3️⃣ Loading configuration...")
    config = load_config_from_file()
    if config:
        print("   Configuration loaded successfully")
        print(f"   📋 Variables: {len(config.get('variables', {}))}")
        print(f"   🛠️ Skills: {len(config.get('skills', {}))}")
        print(f"   📝 Tasks: {len(config.get('tasks', []))}")
    else:
        print("   ERROR: Failed to load configuration")
        return
    
    print()
    
    # 4. Create input data
    print("4️⃣ Creating input data...")
    input_data = create_example_input_data()
    print(f"   Created input data with {len(input_data)} variables")
    
    # Show a few sample variables
    print("   Summary: Sample input variables:")
    count = 0
    for var_name, value in input_data.items():
        if count < 3:
            print(f"      {var_name}: {value}")
            count += 1
    if len(input_data) > 3:
        print(f"      ... and {len(input_data) - 3} more")
    
    print()
    
    # 5. Run optimization
    print("5. Running optimization...")
    print("   This may take a moment...")
    
    result = client.optimize(input_data, config)
    print_optimization_results(result)
    
    print()
    
    # 6. Example of getting updated cache stats
    print("6️⃣ Checking cache stats after optimization...")
    final_stats = client.get_cache_stats()
    if final_stats.get('status') == 'success':
        cache_details = final_stats.get('cache_details', {})
        total_items = sum(
            stats.get('active_items', 0) 
            for stats in cache_details.values()
        )
        print(f"   Summary: Total cached items: {total_items}")
    
    print()
    print("Example completed!")
    print("\n💡 Next steps:")
    print("   - Modify input_data with your actual plant values")
    print("   - Customize config for your specific process")
    print("   - Integrate this client into your application")
    print("   - Monitor optimization results and constraints")


if __name__ == "__main__":
    main()
