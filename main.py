import time
from datetime import datetime
from typing import Dict, List, Optional

from rto.strategy import OptimizationStrategy
from utils import ConfigManager, DatabaseManager, post_process_optimization_result

def run_optimization_cycle(config_manager: ConfigManager) -> None:
    """Run a single optimization cycle"""
    try:
        # Load strategy from MinIO
        strategy = OptimizationStrategy(use_minio=True)
        print(f"Model outputs: {strategy.get_predicted_variable_ids()}")
        print("Strategy loaded successfully from MinIO.")

        # Get required variables
        operative_vars = strategy.get_operative_variable_ids()
        informative_vars = strategy.get_informative_variable_ids()
        calculated_vars = strategy.get_calculated_variable_ids()
        required_vars = operative_vars + informative_vars

        # Get last run timestamp
        last_timestamp = config_manager.get_last_run_timestamp()
        if last_timestamp:
            print(f"Last run timestamp from config: {last_timestamp}")

        # Get latest data from database
        db = DatabaseManager()
        result = db.get_latest_data(required_vars, last_timestamp)
        last_timestamp = result['timestamp']
        print("Running optimization cycle for timestamp: ", last_timestamp)
        latest_data = result['data']

        # Check for missing variables
        missing_vars = []
        for var in required_vars:
            if latest_data.get(var) is None:
                missing_vars.append(var)
                print(f"Warning: {var} has None values - dof: None, current: None")

        if missing_vars:
            print(f"\nMissing variables: {missing_vars}")
            return

        # Run optimization
        print("\nRunning optimizer...")
        final_context = strategy.run_cycle(latest_data)

        # Post-process optimization results
        post_process_optimization_result(final_context, strategy)

        # Update last run timestamp in config
        config_manager.update_last_run_timestamp(last_timestamp)

    except Exception as e:
        print(f"Error in optimization cycle: {str(e)}")

def main():
    """Main function to run continuous optimization"""
    config_manager = ConfigManager()

    try:
        while True:
            run_optimization_cycle(config_manager)
            print("\nCycle will run in 1 minute...")
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nOptimization stopped by user")
    except Exception as e:
        print(f"\n❌ Optimization failed: {str(e)}")

if __name__ == "__main__":
    main()