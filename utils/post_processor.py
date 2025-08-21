"""Utility functions for post-processing optimization results."""
from typing import Any
from rto.data_context import DataContext

def post_process_optimization_result(final_context: DataContext, strategy: Any) -> None:
    """Process and display the optimization results in a structured format.
    
    Args:
        final_context: The final data context after optimization
        strategy: The optimization strategy instance
    """
    # Print predicted outcomes
    print("\nKey Predicted Outcomes:")
    predicted_vars = strategy.get_predicted_variable_ids()
    for var_id in predicted_vars:
        if final_context.has_variable(var_id):
            value = final_context.get_variable(var_id).dof_value
            print(f"  - {var_id}: {value:.4f}")

    # Get fixed input variables
    fixed_input_vars = strategy.get_fixed_input_variable_ids()
        
    # Print operative variables
    print("\nOperative Variables (remaining operative/optimizable):")
    remaining_operative_vars = [var_id for var_id in strategy.get_operative_variable_ids() 
                              if var_id not in fixed_input_vars]
    for var_id in remaining_operative_vars:
        if final_context.has_variable(var_id):
            variable = final_context.get_variable(var_id)
            print(f"  - {var_id}:")
            print(f"    - Initial: {variable.current_value:.2f}")
            print(f"    - Recommended: {variable.recommended_value:.2f}")
            print(f"    - Change: {variable.recommended_value - variable.current_value:+.2f}")
    
    # Print calculated variables
    print("\nCalculated Variables (now operative/optimizable):")
    calculated_vars = strategy.get_calculated_variable_ids()
    for var_id in calculated_vars:
        if final_context.has_variable(var_id):
            variable = final_context.get_variable(var_id)
            print(f"  - {var_id}:")
            print(f"    - Initial: {variable.current_value:.2f}")
            print(f"    - Recommended: {variable.recommended_value:.2f}")
            print(f"    - Change: {variable.recommended_value - variable.current_value:+.2f}")