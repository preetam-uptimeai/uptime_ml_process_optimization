from .base import Skill
from scipy.optimize import minimize
import numpy as np

class OptimizationSkill(Skill):
    """
    A skill that performs numerical optimization using scipy's minimize function.
    """
    def __init__(self, name, config):
        super().__init__(name, config)
        self.cost_skill_name = config['config']['cost_skill_name']
        self.cost_feature_name = config['config']['cost_feature_name']
        self.algorithm = config['config'].get('algorithm', 'SLSQP')
        self._strategy = None  # Will be set by strategy.py

    def set_strategy(self, strategy):
        """Set the reference to the parent strategy object."""
        self._strategy = strategy

    def execute(self, context):
        if not self._strategy:
            raise RuntimeError("Strategy reference not set in OptimizationSkill")

        # Get the cost calculation skill
        cost_skill = self._strategy._skills[self.cost_skill_name]
        
        # Get optimizable variables (calculated variables only after pre-calculation)
        optimizable_vars = self._strategy.get_optimizable_variable_ids()
        
        # Filter inputs to only include optimizable variables
        optimizable_inputs = [var_id for var_id in self.inputs if var_id in optimizable_vars]
        
        # Define the objective function for scipy.minimize
        def objective(x):
            # Update the DOF values in the context for optimizable variables
            for var_id, value in zip(optimizable_inputs, x):
                context.get_variable(var_id).dof_value = value
            
            # Run the cost calculation
            cost_skill.execute(context)
            
            # Get the cost value
            cost = context.get_variable(self.cost_feature_name).dof_value
            #print(f"Cost: {cost}")
            return cost

        # Get initial values and bounds for optimizable variables
        x0 = []
        bounds = []
        eps_values = []
        for var_id in optimizable_inputs:
            var = context.get_variable(var_id)
            # Ensure we have a valid current_value
            if var.current_value is None:
                print(f"Warning: {var_id} has None current_value, using 0.0")
                var.current_value = 0.0
                var.dof_value = 0.0
            
            # Check if threshold exists
            if not hasattr(var, 'threshold') or var.threshold is None:
                print(f"Warning: {var_id} has no threshold, using 1.0")
                threshold = 1.0
            else:
                threshold = var.threshold
            
            x0.append(var.current_value)
            min_bound = var.current_value - threshold
            max_bound = var.current_value + threshold
            bounds.append((min_bound, max_bound))
            eps_values.append(0.01 * var.current_value)

        # Run the optimization
        result = minimize(
            objective,
            x0,
            method=self.algorithm,
            bounds=bounds,
            options={
                    'maxiter': 1000,
                    'disp': False,
                    'ftol': 1e-8,
                    'finite_diff_rel_step': None, #use this for auto handling of eps
                    #'eps': eps_values
                }
        )

        # Store the optimal values
        if result.success:
            print("Number of iterations: ", result.nit)
            for var_id, value in zip(optimizable_inputs, result.x):
                var = context.get_variable(var_id)
                var.recommended_value = value
                var.dof_value = value  # Update DOF value to optimal
        else:
            raise RuntimeError(f"Optimization failed: {result.message}")