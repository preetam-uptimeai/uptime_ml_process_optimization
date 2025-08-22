from .base import Skill

class Constraint(Skill):
    """
    A skill that evaluates how well a variable stays within its operational limits.
    Returns a score between 0 (violation) and 1 (perfect).
    """
    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config['config']
        self.var_min = cfg.get('var_min', float('-inf'))
        self.var_max = cfg.get('var_max', float('inf'))
        self.op_min = cfg.get('op_min', self.var_min)
        self.op_max = cfg.get('op_max', self.var_max)

    def execute(self, context):
        # Get the input variable value
        input_var = context.get_variable(self.inputs[0])
        value = input_var.dof_value
        
        # Handle None values
        if value is None:
            print(f"Warning: Constraint {self.name} received None value for {self.inputs[0]}")
            value = 0.0  # Default fallback

        def calculate_constraint(value, op_min, op_max, phys_min, phys_max):
            # Handle cases where operating limits match physical limits
            if op_min == phys_min:
                if value < op_min:
                    return 0.0
            if op_max == phys_max:
                if value > op_max:
                    return 0.0
                    
            # Normal case when limits don't match
            if op_min <= value <= op_max:
                return 1.0
            elif value < op_min and op_min != phys_min:
                return (value - phys_min) / (op_min - phys_min)
            elif value > op_max and op_max != phys_max:
                return (phys_max - value) / (phys_max - op_max)
            else:
                return 0.0  # Fallback for any edge cases

        # Calculate constraint using the function
        score = calculate_constraint(value, self.op_min, self.op_max, self.var_min, self.var_max)
        # print(f"Constraint score for {self.inputs[0]}: {score}")
        # Set the output
        output_var = context.get_variable(self.outputs[0])
        output_var.dof_value = score