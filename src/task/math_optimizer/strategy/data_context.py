from.variable import Variable

class DataContext:
    """
    A transient, in-memory container holding the state of all Variable
    objects for a single execution cycle.
    """
    def __init__(self, variables_config):
        self._variables = {
            var_id: Variable(var_id, config)
            for var_id, config in variables_config.items()
        }

    def get_variable(self, var_id):
        if var_id not in self._variables:
            raise KeyError(f"Variable '{var_id}' not found in DataContext.")
        return self._variables[var_id]

    def has_variable(self, var_id):
        return var_id in self._variables

    def populate_initial_data(self, data):
        """Populates the context with live data at the start of a cycle."""
        for var_id, value in data.items():
            if self.has_variable(var_id):
                self.get_variable(var_id).set_initial_value(value)
        
        # Initialize all other variables with default values to prevent None errors
        for var_id, variable in self._variables.items():
            if variable.current_value is None:
                # Set default values based on variable type
                if variable.var_type in ['Delta', 'Predicted', 'Constraint', 'CalculatedKPI']:
                    variable.current_value = 0.0
                    variable.dof_value = 0.0
                    variable.recommended_value = 0.0
                elif variable.var_type == 'Calculated':
                    # For calculated variables, we'll set them after pre-calculation
                    variable.current_value = 0.0
                    variable.dof_value = 0.0
                    variable.recommended_value = 0.0

    def get_all_variables(self):
        return self._variables