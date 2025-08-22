from .base import Skill

class CompositionSkill(Skill):
    """
    A skill that executes a sequence of other skills in order.
    """
    def __init__(self, name, config):
        super().__init__(name, config)
        self.skill_sequence_names = config['config'].get('skill_sequence', [])
        self.skill_sequence = []  # Will be populated by resolve_skills

    def resolve_skills(self, skill_registry):
        """
        Resolves skill names to actual skill objects.
        Called after all skills are instantiated.
        """
        self.skill_sequence = []
        for skill_name in self.skill_sequence_names:
            if skill_name not in skill_registry:
                raise ValueError(f"Skill '{skill_name}' not found in registry")
            self.skill_sequence.append(skill_registry[skill_name])

    def execute(self, context):
        """
        Executes each skill in the sequence in order.
        """
        for skill in self.skill_sequence:
            skill.execute(context)