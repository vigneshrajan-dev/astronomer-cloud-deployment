from airflow.models.baseoperator import BaseOperator

class GreetingOperator(BaseOperator):
    """
    A simple custom operator that logs a greeting message.

    :param name: The name to greet.
    :type name: str
    """

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        """
        The main logic of the operator. It logs the greeting message.
        """
        self.log.info(f"Hello, {self.name}! 👋")
        self.log.info("This greeting comes from a custom operator.")
        return f"Successfully greeted {self.name}."