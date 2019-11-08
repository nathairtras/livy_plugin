from airflow.plugins_manager import AirflowPlugin
from livy.operators.livy_operator import LivyOperator


class livy_plugin(AirflowPlugin):
    name = "livy_plugin"
    operators = [LivyOperator]
    # Leave in for explicitness
    hooks = []
    flask_blueprints = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []