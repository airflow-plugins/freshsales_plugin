from airflow.plugins_manager import AirflowPlugin

from freshsales_plugin.hooks.freshsales_hook import FreshSalesHook
from freshsales_plugin.operators.freshsales_to_s3_operator import FreshsalesToS3Operator


class freshsales_plugin(AirflowPlugin):
    name = "freshsales_plugin"
    operators = [FreshsalesToS3Operator]
    hooks = [FreshSalesHook]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
