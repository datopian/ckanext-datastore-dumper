import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckanext.datastore_uploader.logic import action, auth
import ckanext.datastore_uploader.views as views


class DatastoreUploaderPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "datastore_uploader")

    # IActions
    def get_actions(self):
        return action.get_actions()

    # IAuthFunctions
    def get_auth_functions(self):
        return auth.get_auth_functions()

    # IBlueprint
    def get_blueprint(self):
        return views.get_blueprints()
