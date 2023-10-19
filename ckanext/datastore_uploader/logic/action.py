import json
import ckan.plugins.toolkit as tk
import ckan.plugins as plugins
from ckanext.datastore_uploader.logic.jobs import enqueue_datastore_upload

log = __import__("logging").getLogger(__name__)


@plugins.toolkit.chained_action
def datastore_create(up_func, context, data_dict):
    result = up_func(context, data_dict)
    datastore_upload(context, result)
    return result


@plugins.toolkit.chained_action
def datastore_upsert(up_func, context, data_dict):
    result = up_func(context, data_dict)
    datastore_upload(context, result)
    return result


def datastore_upload(context, data_dict):
    """
    Generate a datastore file from a resource and
    upload it to the filestore.
    """
    try:
        tk.check_access("datastore_upload", context, data_dict)
        resource_dict = tk.get_action("resource_show")(
            context, {"id": data_dict["resource_id"]}
        )
        user_dict = tk.get_action("user_show")(context, {"id": context["user"]})

        if resource_dict["url_type"] == "datastore":
            plugins.toolkit.get_action("task_status_update")(
                context,
                {
                    "entity_id": resource_dict["id"],
                    "entity_type": "resource",
                    "task_type": "datastore_upload",
                    "key": "datastore_upload",
                    "value": json.dumps(
                        {"user_name": context.get("user", False), "error": False}
                    ),
                    "state": "pending",
                },
            )
            enqueue_datastore_upload(resource_dict, user_dict)
    except Exception as e:
        log.info("Failed to enqueue datastore upload job: {}".format(e))
        pass


def get_actions():
    return {
        "datastore_create": datastore_create,
        "datastore_upsert": datastore_upsert,
        "datastore_upload": datastore_upload,
    }
