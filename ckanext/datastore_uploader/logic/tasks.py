import ckan.plugins.toolkit as tk
import ckan.lib.uploader as uploader
from werkzeug.datastructures import FileStorage
import urlparse


import requests
from io import BytesIO

log = __import__("logging").getLogger(__name__)


def datastore_upload(resource_dict):
    site_url = tk.config.get("ckan.site_url", "")
    url = urlparse.urljoin(site_url, "/datastore/dump" + "/" + resource_dict["id"])
    response = requests.get(url, stream=True, params={"bom": True})
    file_storage = FileStorage(
        stream=BytesIO(response.content), filename="{}.csv".format(resource_dict["id"])
    )
    data_dict = {
        "resource_id": resource_dict["id"],
        "upload": file_storage,
        "url": "{}.csv".format(resource_dict["id"]),
    }
    upload = uploader.get_resource_uploader(data_dict)
    upload.upload(resource_dict["id"], uploader.get_max_resource_size())

    context = {"ignore_auth": True}

    tk.get_action("task_status_update")(
        context,
        {
            "entity_id": resource_dict["id"],
            "entity_type": "resource",
            "task_type": "datastore_upload",
            "key": "datastore_upload",
            "state": "completed",
        },
    )
