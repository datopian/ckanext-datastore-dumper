try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse
    
import requests
from io import BytesIO
from werkzeug.datastructures import FileStorage

import ckan.model as model
import ckan.plugins.toolkit as tk
import ckan.lib.uploader as uploader


log = __import__("logging").getLogger(__name__)


def datastore_upload(resource_dict, user_dict):
    context = {
        "model": model,
        "session": model.Session,
        "ignore_auth": True,
        "user": user_dict["name"],
    }

    site_url = tk.config.get("ckan.site_url", "")
    url = urlparse.urljoin(site_url, "/datastore/dump" + "/" + resource_dict["id"])

    api_token = user_dict.get("apikey", None)

    if api_token is None:
        api_token = tk.config.get("ckan.datapusher.api_token")

    headers = {
        "Authorization": api_token,
    }
    response = requests.get(url, headers=headers, stream=True, params={"bom": True})
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
