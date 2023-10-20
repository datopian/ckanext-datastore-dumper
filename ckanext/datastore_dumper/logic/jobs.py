import ckan.plugins.toolkit as toolkit
from ckanext.datastore_dumper.logic.tasks import datastore_upload

from ckan.lib.jobs import DEFAULT_QUEUE_NAME

log = __import__("logging").getLogger(__name__)


def enqueue_datastore_upload(resource_dict, user_dict):
    queue = DEFAULT_QUEUE_NAME
    log.debug(
        "Queuing job datastore upload : {}".format( resource_dict["id"])
    )

    toolkit.enqueue_job(
        datastore_upload,
        [resource_dict, user_dict],
        title="Uploading  {}".format( resource_dict["id"]),
        queue=queue,
    )
