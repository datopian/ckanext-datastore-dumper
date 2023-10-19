import ckan.plugins.toolkit as toolkit
from ckanext.datastore_uploader.logic.tasks import datastore_upload

from ckan.lib.jobs import DEFAULT_QUEUE_NAME

log = __import__("logging").getLogger(__name__)


def enqueue_datastore_upload(resource_id, operation):
    queue = DEFAULT_QUEUE_NAME
    log.debug("Queuing job datastore upload : {} {}".format(operation, resource_id))

    toolkit.enqueue_job(
        datastore_upload,
        [resource_id],
        title='Uploading "{}" {}'.format(operation, resource_id),
        queue=queue,
    )
