import flask
from flask import Blueprint, make_response
from ckanext.datastore.controller import dump_to
from flask.views import MethodView
import ckan.lib.uploader as uploader

from ckan.plugins.toolkit import (
    Invalid,
    ObjectNotFound,
    get_validator,
    _,
    request,
    config,
    abort,
    get_action,
    redirect_to,
)

int_validator = get_validator("int_validator")
boolean_validator = get_validator("boolean_validator")

DUMP_FORMATS = "csv", "tsv", "json", "xml"

datastore_uploader = Blueprint("datastore_uploader", __name__)


log = __import__("logging").getLogger(__name__)


class DatastoreModifiedController(MethodView):
    def get(self, resource_id):
        # Check if datastore_upload ask is completed
        context = {"ignore_auth": True}
        task_status = get_action("task_status_show")(
            context,
            {
                "entity_id": resource_id,
                "entity_type": "resource",
                "task_type": "datastore_upload",
                "key": "datastore_upload",
            },
        )
        resource = get_action("resource_show")(context, {"id": resource_id})

        if task_status.get("state", False) == "completed":
            ## check if s3filestore is in plugin list
            if "s3filestore" in config.get("ckan.plugins"):
                try:
                    upload = uploader.get_resource_uploader(resource)
                    bucket_name = config.get("ckanext.s3filestore.aws_bucket_name")
                    region = config.get("ckanext.s3filestore.region_name")
                    host_name = config.get("ckanext.s3filestore.host_name")
                    bucket = upload.get_s3_bucket(bucket_name)
                    filename = resource_id + ".csv"
                    key_path = upload.get_path(resource_id, filename)
                    s3 = upload.get_s3_session()
                    client = s3.client(service_name="s3", endpoint_url=host_name)
                    url = client.generate_presigned_url(
                        ClientMethod="get_object",
                        Params={"Bucket": bucket.name, "Key": key_path},
                        ExpiresIn=60,
                    )
                    redirect_to(url)
                except Exception as e:
                    pass
            else:
                upload = uploader.get_resource_uploader(resource)
                filepath = upload.get_path(resource_id)
                resp = flask.send_file(filepath, as_attachment=True)
                if resource.get("mimetype"):
                    resp.headers["Content-Type"] = resource["mimetype"]
                return resp

        try:
            offset = int_validator(request.args.get("offset", 0), {})
        except Invalid as e:
            abort(400, "offset: " + e.error)
        try:
            limit = int_validator(request.args.get("limit"), {})
        except Invalid as e:
            abort(400, "limit: " + e.error)

        bom = boolean_validator(request.args.get("bom"), {})
        fmt = request.args.get("format", "csv")
        response = make_response()
        response.headers["content-type"] = "application/octet-stream"

        if fmt not in DUMP_FORMATS:
            abort(400, _("format: must be one of %s") % ", ".join(DUMP_FORMATS))
        try:
            dump_to(
                resource_id,
                response.stream,
                fmt=fmt,
                offset=offset,
                limit=limit,
                options={"bom": bom},
            )
        except ObjectNotFound:
            abort(404, _("DataStore resource not found"))
        return response


datastore_uploader.add_url_rule(
    "/datastore/dump/<resource_id>",
    view_func=DatastoreModifiedController.as_view(str("dump")),
)


def get_blueprints():
    return [datastore_uploader]
