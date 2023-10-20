import flask
from flask import Blueprint, make_response
from flask.views import MethodView

import ckan.lib.uploader as uploader
import ckan.model as model
from ckan.plugins.toolkit import (
    c,
    ObjectNotFound,
    NotAuthorized,
    get_validator,
    config,
    _,
    request,
    abort,
    get_action,
    redirect_to
)

try:
    from ckanext.datastore.controller import dump_to
except ImportError:
    from ckanext.datastore.blueprint import dump_to

import ckan.lib.navl.dictization_functions as dict_fns
from ckanext.datastore.logic.schema import (
    list_of_strings_or_string,
    json_validator,
    unicode_or_json_validator,
)

int_validator = get_validator("int_validator")
boolean_validator = get_validator("boolean_validator")
ignore_missing = get_validator("ignore_missing")
one_of = get_validator("one_of")
default = get_validator("default")
unicode_only = get_validator("unicode_only")

DUMP_FORMATS = "csv", "tsv", "json", "xml"

datastore_dumper = Blueprint("datastore_dumper", __name__)

log = __import__("logging").getLogger(__name__)


def dump_schema():
    return {
        "offset": [default(0), int_validator],
        "limit": [ignore_missing, int_validator],
        "format": [default("csv"), one_of(DUMP_FORMATS)],
        "bom": [default(False), boolean_validator],
        "filters": [ignore_missing, json_validator],
        "q": [ignore_missing, unicode_or_json_validator],
        "distinct": [ignore_missing, boolean_validator],
        "plain": [ignore_missing, boolean_validator],
        "language": [ignore_missing, unicode_only],
        "fields": [ignore_missing, list_of_strings_or_string],
        "sort": [default("_id"), list_of_strings_or_string],
    }

class DatastoreModifiedController(MethodView):
    def get(self, resource_id):
        data, errors = dict_fns.validate(request.args.to_dict(), dump_schema())
        if errors:
            abort(
                400,
                "\n".join("{0}: {1}".format(k, " ".join(e)) for k, e in errors.items()),
            )

        context = {
            "model": model,
            "session": model.Session,
            "user": c.user,
            "auth_user_obj": c.userobj,
        }

        try:
            resource = get_action("resource_show")(context, {"id": resource_id})
        except NotAuthorized:
            return abort(404, _("Not authorized to read resource %s") % resource_id)
        except ObjectNotFound:
            return abort(404, _("Resource %s not found") % resource_id)

        task_status = get_action("task_status_show")(
            context,
            {
                "entity_id": resource_id,
                "entity_type": "resource",
                "task_type": "datastore_upload",
                "key": "datastore_upload",
            },
        )

        if (
            task_status.get("state", False) == "completed"
            and resource.get("url_type") == "datastore" and data["format"]
        ):
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
                    return redirect_to(url)
                except Exception as e:
                    log.info("Failed to get file from s3 filestore: {}".format(e))
                    pass
            else:
                filename = resource_id + ".csv"
                upload = uploader.get_resource_uploader(resource)
                filepath = upload.get_path(resource_id)
                resp = flask.send_file(
                    filepath, attachment_filename=filename, as_attachment=True
                )
                if resource.get("mimetype"):
                    resp.headers["Content-Type"] = resource["mimetype"]
                return resp

        response = make_response()

        content_type = {
            "csv": "text/csv; charset=utf-8",
            "tsv": "text/tab-separated-values; charset=utf-8",
            "json": "application/json; charset=utf-8",
            "xml": "text/xml; charset=utf-8",
        }

        try:
            try:
                response.headers["content-type"] = content_type[data["format"]]
                dump_to(
                    resource_id,
                    response.stream,
                    fmt=data["format"],
                    offset=data["offset"],
                    limit=data.get("limit"),
                    options={"bom": data["bom"]},
                )
            except Exception:
                dump_to(
                    resource_id,
                    response,
                    fmt=data["format"],
                    offset=data["offset"],
                    limit=data.get("limit"),
                    options={"bom": data["bom"]},
                    sort=data["sort"],
                    search_params={
                        k: v
                        for k, v in data.items()
                        if k
                        in ["filters", "q", "distinct", "plain", "language", "fields"]
                    },
                )
        except ObjectNotFound:
            abort(404, _("DataStore resource not found"))
        return response


datastore_dumper.add_url_rule(
    "/datastore/dump/<resource_id>",
    view_func=DatastoreModifiedController.as_view(str("dump")),
)


def get_blueprints():
    return [datastore_dumper]
