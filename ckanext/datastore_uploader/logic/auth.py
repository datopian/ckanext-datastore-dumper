import ckan.plugins.toolkit as tk


@tk.auth_allow_anonymous_access
def datastore_upload(context, data_dict, privilege="resource_update"):
    if "id" not in data_dict:
        data_dict["id"] = data_dict.get("resource_id")

    user = context.get("user")

    authorized = tk.check_access(privilege, context, data_dict)

    if not authorized:
        return {
            "success": False,
            "msg": tk._(
                "User {0} not authorized to update resource {1}".format(
                    str(user), data_dict["id"]
                )
            ),
        }
    else:
        return {"success": True}


def get_auth_functions():
    return {
        "datastore_upload": datastore_upload,
    }
