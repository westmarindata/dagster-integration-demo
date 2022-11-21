from dagster import asset


@asset(required_resource_keys={"my_resource"}, config_schema={"project_id": str})
def project_asset(context):
    project_id = context.op_config["project_id"]
    data = context.resources.my_resource.run_and_poll(project_id=project_id)
    return data
