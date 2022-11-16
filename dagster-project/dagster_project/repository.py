from dagster import job, repository, load_assets_from_modules, with_resources

from dagster_project.ops import hello
from demo_integration.ops import project_op
from demo_integration.resources import my_resource
from demo_integration import assets


@job
def hello_job():
    hello.say_hello()


API_KEY = "abc123"
PROJ_ID = "i-love-uuids"

configured_resource = my_resource.configured({"api_key": API_KEY})
op_config = {"ops": {"project_op": {"config": {"project_id": PROJ_ID}}}}


@job(
    resource_defs={"my_resource": configured_resource},
    config=op_config,
)
def project_job():
    project_op()


project_assets = with_resources(
    load_assets_from_modules([assets]),
    {"my_resource": configured_resource},
)


@repository
def dagster_project():
    return [hello_job, project_job, project_assets]
