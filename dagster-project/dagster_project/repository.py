from dagster import job, repository

from dagster_project.ops import hello
from demo_integration.ops import project_op
from demo_integration.resources import my_resource


@job
def hello_job():
    hello.say_hello()


API_KEY = "abc123"
PROJ_ID = "i-love-uuids"

configured_resource = my_resource.configured({"api_key": API_KEY})
configured_op = project_op.configured({"project_id": PROJ_ID}, name="run_job")


@job(resource_defs={"my_resource": configured_resource})
def project_job():
    configured_op()


@repository
def dagster_project():
    return [hello_job, project_job]
