from dagster import job, repository, load_assets_from_modules, with_resources
from dagster import AssetsDefinition, AssetKey, asset, Field, Noneable

from dagster_project.ops import hello
from demo_integration.ops import project_op
from demo_integration.resources import my_resource
from demo_integration import assets
from demo_integration.consts import DEFAULT_POLL_INTERVAL
from demo_integration.types import MyOutput


API_KEY = "abc123"
PROJ_ID = "i-love-uuids"

my_resource = my_resource.configured({"api_key": API_KEY})
op_config = {"ops": {"project_op": {"config": {"project_id": PROJ_ID}}}}


def make_project_asset(project_id: str) -> AssetsDefinition:
    @asset(
        name=project_id,
        required_resource_keys={"my_resource"},
        config_schema={
            "poll_interval": Field(
                float,
                default_value=DEFAULT_POLL_INTERVAL,
                description="The time (in seconds) that will be waited between successive "
                "polls.",
            ),
            "poll_timeout": Field(
                Noneable(float),
                default_value=None,
                description="The maximum time that will waited before this operation is "
                "timed out. By default, this will never time out.",
            ),
        },
    )
    def project_asset(context) -> MyOutput:
        return context.resources.my_resource.run_and_poll(
            project_id=project_id,
            poll_interval=context.op_config["poll_interval"],
            poll_timeout=context.op_config["poll_timeout"],
        )

    return project_asset


project_assets = with_resources(
    definitions=[make_project_asset(PROJ_ID)],
    resource_defs={"my_resource": my_resource},
    resource_config_by_key={PROJ_ID: {"poll_interval": 1, "poll_timeout": 10}},
)


@repository
def dagster_project():
    return [project_assets]
