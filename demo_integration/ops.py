import json
from dagster import (
    AssetMaterialization,
    Field,
    In,
    MetadataValue,
    Noneable,
    Nothing,
    Out,
    Output,
    op,
)

from .resources import DEFAULT_POLL_INTERVAL
from .types import MyOutput


@op(
    required_resource_keys={"my_resource"},
    ins={"start_after": In(Nothing)},
    out=Out(
        MyOutput,
        description="Parsed json dictionary representing the details of the "
        "project after the run successfully completes.",
    ),
    config_schema={
        "project_id": Field(
            str, is_required=True, description="The Project ID that this op will run."
        ),
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
def project_op(context):

    my_output: MyOutput = context.resources.my_resource.run_and_poll(
        project_id=context.op_config["project_id"],
        poll_interval=context.op_config["poll_interval"],
        poll_timeout=context.op_config["poll_timeout"],
    )
    asset_name = ["my_output", json.loads(my_output.run_response["data"])["runId"]]

    context.log_event(
        AssetMaterialization(
            asset_name,
            description="Project Details",
        )
    )
    yield Output(my_output)
