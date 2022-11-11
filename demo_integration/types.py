from typing import NamedTuple, TypedDict

RunResponse = TypedDict(
    "RunResponse",
    {
        "args": dict,
        "data": dict,
        "form": dict,
        "headers": dict,
        "json": dict,
        "origin": str,
        "url": str,
    },
)

StatusResponse = TypedDict(
    "StatusResponse",
    {
        "args": dict,
        "data": dict,
        "form": dict,
        "headers": dict,
        "json": dict,
        "method": str,
        "origin": str,
        "url": str,
    },
)


class MyOutput(
    NamedTuple(
        "_MyOutput",
        [
            ("run_response", RunResponse),
            ("status_response", StatusResponse),
        ],
    )
):
    pass
