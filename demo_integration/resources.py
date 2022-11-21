import datetime
import json
import logging
import time
from typing import Any, Dict, Optional, cast
from urllib.parse import urljoin

import requests
from dagster import Failure, Field, StringSource, get_dagster_logger, resource

from .consts import (
    API_BASE,
    COMPLETE,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_POLL_TIMEOUT,
    TERMINAL_STATUSES,
    VALID_STATUSES,
)
from .types import MyOutput, RunResponse, StatusResponse


class MyResource:
    def __init__(
        self,
        api_key: str,
        base_url: str = API_BASE,
        log: logging.Logger = get_dagster_logger(),
        request_max_retries: int = 3,
        request_retry_delay: float = 0.25,
    ):

        self._log = log
        self._api_key = api_key
        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay
        self.api_base_url = base_url

    def make_request(
        self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:

        user_agent = "MyDagsterLibrary1.0"
        headers = {"Authorization": f"Bearer {self._api_key}", "User-Agent": user_agent}
        url = urljoin(self.api_base_url, endpoint)

        num_retries = 0
        while True:
            try:
                if method == "GET":
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        params=data,
                    )
                elif method == "POST":
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        json=data,
                    )
                else:
                    response = requests.request(
                        method=method,
                        url=url,
                        headers=headers,
                        data=data,
                    )

                response.raise_for_status()
                if response.headers.get("Content-Type", "").startswith(
                    "application/json"
                ):
                    try:
                        response_json = response.json()
                    except requests.exceptions.JSONDecodeError:
                        self._log.error("Failed to decode response from API.")
                        self._log.error("API returned: %s", response.text)
                        raise Failure(
                            "Unexpected response from API.Failed to decode to JSON."
                        )
                    return response_json
            except requests.RequestException as e:
                self._log.error("Request to API failed: %s", e)
                if num_retries == self._request_max_retries:
                    break
                num_retries += 1
                time.sleep(self._request_retry_delay)

        raise Failure("Exceeded max number of retries.")

    def run_project(
        self,
        project_id: str,
    ) -> RunResponse:
        method = "POST"
        endpoint = f"/anything/{project_id}/run"
        response = cast(
            RunResponse,
            # Fake a run id for demo purposes
            self.make_request(method=method, endpoint=endpoint, data={"runId": "123"}),
        )
        return response

    def run_status(self, project_id, run_id) -> StatusResponse:

        endpoint = f"/anything/project/{project_id}/run/{run_id}"
        method = "GET"

        response = cast(
            StatusResponse,
            self.make_request(
                method=method, endpoint=endpoint, data={"status": "COMPLETED"}
            ),
        )
        return response

    def run_and_poll(
        self,
        project_id: str,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = DEFAULT_POLL_TIMEOUT,
    ):
        run_response = self.run_project(project_id)
        run_id = json.loads(run_response["data"])["runId"]
        poll_start = datetime.datetime.now()
        while True:
            run_status = self.run_status(project_id, run_id)
            project_status = run_status["args"]["status"]

            self._log.debug(run_status)
            self._log.info(
                f"Polling Project {project_id}. Current status: {project_status}."
            )

            if project_status not in VALID_STATUSES:
                raise Failure(
                    f"Received an unexpected status from the API: {project_status}"
                )

            if project_status == COMPLETE:
                break

            if project_status in TERMINAL_STATUSES:
                raise Failure(f"Project Run failed with status {project_status}. ")

            if (
                poll_timeout
                and datetime.datetime.now()
                > poll_start + datetime.timedelta(seconds=poll_timeout)
            ):
                raise Failure(
                    f"Project {project_id} for run: {run_id}' timed out after "
                    f"{datetime.datetime.now() - poll_start}. "
                    f"Last status was {project_status}. "
                )

            time.sleep(poll_interval)
        return MyOutput(run_response=run_response, status_response=run_status)


@resource(
    config_schema={
        "api_key": Field(
            StringSource,
            is_required=True,
            description="API Key.",
        ),
        "base_url": Field(
            StringSource,
            default_value="http://httpbin.org",
            description="Base URL for API requests.",
        ),
        "request_max_retries": Field(
            int,
            default_value=3,
            description="The maximum times requests to the API should be retried "
            "before failing.",
        ),
        "request_retry_delay": Field(
            float,
            default_value=0.25,
            description="Time (in seconds) to wait between each request retry.",
        ),
    },
    description="This resource helps manage Hex",
)
def my_resource(context) -> MyResource:
    """
    This resource allows users to programmatically interface with the REST API
    """
    return MyResource(
        api_key=context.resource_config["api_key"],
        base_url=context.resource_config["base_url"],
        log=context.log,
        request_max_retries=context.resource_config["request_max_retries"],
        request_retry_delay=context.resource_config["request_retry_delay"],
    )
