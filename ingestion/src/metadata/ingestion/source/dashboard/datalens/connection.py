#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Source connection handler for DataLens
"""
from typing import Optional

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.generated.schema.entity.services.connections.testConnectionResult import (
    TestConnectionResult,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.datalens.auth import resolve_iam_token
from metadata.ingestion.source.dashboard.datalens.client import DataLensApiClient
from metadata.utils.constants import THREE_MIN


def _resolve_connection_model(connection: DataLensConnection):
    """
    DataLensConnection can come as a RootModel (connection.root).
    Normalize to the inner model with actual fields.
    """
    return getattr(connection, "root", connection)


def _resolve_service_type(service_connection: DataLensConnection) -> str:
    """
    Return DataLens service type from the model when available.
    Fallback to literal value for backward compatibility.
    """
    connection = _resolve_connection_model(service_connection)
    connection_type = getattr(connection, "type", None)
    if hasattr(connection_type, "value"):
        return connection_type.value
    if isinstance(connection_type, str):
        return connection_type
    return "DataLens"


def get_connection(connection: DataLensConnection) -> DataLensApiClient:
    """Create connection to DataLens"""
    connection = _resolve_connection_model(connection)
    verify_ssl = connection.verifySSL if connection.verifySSL is not None else True
    timeout = getattr(connection, "timeout", None)
    iam_token = resolve_iam_token(
        iam_token=(
            connection.iamToken.get_secret_value() if connection.iamToken else None
        ),
        service_account_json=(
            connection.serviceAccountJson.get_secret_value()
            if getattr(connection, "serviceAccountJson", None)
            else None
        ),
        verify_ssl=verify_ssl,
        timeout=timeout or 30,
    )

    return DataLensApiClient(
        api_url=connection.apiURL,
        iam_token=iam_token,
        organization_id=connection.organizationId,
        api_version=connection.apiVersion or "1",
        verify_ssl=verify_ssl,
        page_size=connection.pageSize or 100,
        request_delay_seconds=getattr(connection, "requestDelaySeconds", 0.2),
        rate_limit_retry_seconds=getattr(connection, "rateLimitRetrySeconds", 60),
        rate_limit_max_retries=getattr(connection, "rateLimitMaxRetries", 3),
        timeout=timeout,
    )


def test_connection(
    metadata: OpenMetadata,
    client: DataLensApiClient,
    service_connection: DataLensConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
    timeout_seconds: Optional[int] = THREE_MIN,
) -> TestConnectionResult:
    """Test connection to DataLens API"""

    def custom_executor():
        response = client.test_connection()
        if not response or not response.get("entries"):
            raise Exception("Empty response from DataLens /rpc/getEntries")

    test_fn = {"GetDashboards": custom_executor}

    return test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=_resolve_service_type(service_connection),
        automation_workflow=automation_workflow,
        timeout_seconds=timeout_seconds,
    )
