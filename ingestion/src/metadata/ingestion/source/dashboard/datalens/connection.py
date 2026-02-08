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
from metadata.ingestion.source.dashboard.datalens.client import DataLensApiClient
from metadata.utils.constants import THREE_MIN


def get_connection(connection: DataLensConnection) -> DataLensApiClient:
    """Create connection to DataLens"""
    return DataLensApiClient(
        api_url=connection.apiURL,
        iam_token=connection.iamToken.get_secret_value(),
        organization_id=connection.organizationId,
        api_version=connection.apiVersion or "1",
        verify_ssl=connection.verifySSL if connection.verifySSL is not None else True,
        page_size=connection.pageSize or 100,
        request_delay_seconds=getattr(connection, "requestDelaySeconds", 0.2),
        rate_limit_retry_seconds=getattr(connection, "rateLimitRetrySeconds", 60),
        rate_limit_max_retries=getattr(connection, "rateLimitMaxRetries", 3),
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
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
        timeout_seconds=timeout_seconds,
    )
