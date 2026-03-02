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
Tests for DataLens connection helpers
"""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.ingestion.source.dashboard.datalens.connection import (
    get_connection,
    test_connection as run_test_connection,
)


class TestDataLensConnection(TestCase):
    @patch(
        "metadata.ingestion.source.dashboard.datalens.connection.DataLensApiClient"
    )
    @patch(
        "metadata.ingestion.source.dashboard.datalens.connection.resolve_iam_token",
        return_value="generated-token",
    )
    def test_get_connection_supports_root_model(
        self, mock_resolve_iam_token, mock_client
    ):
        connection = DataLensConnection.model_validate(
            {"organizationId": "org-id", "iamToken": "iam-token"}
        )

        get_connection(connection)

        mock_resolve_iam_token.assert_called_once_with(
            iam_token="iam-token",
            service_account_json=None,
            verify_ssl=True,
            timeout=30,
        )
        kwargs = mock_client.call_args.kwargs
        self.assertEqual(kwargs["organization_id"], "org-id")
        self.assertEqual(kwargs["iam_token"], "generated-token")
        self.assertEqual(str(kwargs["api_url"]), "https://api.datalens.tech")

    @patch(
        "metadata.ingestion.source.dashboard.datalens.connection.test_connection_steps",
        return_value=MagicMock(),
    )
    def test_test_connection_falls_back_when_type_is_missing(
        self, mock_test_connection_steps
    ):
        metadata = MagicMock()
        client = MagicMock()
        client.test_connection.return_value = {"entries": [{"key": "value"}]}
        service_connection = SimpleNamespace()

        run_test_connection(
            metadata=metadata,
            client=client,
            service_connection=service_connection,
        )

        self.assertEqual(
            mock_test_connection_steps.call_args.kwargs["service_type"], "DataLens"
        )
