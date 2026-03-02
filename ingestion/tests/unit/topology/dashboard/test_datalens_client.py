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
Test DataLens API Client
"""

from unittest import TestCase
from unittest.mock import MagicMock, patch

from requests.exceptions import HTTPError

from metadata.ingestion.source.dashboard.datalens.client import DataLensApiClient


class TestDataLensApiClient(TestCase):
    """Test DataLens API Client"""

    def setUp(self):
        self.client = DataLensApiClient(
            api_url="https://api.datalens.tech",
            iam_token="token",
            organization_id="org-id",
            request_delay_seconds=0.0,
            rate_limit_retry_seconds=60,
            rate_limit_max_retries=3,
        )

    @patch("time.sleep")
    def test_retries_with_retry_after_header(self, mock_sleep):
        """When 429 includes Retry-After, we should sleep that value."""
        limit_response = MagicMock()
        limit_response.status_code = 429
        limit_response.headers = {"Retry-After": "7"}
        limit_response.text = "rate limit"
        limit_response.raise_for_status.side_effect = HTTPError(response=limit_response)

        success_response = MagicMock()
        success_response.status_code = 200
        success_response.headers = {}
        success_response.text = '{"entries":[]}'
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {"entries": []}

        with patch.object(
            self.client.client._session,  # pylint: disable=protected-access
            "request",
            side_effect=[limit_response, success_response],
        ):
            response = self.client._post("/getWorkbook", {"workbookId": "1"})

        self.assertEqual(response, {"entries": []})
        mock_sleep.assert_any_call(7)
        self.assertNotIn(60, [call.args[0] for call in mock_sleep.call_args_list])
