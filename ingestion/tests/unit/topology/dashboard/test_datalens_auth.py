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
Tests for DataLens auth helper
"""

import json
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.ingestion.source.dashboard.datalens.auth import (
    _parse_service_account_json,
    create_iam_token_from_service_account_json,
    resolve_iam_token,
)


class TestDataLensAuth(TestCase):
    def test_resolve_prefers_iam_token(self):
        with patch(
            "metadata.ingestion.source.dashboard.datalens.auth.create_iam_token_from_service_account_json",
            return_value="generated-token",
        ) as mock_create:
            token = resolve_iam_token(
                iam_token="static-token",
                service_account_json='{"id":"kid","service_account_id":"sa","private_key":"pk"}',
            )

        self.assertEqual(token, "static-token")
        mock_create.assert_not_called()

    def test_resolve_uses_iam_token_when_service_account_missing(self):
        token = resolve_iam_token(iam_token="static-token", service_account_json=None)
        self.assertEqual(token, "static-token")

    def test_resolve_uses_service_account_when_iam_missing(self):
        with patch(
            "metadata.ingestion.source.dashboard.datalens.auth.create_iam_token_from_service_account_json",
            return_value="generated-token",
        ) as mock_create:
            token = resolve_iam_token(
                iam_token=None,
                service_account_json='{"id":"kid","service_account_id":"sa","private_key":"pk"}',
            )

        self.assertEqual(token, "generated-token")
        mock_create.assert_called_once()

    def test_resolve_uses_service_account_when_json_passed_as_iam_token(self):
        service_account_json = (
            '{"id":"kid","service_account_id":"sa","private_key":"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n"}'
        )
        with patch(
            "metadata.ingestion.source.dashboard.datalens.auth.create_iam_token_from_service_account_json",
            return_value="generated-token",
        ) as mock_create:
            token = resolve_iam_token(
                iam_token=service_account_json,
                service_account_json=None,
            )

        self.assertEqual(token, "generated-token")
        mock_create.assert_called_once()

    def test_resolve_ignores_masked_values(self):
        with self.assertRaises(ValueError):
            resolve_iam_token(iam_token="********", service_account_json="********")

    def test_resolve_raises_when_no_credentials(self):
        with self.assertRaises(ValueError):
            resolve_iam_token(iam_token=None, service_account_json=None)

    def test_create_iam_token_from_service_account_json(self):
        service_account_json = json.dumps(
            {
                "id": "kid",
                "service_account_id": "sa",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n",
            }
        )
        with patch(
            "metadata.ingestion.source.dashboard.datalens.auth._create_service_account_jwt",
            return_value="signed-jwt",
        ) as mock_create_jwt:
            response = MagicMock()
            response.raise_for_status.return_value = None
            response.text = '{"iamToken":"generated-token"}'
            response.json.return_value = {"iamToken": "generated-token"}
            with patch(
                "metadata.ingestion.source.dashboard.datalens.auth.requests.post",
                return_value=response,
            ) as mock_post:
                token = create_iam_token_from_service_account_json(
                    service_account_json, verify_ssl=False, timeout=12
                )

        self.assertEqual(token, "generated-token")
        mock_create_jwt.assert_called_once()
        mock_post.assert_called_once_with(
            "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            json={"jwt": "signed-jwt"},
            timeout=12,
            verify=False,
        )

    def test_connection_schema_requires_auth_variant(self):
        with self.assertRaises(ValidationError):
            DataLensConnection(organizationId="org-id")

    def test_parse_service_account_json_with_raw_newlines(self):
        service_account_json = (
            '{"id":"kid","service_account_id":"sa","private_key":"PLEASE DO NOT REMOVE THIS LINE! '
            'Yandex.Cloud SA Key ID <kid>\n'
            "-----BEGIN PRIVATE KEY-----\n"
            "abc\n"
            '-----END PRIVATE KEY-----\n"}'
        )

        parsed = _parse_service_account_json(service_account_json)

        self.assertEqual(parsed["key_id"], "kid")
        self.assertEqual(parsed["service_account_id"], "sa")
        self.assertIn("-----BEGIN PRIVATE KEY-----", parsed["private_key"])
