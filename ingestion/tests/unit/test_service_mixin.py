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
Tests for OMeta service mixin helpers
"""

from types import SimpleNamespace
from unittest import TestCase

from metadata.generated.schema.api.services.createDashboardService import (
    CreateDashboardServiceRequest,
)
from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.dashboardService import (
    DashboardConnection,
)
from metadata.ingestion.ometa.mixins.service_mixin import OMetaServiceMixin


class _DummyServiceMixin(OMetaServiceMixin):
    def __init__(self, store_service_connection: bool = True):
        self.config = SimpleNamespace(storeServiceConnection=store_service_connection)

    @staticmethod
    def get_create_entity_type(entity):  # pylint: disable=unused-argument
        return CreateDashboardServiceRequest


class TestServiceMixin(TestCase):
    def test_get_create_service_from_source_with_root_model_connection(self):
        service_connection = DataLensConnection.model_validate(
            {"organizationId": "org-id", "iamToken": "token"}
        )
        dashboard_connection = DashboardConnection(config=service_connection)
        workflow_source = SimpleNamespace(
            serviceName="datalens-test",
            serviceConnection=SimpleNamespace(root=dashboard_connection),
        )
        mixin = _DummyServiceMixin(store_service_connection=True)

        request = mixin.get_create_service_from_source(
            entity=DashboardService, config=workflow_source
        )

        self.assertEqual(request.serviceType.value, "DataLens")
        self.assertIsNotNone(request.connection)

    def test_get_create_service_from_source_without_storing_connection(self):
        service_connection = DataLensConnection.model_validate(
            {"organizationId": "org-id", "iamToken": "token"}
        )
        dashboard_connection = DashboardConnection(config=service_connection)
        workflow_source = SimpleNamespace(
            serviceName="datalens-test",
            serviceConnection=SimpleNamespace(root=dashboard_connection),
        )
        mixin = _DummyServiceMixin(store_service_connection=False)

        request = mixin.get_create_service_from_source(
            entity=DashboardService, config=workflow_source
        )

        self.assertEqual(request.serviceType.value, "DataLens")
        self.assertIsNone(request.connection)
