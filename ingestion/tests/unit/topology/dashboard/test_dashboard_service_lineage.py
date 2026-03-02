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
Test dashboard service helpers
"""

from unittest import TestCase

from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource


class _DummyDashboardServiceSource(DashboardServiceSource):
    @classmethod
    def create(cls, config_dict: dict, metadata, pipeline_name=None):
        return cls.__new__(cls)

    def yield_dashboard(self, dashboard_details):
        return []

    def yield_dashboard_lineage_details(self, dashboard_details, db_service_prefix=None):
        return []

    def yield_dashboard_chart(self, dashboard_details):
        return []

    def get_dashboards_list(self):
        return []

    def get_dashboard_name(self, dashboard):
        return ""

    def get_dashboard_details(self, dashboard):
        return {}


class TestDashboardServiceHelpers(TestCase):
    def test_resolve_service_type_name_with_root_model_connection(self):
        source = _DummyDashboardServiceSource.__new__(_DummyDashboardServiceSource)
        source.service_connection = DataLensConnection.model_validate(
            {"organizationId": "org-id", "iamToken": "token"}
        )

        self.assertEqual(source._get_service_connection_type_name(), "DataLens")
        self.assertEqual(source.name, "DataLens")
