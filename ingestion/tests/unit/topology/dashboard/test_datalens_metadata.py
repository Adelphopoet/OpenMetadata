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
Tests for DataLens metadata helpers
"""

from unittest import TestCase
from uuid import uuid4

from metadata.generated.schema.type.basic import Uuid
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.ingestion.source.dashboard.datalens.metadata import DataLensSource


class TestDataLensMetadata(TestCase):
    def test_deduplicate_dashboards_by_entry_id(self):
        source = object.__new__(DataLensSource)

        result = source._deduplicate_dashboards(
            [
                {"entryId": "dash-1", "key": "one"},
                {"entryId": "dash-2", "key": "two"},
                {"entryId": "dash-1", "key": "one-duplicate"},
                {"savedId": "dash-3", "key": "three"},
                {"savedId": "dash-3", "key": "three-duplicate"},
            ]
        )

        self.assertEqual(
            [item.get("entryId") or item.get("savedId") for item in result],
            ["dash-1", "dash-2", "dash-3"],
        )

    def test_get_source_url_uses_host_port_for_root_model_connection(self):
        source = object.__new__(DataLensSource)
        source.service_connection = DataLensConnection.model_validate(
            {
                "organizationId": "org-id",
                "iamToken": "token",
                "hostPort": "https://datalens.yandex.ru",
            }
        )

        result = source._get_source_url({"entryId": "0u89tvgst742l", "links": {}})

        self.assertIsNotNone(result)
        self.assertEqual(str(result.root), "https://datalens.yandex.ru/?entryId=0u89tvgst742l")

    def test_collection_name_pattern_reads_root_model_connection(self):
        source = object.__new__(DataLensSource)
        source.service_connection = DataLensConnection.model_validate(
            {
                "organizationId": "org-id",
                "iamToken": "token",
                "collectionNamePattern": "[FoodTech] Рост",
            }
        )

        result = source._get_collection_name_pattern()

        self.assertEqual(result, "[FoodTech] Рост")

    def test_collection_pattern_matches_path_or_title(self):
        source = object.__new__(DataLensSource)
        source._collection_name_pattern = "[foodtech] рост"
        source._only_dashboards_in_collections = False
        source._get_collection_id_from_entry = lambda entry: "child-id"
        source._get_collection_title = lambda collection_id: "child collection"
        source._get_collection_path = (
            lambda collection_id: "[FoodTech] Рост / child collection"
        )

        result = source._collection_name_matches(
            {"entryId": "dash-id", "collectionTitle": None}
        )

        self.assertTrue(result)

    def test_yield_dashboard_lineage_uses_dashboard_chart_refs_when_context_empty(self):
        source = object.__new__(DataLensSource)
        dashboard_id = uuid4()
        chart_id_1 = uuid4()
        chart_id_2 = uuid4()

        source.context = type(
            "CtxMgr",
            (),
            {
                "get": lambda _self: type(
                    "Ctx", (), {"dashboard_service": "test", "charts": []}
                )()
            },
        )()

        dashboard_entity = type(
            "DashboardEntity",
            (),
            {
                "id": Uuid(dashboard_id),
                "charts": [
                    EntityReference(id=Uuid(chart_id_1), type="chart"),
                    EntityReference(id=Uuid(chart_id_2), type="chart"),
                ],
            },
        )()

        source.metadata = type(
            "Metadata",
            (),
            {"get_by_name": lambda _self, entity, fqn, fields=None: dashboard_entity},
        )()

        result = list(
            source.yield_dashboard_lineage_details({"entry": {"entryId": "dash-id"}})
        )

        self.assertEqual(len(result), 2)
        edge_types = [
            (
                item.right.edge.fromEntity.type,
                item.right.edge.toEntity.type,
                item.right.edge.lineageDetails.source.value,
            )
            for item in result
            if item.right
        ]
        self.assertEqual(
            edge_types,
            [
                ("chart", "dashboard", "DashboardLineage"),
                ("chart", "dashboard", "DashboardLineage"),
            ],
        )

    def test_yield_dashboard_skips_missing_chart_refs(self):
        source = object.__new__(DataLensSource)
        source.service_connection = DataLensConnection.model_validate(
            {
                "organizationId": "org-id",
                "iamToken": "token",
                "hostPort": "https://datalens.yandex.ru",
            }
        )
        source.source_config = type("Cfg", (), {"dashboardFilterPattern": None})()
        source.status = type("Status", (), {"filter": lambda *_args, **_kwargs: None})()
        source.register_record = lambda *_args, **_kwargs: None
        source.context = type(
            "CtxMgr",
            (),
            {
                "get": lambda _self: type(
                    "Ctx",
                    (),
                    {
                        "dashboard_service": "test2",
                        "charts": ["missing-chart", "existing-chart"],
                        "project_name": None,
                    },
                )()
            },
        )()

        source.metadata = type(
            "Metadata",
            (),
            {
                "get_by_name": lambda _self, entity, fqn, fields=None: object()
                if fqn.endswith(".existing-chart")
                else None
            },
        )()

        result = list(
            source.yield_dashboard(
                {"entry": {"entryId": "dash-1", "key": "Dashboard 1", "annotation": {}}}
            )
        )

        self.assertEqual(len(result), 1)
        request = result[0].right
        self.assertIsNotNone(request)
        self.assertEqual(len(request.charts), 1)
        self.assertEqual(request.charts[0].root, "test2.existing-chart")
