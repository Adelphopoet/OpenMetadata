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
DataLens source module
"""
import traceback
from typing import Any, Iterable, List, Optional

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.services.connections.dashboard.dataLensConnection import (
    DataLensConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
    SourceUrl,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.dashboard.dashboard_service import DashboardServiceSource
from metadata.utils import fqn
from metadata.utils.filters import filter_by_chart, filter_by_dashboard
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class DataLensSource(DashboardServiceSource):
    """
    DataLens Source Class
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.dashboard_list: List[dict] = []

    @classmethod
    def create(
        cls,
        config_dict: dict,
        metadata: OpenMetadata,
        pipeline_name: Optional[str] = None,
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: DataLensConnection = config.serviceConnection.root.config
        if not isinstance(connection, DataLensConnection):
            raise InvalidSourceException(
                f"Expected DataLensConnection, but got {connection}"
            )
        return cls(config, metadata)

    def prepare(self):
        """Fetch the list of dashboards"""
        try:
            self.dashboard_list = list(self.client.list_dashboards())
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to fetch DataLens dashboards: %s", exc)
            self.dashboard_list = []

    def get_dashboards_list(self) -> Optional[List[dict]]:
        return self.dashboard_list

    def get_dashboard_name(self, dashboard: dict) -> str:
        return dashboard.get("entryId") or dashboard.get("savedId") or ""

    def get_dashboard_details(self, dashboard: dict) -> Optional[dict]:
        try:
            dashboard_id = dashboard.get("entryId") or dashboard.get("savedId")
            if not dashboard_id:
                return None
            details = self.client.get_dashboard(dashboard_id)
            if details and details.get("entry"):
                # Keep original list entry data as fallback context
                details["_list_entry"] = dashboard
            return details
        except Exception as exc:
            logger.warning("Failed to get DataLens dashboard details: %s", exc)
            return None

    def _get_entry(self, dashboard_details: dict) -> dict:
        if not dashboard_details:
            return {}
        entry = dashboard_details.get("entry") or {}
        # Fill gaps from list entry if needed
        list_entry = dashboard_details.get("_list_entry") or {}
        merged = {**list_entry, **entry}
        return merged

    def _get_source_url(self, entry: dict) -> Optional[SourceUrl]:
        links = entry.get("links") or {}
        if isinstance(links, dict):
            url = links.get("self") or links.get("public")
            if url:
                return SourceUrl(url)
        host_port = self.service_connection.hostPort
        if host_port and entry.get("entryId"):
            base = clean_uri(host_port)
            return SourceUrl(f"{base}/?entryId={entry.get('entryId')}")
        return None

    def yield_dashboard(
        self, dashboard_details: dict
    ) -> Iterable[Either[CreateDashboardRequest]]:
        try:
            entry = self._get_entry(dashboard_details)
            dashboard_name = entry.get("entryId") or entry.get("savedId")
            display_name = entry.get("key") or dashboard_name

            if filter_by_dashboard(
                self.source_config.dashboardFilterPattern, display_name
            ):
                self.status.filter(display_name, "Dashboard filtered out")
                return

            description = None
            annotation = entry.get("annotation") or {}
            if isinstance(annotation, dict):
                description = annotation.get("description")

            project = entry.get("collectionTitle") or entry.get("workbookTitle")

            dashboard_request = CreateDashboardRequest(
                name=EntityName(dashboard_name),
                displayName=display_name,
                description=Markdown(description) if description else None,
                charts=[
                    FullyQualifiedEntityName(
                        fqn.build(
                            self.metadata,
                            entity_type=Chart,
                            service_name=self.context.get().dashboard_service,
                            chart_name=chart,
                        )
                    )
                    for chart in self.context.get().charts or []
                ],
                service=FullyQualifiedEntityName(self.context.get().dashboard_service),
                sourceUrl=self._get_source_url(entry),
                project=project,
                owners=None,
            )

            yield Either(right=dashboard_request)
            self.register_record(dashboard_request=dashboard_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name="Dashboard",
                    error=f"Error to yield dashboard {dashboard_details}: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def _iter_widget_charts(self, entry: dict) -> Iterable[dict]:
        data = entry.get("data") or {}
        for tab in data.get("tabs") or []:
            for item in tab.get("items") or []:
                if item.get("type") != "widget":
                    continue
                widget_data = item.get("data") or {}
                for chart_tab in widget_data.get("tabs") or []:
                    yield chart_tab

    def yield_dashboard_chart(
        self, dashboard_details: dict
    ) -> Iterable[Either[CreateChartRequest]]:
        entry = self._get_entry(dashboard_details)
        if not entry:
            return
        seen = set()
        for chart_tab in self._iter_widget_charts(entry):
            chart_id = chart_tab.get("chartId")
            if not chart_id or chart_id in seen:
                continue
            seen.add(chart_id)
            display_name = chart_tab.get("title") or chart_id
            if filter_by_chart(self.source_config.chartFilterPattern, display_name):
                self.status.filter(display_name, "Chart filtered out")
                continue
            yield Either(
                right=CreateChartRequest(
                    name=EntityName(chart_id),
                    displayName=display_name,
                    description=(
                        Markdown(chart_tab.get("description"))
                        if chart_tab.get("description")
                        else None
                    ),
                    chartType=ChartType.Other,
                    service=FullyQualifiedEntityName(
                        self.context.get().dashboard_service
                    ),
                )
            )

    def yield_dashboard_lineage_details(
        self, dashboard_details: Any, db_service_prefix: Optional[str] = None
    ) -> Iterable[Either[AddLineageRequest]]:
        return []

    def get_project_name(self, dashboard_details: dict) -> Optional[str]:
        entry = self._get_entry(dashboard_details)
        return entry.get("collectionTitle") or entry.get("workbookTitle")
