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
import json
import traceback
from typing import Any, Dict, Iterable, List, Optional, Set

from metadata.generated.schema.api.data.createChart import CreateChartRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDashboardDataModel import (
    CreateDashboardDataModelRequest,
)
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.chart import Chart, ChartType
from metadata.generated.schema.entity.data.dashboard import Dashboard as LineageDashboard
from metadata.generated.schema.entity.data.dashboardDataModel import (
    DashboardDataModel,
    DataModelType,
)
from metadata.generated.schema.entity.data.table import Column, DataType
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
from metadata.ingestion.source.dashboard.dashboard_service import (
    DashboardServiceSource,
    DashboardServiceTopology,
)
from metadata.utils import fqn
from metadata.ingestion.source.database.column_type_parser import ColumnTypeParser
from metadata.utils.filters import (
    filter_by_chart,
    filter_by_dashboard,
    filter_by_datamodel,
)
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class DataLensSource(DashboardServiceSource):
    """
    DataLens Source Class
    """

    topology = DashboardServiceTopology()
    topology.root.children = ["dashboard", "bulk_data_model"]
    topology.root.post_process = [
        "mark_dashboards_as_deleted",
        "mark_datamodels_as_deleted",
        "yield_datalens_chart_datamodel_lineage",
    ]

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__(config, metadata)
        self.dashboard_list: List[dict] = []
        self.dataset_entries: List[dict] = []
        self.dataset_id_set: Set[str] = set()
        self.dataset_entry_by_id: Dict[str, dict] = {}
        self.workbooks_by_id: Dict[str, dict] = {}
        self.collections_by_id: Dict[str, dict] = {}
        self.chart_details_cache: Dict[str, dict] = {}
        self.dashboard_details_cache: Dict[str, dict] = {}
        self.collection_path_cache: Dict[str, str] = {}
        self._dashboard_entries_by_id: Dict[str, dict] = {}
        self._collection_name_pattern: Optional[str] = None

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
            self.dashboard_list = list(
                self.client.list_dashboards(include_data=True)
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error("Failed to fetch DataLens dashboards: %s", exc)
            self.dashboard_list = []

        self._dashboard_entries_by_id = {
            self._get_entry_id(entry): entry
            for entry in self.dashboard_list
            if self._get_entry_id(entry)
        }

        self._collection_name_pattern = self._get_collection_name_pattern()
        if self._collection_name_pattern:
            self.source_config.markDeletedDashboards = False
            self.source_config.markDeletedDataModels = False
            self._prefetch_scoped_workbooks_collections()
            self.dashboard_list = [
                dashboard
                for dashboard in self.dashboard_list
                if self._collection_name_matches(dashboard)
            ]
        else:
            self._load_collections()
            self._load_workbooks()
        self._load_datasets()

    def _get_collection_name_pattern(self) -> Optional[str]:
        pattern = getattr(self.service_connection, "collectionNamePattern", None)
        if pattern is None:
            return None
        try:
            pattern = str(pattern).strip()
        except Exception:
            return None
        return pattern or None

    def _prefetch_scoped_workbooks_collections(self) -> None:
        workbook_ids: Set[str] = set()
        collection_ids: Set[str] = set()
        for dashboard in self.dashboard_list:
            workbook_id = dashboard.get("workbookId")
            collection_id = dashboard.get("collectionId")
            if workbook_id:
                workbook_ids.add(workbook_id)
            if collection_id:
                collection_ids.add(collection_id)
        for workbook_id in workbook_ids:
            try:
                workbook = self.client.get_workbook(workbook_id)
                if workbook:
                    self.workbooks_by_id[workbook_id] = workbook
                    if workbook.get("collectionId"):
                        collection_ids.add(workbook.get("collectionId"))
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug(traceback.format_exc())
                logger.warning(
                    "Failed to prefetch workbook %s: %s", workbook_id, exc
                )
        for collection_id in collection_ids:
            try:
                collection = self.client.get_collection(collection_id)
                if collection:
                    self.collections_by_id[collection_id] = collection
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug(traceback.format_exc())
                logger.warning(
                    "Failed to prefetch collection %s: %s", collection_id, exc
                )

    def _load_collections(self) -> None:
        try:
            queue: List[Optional[str]] = [None]
            seen: Set[str] = set()
            while queue:
                collection_id = queue.pop(0)
                for collection in self.client.list_collections(collection_id):
                    cid = collection.get("collectionId")
                    if not cid or cid in seen:
                        continue
                    seen.add(cid)
                    self.collections_by_id[cid] = collection
                    queue.append(cid)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(traceback.format_exc())
            logger.warning("Failed to load DataLens collections: %s", exc)

    def _load_workbooks(self) -> None:
        try:
            for workbook in self.client.list_workbooks():
                workbook_id = workbook.get("workbookId")
                if workbook_id:
                    self.workbooks_by_id[workbook_id] = workbook
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(traceback.format_exc())
            logger.warning("Failed to load DataLens workbooks: %s", exc)

    def _load_datasets(self) -> None:
        if not self.source_config.includeDataModels:
            return
        try:
            if self._collection_name_pattern:
                self.dataset_entries = self._load_datasets_from_relations()
            else:
                self.dataset_entries = list(self.client.list_datasets())
                if not self.dataset_entries:
                    self.dataset_entries = list(
                        self.client.list_datasets(include_data=True)
                    )
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(traceback.format_exc())
            logger.warning("Failed to load DataLens datasets: %s", exc)
            self.dataset_entries = []

        if not self.dataset_entries:
            self.dataset_entries = self._load_datasets_from_relations()

        self.dataset_id_set.clear()
        self.dataset_entry_by_id.clear()
        for entry in self.dataset_entries:
            dataset_id = self._get_entry_id(entry)
            if dataset_id:
                self.dataset_id_set.add(dataset_id)
                self.dataset_entry_by_id[dataset_id] = entry

    def _chunked(self, values: List[str], size: int = 100) -> Iterable[List[str]]:
        for idx in range(0, len(values), size):
            yield values[idx : idx + size]

    def _collect_chart_ids(self) -> Set[str]:
        chart_ids: Set[str] = set()
        for dashboard in self.dashboard_list:
            details = self.get_dashboard_details(dashboard)
            if not details:
                continue
            entry = self._get_entry(details)
            for chart_tab in self._iter_widget_charts(entry):
                chart_id = chart_tab.get("chartId")
                if chart_id:
                    chart_ids.add(chart_id)
        return chart_ids

    def _load_datasets_from_relations(self) -> List[dict]:
        entry_ids = list(self._collect_chart_ids())
        if not entry_ids:
            entry_ids = [
                entry_id
                for entry_id in (
                    self._get_entry_id(dashboard) for dashboard in self.dashboard_list
                )
                if entry_id
            ]
        if not entry_ids:
            return []

        entries_by_id: Dict[str, dict] = {}
        for chunk in self._chunked(entry_ids, size=100):
            for relation in self.client.list_entries_relations(
                chunk, scope="dataset"
            ):
                if relation.get("isLocked"):
                    continue
                entry_id = relation.get("entryId")
                if not entry_id:
                    continue
                entries_by_id.setdefault(entry_id, relation)
        return list(entries_by_id.values())

    def get_dashboards_list(self) -> Optional[List[dict]]:
        return self.dashboard_list

    def get_dashboard_name(self, dashboard: dict) -> str:
        return dashboard.get("entryId") or dashboard.get("savedId") or ""

    def get_dashboard_details(self, dashboard: dict) -> Optional[dict]:
        dashboard_id = dashboard.get("entryId") or dashboard.get("savedId")
        if not dashboard_id:
            return None
        cached = self.dashboard_details_cache.get(dashboard_id)
        if cached:
            return cached
        details: Optional[dict] = None
        try:
            details = self.client.get_dashboard(dashboard_id)
        except Exception as exc:
            logger.warning("Failed to get DataLens dashboard details: %s", exc)
            details = None
        if not details or not details.get("entry"):
            fallback_entry = dashboard
            if not fallback_entry.get("data"):
                entry_fallback = self._get_dashboard_entry(dashboard_id)
                if entry_fallback:
                    fallback_entry = entry_fallback
            details = {"entry": fallback_entry, "_list_entry": dashboard}
        else:
            # Keep original list entry data as fallback context
            details["_list_entry"] = dashboard
        self.dashboard_details_cache[dashboard_id] = details
        return details

    def _get_entry(self, dashboard_details: dict) -> dict:
        if not dashboard_details:
            return {}
        entry = dashboard_details.get("entry") or {}
        # Fill gaps from list entry if needed
        list_entry = dashboard_details.get("_list_entry") or {}
        merged = {**list_entry, **entry}
        return merged

    def _get_entry_id(self, entry: dict) -> Optional[str]:
        return entry.get("entryId") or entry.get("savedId")

    def _get_dashboard_entry(self, dashboard_id: str) -> Optional[dict]:
        if not dashboard_id:
            return None
        entry = self._dashboard_entries_by_id.get(dashboard_id)
        if entry:
            return entry
        try:
            response = self.client.list_entries(
                ids=[dashboard_id], include_links=True, include_data=True
            )
            entries = response.get("entries") or []
            if entries:
                entry = entries[0]
                entry_id = self._get_entry_id(entry)
                if entry_id:
                    self._dashboard_entries_by_id[entry_id] = entry
                return entry
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(traceback.format_exc())
            logger.warning("Failed to load dashboard entry %s: %s", dashboard_id, exc)
        return None

    def _get_collection_id_from_entry(self, entry: dict) -> Optional[str]:
        collection_id = entry.get("collectionId")
        if collection_id:
            return collection_id
        workbook_id = entry.get("workbookId")
        if not workbook_id:
            return None
        workbook = self.workbooks_by_id.get(workbook_id)
        if workbook:
            return workbook.get("collectionId")
        try:
            workbook = self.client.get_workbook(workbook_id)
            if workbook:
                self.workbooks_by_id[workbook_id] = workbook
                return workbook.get("collectionId")
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning("Failed to fetch workbook %s: %s", workbook_id, exc)
        return None

    def _collection_name_matches(self, entry: dict) -> bool:
        if not self._collection_name_pattern:
            return True
        pattern = self._collection_name_pattern.lower()
        collection_id = self._get_collection_id_from_entry(entry)
        collection_title = entry.get("collectionTitle")
        if not collection_title and collection_id:
            collection_title = self._get_collection_title(collection_id)
        collection_path = (
            self._get_collection_path(collection_id) if collection_id else None
        )
        for value in (collection_title, collection_path):
            if value and pattern in value.lower():
                return True
        return False

    def _get_workbook_title(self, workbook_id: Optional[str]) -> Optional[str]:
        if not workbook_id:
            return None
        workbook = self.workbooks_by_id.get(workbook_id)
        if workbook:
            return workbook.get("title")
        try:
            workbook = self.client.get_workbook(workbook_id)
            if workbook:
                self.workbooks_by_id[workbook_id] = workbook
                return workbook.get("title")
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning("Failed to fetch workbook %s: %s", workbook_id, exc)
        return None

    def _get_collection_title(self, collection_id: Optional[str]) -> Optional[str]:
        if not collection_id:
            return None
        cached = self.collections_by_id.get(collection_id)
        if cached:
            return cached.get("title")
        try:
            collection = self.client.get_collection(collection_id)
            if collection:
                self.collections_by_id[collection_id] = collection
                return collection.get("title")
        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning("Failed to fetch collection %s: %s", collection_id, exc)
        return None

    def _get_collection_path(self, collection_id: Optional[str]) -> Optional[str]:
        if not collection_id:
            return None
        cached = self.collection_path_cache.get(collection_id)
        if cached:
            return cached
        collection = self.collections_by_id.get(collection_id)
        if not collection:
            try:
                collection = self.client.get_collection(collection_id)
                if collection:
                    self.collections_by_id[collection_id] = collection
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning(
                    "Failed to fetch collection %s: %s", collection_id, exc
                )
                collection = None
        if not collection:
            return None
        title = collection.get("title")
        parent_id = collection.get("parentId")
        parent_path = self._get_collection_path(parent_id) if parent_id else None
        path = f"{parent_path} / {title}" if parent_path and title else title
        if path:
            self.collection_path_cache[collection_id] = path
        return path

    def _get_project_from_entry(self, entry: dict) -> Optional[str]:
        collection_id = entry.get("collectionId")
        collection_path = (
            self._get_collection_path(collection_id)
            if collection_id
            else entry.get("collectionTitle")
        )
        workbook_title = entry.get("workbookTitle")
        if not workbook_title:
            workbook_id = entry.get("workbookId")
            workbook = self.workbooks_by_id.get(workbook_id)
            if workbook:
                workbook_title = workbook.get("title")
                if not collection_path and workbook.get("collectionId"):
                    collection_path = self._get_collection_path(
                        workbook.get("collectionId")
                    )
            else:
                workbook_title = self._get_workbook_title(workbook_id)
        if collection_path and workbook_title:
            return f"{collection_path} / {workbook_title}"
        return collection_path or workbook_title

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

            project = self.context.get().project_name or self._get_project_from_entry(
                entry
            )

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

        chart_items = list(self._iter_widget_charts(entry))
        if not chart_items:
            dashboard_id = self._get_entry_id(entry)
            entry_fallback = (
                self._get_dashboard_entry(dashboard_id) if dashboard_id else None
            )
            if entry_fallback:
                entry = {**entry_fallback, **entry}
                chart_items = list(self._iter_widget_charts(entry))

        seen = set()
        for chart_tab in chart_items:
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

    def list_datamodels(self) -> Iterable[Any]:
        if not self.source_config.includeDataModels:
            return []
        return self.dataset_entries

    def _get_dataset_columns(self, dataset_read: dict) -> List[Column]:
        columns: List[Column] = []
        dataset = dataset_read.get("dataset") or {}
        for field in dataset.get("result_schema") or []:
            try:
                data_type = field.get("data_type") or field.get("type")
                data_type_display = (
                    data_type if data_type else DataType.UNKNOWN.value
                )
                parsed_column = {
                    "dataTypeDisplay": data_type_display,
                    "dataType": ColumnTypeParser.get_column_type(
                        data_type.upper() if isinstance(data_type, str) else None
                    ),
                    "name": field.get("guid") or field.get("title"),
                    "displayName": field.get("title")
                    or field.get("guid")
                    or field.get("name"),
                    "description": field.get("description"),
                }
                if data_type and str(data_type).upper() == DataType.ARRAY.value:
                    parsed_column["arrayDataType"] = DataType.UNKNOWN
                if parsed_column["name"]:
                    columns.append(Column(**parsed_column))
            except Exception as exc:
                logger.debug(traceback.format_exc())
                logger.warning("Error processing dataset column: %s", exc)
        if not columns:
            columns.append(
                Column(
                    name="column",
                    displayName="column",
                    dataType=DataType.UNKNOWN,
                    dataTypeDisplay=DataType.UNKNOWN.value,
                )
            )
        return columns

    def yield_bulk_datamodel(
        self, dataset_entry: dict
    ) -> Iterable[Either[CreateDashboardDataModelRequest]]:
        try:
            if not self.source_config.includeDataModels:
                return
            if isinstance(dataset_entry, str):
                dataset_id = dataset_entry
                entry = {}
            else:
                dataset_id = self._get_entry_id(dataset_entry)
                entry = dataset_entry
            if not dataset_id:
                return
            display_name = entry.get("key") or dataset_id
            if filter_by_datamodel(
                self.source_config.dataModelFilterPattern, display_name
            ):
                self.status.filter(display_name, "Data model filtered out.")
                return
            try:
                dataset_read = self.client.get_dataset(
                    dataset_id, workbook_id=entry.get("workbookId")
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch DataLens dataset %s: %s", dataset_id, exc
                )
                return
            if not dataset_read or not dataset_read.get("dataset"):
                logger.warning(
                    "Skipping DataLens dataset %s: empty response", dataset_id
                )
                return
            description = None
            description = dataset_read["dataset"].get("description")
            data_model_request = CreateDashboardDataModelRequest(
                name=EntityName(dataset_id),
                displayName=dataset_read.get("key") or display_name,
                description=Markdown(description) if description else None,
                service=FullyQualifiedEntityName(
                    self.context.get().dashboard_service
                ),
                dataModelType=DataModelType.DataLensDataset.value,
                serviceType=self.service_connection.type.value,
                columns=self._get_dataset_columns(dataset_read),
                project=self._get_project_from_entry(entry),
                sourceUrl=self._get_source_url(entry) if entry else None,
            )
            yield Either(right=data_model_request)
            self.register_record_datamodel(datamodel_request=data_model_request)
        except Exception as exc:
            name = (
                dataset_entry.get("key")
                if isinstance(dataset_entry, dict)
                else str(dataset_entry)
            )
            yield Either(
                left=StackTraceError(
                    name=name or "DataModel",
                    error=f"Error yielding Data Model [{dataset_entry}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def _extract_dataset_ids(self, value: Any, dataset_ids: Set[str]) -> Set[str]:
        found: Set[str] = set()
        if value is None:
            return found
        if isinstance(value, str):
            if value in dataset_ids:
                found.add(value)
                return found
            try:
                loaded = json.loads(value)
                return found.union(self._extract_dataset_ids(loaded, dataset_ids))
            except Exception:
                return found
        if isinstance(value, list):
            for item in value:
                found.update(self._extract_dataset_ids(item, dataset_ids))
            return found
        if isinstance(value, dict):
            for key, item in value.items():
                if isinstance(item, str) and item in dataset_ids:
                    found.add(item)
                else:
                    found.update(self._extract_dataset_ids(item, dataset_ids))
            return found
        return found

    def _get_chart_details(self, chart_id: str, workbook_id: Optional[str]) -> dict:
        if chart_id in self.chart_details_cache:
            return self.chart_details_cache[chart_id]
        details: dict = {}
        errors: List[Exception] = []
        last_tb: Optional[str] = None
        for getter in (
            self.client.get_editor_chart,
            self.client.get_ql_chart,
            self.client.get_wizard_chart,
        ):
            try:
                details = getter(chart_id, workbook_id=workbook_id)
                if details:
                    break
            except Exception as exc:
                errors.append(exc)
                last_tb = traceback.format_exc()
                continue
        if not details and errors:
            if last_tb:
                logger.debug(last_tb)
            logger.warning(
                "Failed to get chart details for %s: %s", chart_id, errors[-1]
            )
        self.chart_details_cache[chart_id] = details
        return details

    def _collect_chart_dataset_ids(
        self, entry: dict, chart_id: str
    ) -> Set[str]:
        dataset_ids: Set[str] = set()
        if not self.dataset_id_set:
            return dataset_ids
        for chart_tab in self._iter_widget_charts(entry):
            if chart_tab.get("chartId") == chart_id:
                dataset_ids.update(
                    self._extract_dataset_ids(
                        chart_tab.get("params") or {}, self.dataset_id_set
                    )
                )
        if not dataset_ids:
            chart_details = self._get_chart_details(
                chart_id, workbook_id=entry.get("workbookId")
            )
            chart_entry = chart_details.get("entry") or chart_details
            dataset_ids.update(
                self._extract_dataset_ids(chart_entry, self.dataset_id_set)
            )
        return dataset_ids

    def yield_dashboard_lineage_details(
        self, dashboard_details: Any, db_service_prefix: Optional[str] = None
    ) -> Iterable[Either[AddLineageRequest]]:
        entry = self._get_entry(dashboard_details)
        dashboard_name = entry.get("entryId") or entry.get("savedId")
        if not dashboard_name:
            return []

        try:
            dashboard_fqn = fqn.build(
                self.metadata,
                entity_type=LineageDashboard,
                service_name=self.context.get().dashboard_service,
                dashboard_name=dashboard_name,
            )
            dashboard_entity = self.metadata.get_by_name(
                entity=LineageDashboard, fqn=dashboard_fqn
            )
            if not dashboard_entity:
                return []

            for chart in self.context.get().charts or []:
                try:
                    chart_fqn = fqn.build(
                        self.metadata,
                        entity_type=Chart,
                        service_name=self.context.get().dashboard_service,
                        chart_name=chart,
                    )
                    chart_entity = self.metadata.get_by_name(
                        entity=Chart, fqn=chart_fqn
                    )
                    if chart_entity:
                        lineage = self._get_add_lineage_request(
                            to_entity=dashboard_entity, from_entity=chart_entity
                        )
                        if lineage:
                            yield lineage
                except Exception as exc:
                    yield Either(
                        left=StackTraceError(
                            name="Lineage",
                            error=f"Error linking chart lineage {chart}: {exc}",
                            stackTrace=traceback.format_exc(),
                        )
                    )
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name="Lineage",
                    error=f"Error extracting lineage: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def get_project_name(self, dashboard_details: dict) -> Optional[str]:
        entry = self._get_entry(dashboard_details)
        return self._get_project_from_entry(entry)

    def yield_datalens_chart_datamodel_lineage(
        self,
    ) -> Iterable[Either[AddLineageRequest]]:
        if not self.source_config.includeDataModels:
            return []
        if not self.dataset_id_set:
            return []

        for dashboard in self.dashboard_list:
            details = self.get_dashboard_details(dashboard)
            if not details:
                continue
            entry = self._get_entry(details)
            dashboard_name = entry.get("entryId") or entry.get("savedId")
            if not dashboard_name:
                continue

            for chart_tab in self._iter_widget_charts(entry):
                chart_id = chart_tab.get("chartId")
                if not chart_id:
                    continue
                try:
                    chart_fqn = fqn.build(
                        self.metadata,
                        entity_type=Chart,
                        service_name=self.context.get().dashboard_service,
                        chart_name=chart_id,
                    )
                    chart_entity = self.metadata.get_by_name(
                        entity=Chart, fqn=chart_fqn
                    )
                    if not chart_entity:
                        continue

                    dataset_ids = self._collect_chart_dataset_ids(entry, chart_id)
                    for dataset_id in dataset_ids:
                        datamodel_fqn = fqn.build(
                            self.metadata,
                            entity_type=DashboardDataModel,
                            service_name=self.context.get().dashboard_service,
                            data_model_name=dataset_id,
                        )
                        datamodel_entity = self.metadata.get_by_name(
                            entity=DashboardDataModel, fqn=datamodel_fqn
                        )
                        if not datamodel_entity:
                            continue
                        lineage = self._get_add_lineage_request(
                            to_entity=chart_entity, from_entity=datamodel_entity
                        )
                        if lineage:
                            yield lineage
                except Exception as exc:
                    yield Either(
                        left=StackTraceError(
                            name="Lineage",
                            error=f"Error linking datamodel lineage {chart_id}: {exc}",
                            stackTrace=traceback.format_exc(),
                        )
                    )
