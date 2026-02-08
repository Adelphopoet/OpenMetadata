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
REST client for DataLens
"""

import time
from typing import Dict, Iterable, List, Mapping, Optional

from metadata.ingestion.ometa.client import (
    APIError,
    ClientConfig,
    LimitsException,
    REST,
)
from metadata.utils.helpers import clean_uri
from metadata.utils.logger import utils_logger

logger = utils_logger()


class DataLensApiClient:
    """
    REST client for DataLens API
    """

    def __init__(
        self,
        api_url: str,
        iam_token: str,
        organization_id: str,
        api_version: str = "1",
        verify_ssl: Optional[bool] = True,
        page_size: int = 100,
        request_delay_seconds: float = 0.0,
        rate_limit_retry_seconds: int = 60,
        rate_limit_max_retries: int = 3,
        timeout: Optional[int] = None,
    ):
        self.page_size = page_size
        self.request_delay_seconds = max(0.0, request_delay_seconds)
        self.rate_limit_retry_seconds = max(1, rate_limit_retry_seconds)
        self.rate_limit_max_retries = max(0, rate_limit_max_retries)
        self._last_request_ts: Optional[float] = None
        self._dynamic_delay_seconds: float = 0.0
        self.client = REST(
            ClientConfig(
                base_url=clean_uri(api_url),
                api_version="rpc",
                access_token=iam_token,
                auth_header="x-yacloud-subjecttoken",
                auth_token_mode=None,
                extra_headers={
                    "x-dl-org-id": organization_id,
                    "x-dl-api-version": api_version,
                },
                verify=verify_ssl,
                timeout=timeout,
            )
        )

    def _sleep_if_needed(self) -> None:
        delay = max(self.request_delay_seconds, self._dynamic_delay_seconds)
        if delay <= 0:
            return
        now = time.monotonic()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            remaining = delay - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_ts = time.monotonic()

    def _get_retry_after_seconds(
        self, headers: Optional[Mapping[str, str]]
    ) -> Optional[int]:
        if not headers:
            return None
        value = None
        try:
            value = headers.get("retry-after") or headers.get("Retry-After")
        except Exception:
            value = None
        if value is None:
            return None
        try:
            return max(0, int(float(value)))
        except Exception:
            return None

    def _apply_rate_limit_headers(
        self, headers: Optional[Mapping[str, str]]
    ) -> None:
        if not headers:
            return
        normalized = {k.lower(): v for k, v in headers.items()}
        remaining = normalized.get("ratelimit-remaining")
        if remaining is not None:
            try:
                if int(remaining) <= 0:
                    retry_after = self._get_retry_after_seconds(headers)
                    if retry_after:
                        time.sleep(retry_after)
            except Exception:
                pass
        policy = normalized.get("ratelimit-policy")
        if policy:
            try:
                policy_part = policy.split(",")[0].strip()
                parts = [p.strip() for p in policy_part.split(";") if p.strip()]
                if parts:
                    limit = int(parts[0])
                    window = None
                    for part in parts[1:]:
                        if part.startswith("w="):
                            window = int(part.split("=", 1)[1])
                            break
                    if limit > 0 and window and window > 0:
                        self._dynamic_delay_seconds = max(
                            self._dynamic_delay_seconds, window / limit
                        )
            except Exception:
                pass

    def _post(self, path: str, payload: Dict) -> Dict:
        retries_left = self.rate_limit_max_retries
        while True:
            self._sleep_if_needed()
            try:
                response = self.client.post(path, json=payload) or {}
                self._apply_rate_limit_headers(
                    getattr(self.client, "_last_response_headers", None)
                )
                return response
            except LimitsException:
                try:
                    self.client._limits_reached.delete(path)
                except Exception:
                    pass
                retry_after = self._get_retry_after_seconds(
                    getattr(self.client, "_last_response_headers", None)
                )
                if retry_after is not None:
                    sleep_for = max(1, retry_after)
                else:
                    sleep_for = self.rate_limit_retry_seconds
                if retries_left <= 0:
                    raise
                logger.warning(
                    "Rate limit hit for %s. Sleeping %ss (retries left: %s)",
                    path,
                    sleep_for,
                    retries_left,
                )
                time.sleep(sleep_for)
                retries_left -= 1

    def _log_api_error(self, path: str, payload: Dict, exc: Exception) -> None:
        status = None
        body = None
        if isinstance(exc, APIError) and exc.response is not None:
            status = getattr(exc.response, "status_code", None)
            try:
                body = exc.response.text
            except Exception:
                body = None
        if body and len(body) > 1000:
            body = f"{body[:1000]}..."
        logger.warning(
            "DataLens API error on %s status=%s error=%s payload=%s response=%s",
            path,
            status,
            exc,
            payload,
            body,
        )

    def list_entries(
        self,
        scope: Optional[str] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        include_links: bool = True,
        include_data: bool = False,
        ignore_workbook_entries: Optional[bool] = None,
        ids: Optional[List[str]] = None,
    ) -> Dict:
        """Fetch entries by scope."""
        payload: Dict[str, object] = {
            "page": page,
            "pageSize": page_size or self.page_size,
            "includeLinks": include_links,
            "includeData": include_data,
            "includePermissionsInfo": False,
        }
        if scope:
            payload["scope"] = scope
        if ids:
            payload["ids"] = ids
        if ignore_workbook_entries is not None:
            payload["ignoreWorkbookEntries"] = ignore_workbook_entries
        try:
            return self._post("/getEntries", payload)
        except APIError as exc:
            self._log_api_error("/getEntries", payload, exc)
            if "Validation error" in str(exc) and payload.get("pageSize", 1) != 1:
                payload["pageSize"] = 1
                try:
                    return self._post("/getEntries", payload)
                except APIError as retry_exc:
                    self._log_api_error("/getEntries", payload, retry_exc)
            return {}

    def list_datasets(self, include_data: bool = False) -> Iterable[Dict]:
        """Iterate dataset entries."""
        page = 1
        page_size = self.page_size
        while True:
            response = self.list_entries(
                scope="dataset",
                page=page,
                page_size=page_size,
                include_links=True,
                include_data=include_data,
            )
            entries = response.get("entries") or []
            if not entries:
                break
            for entry in entries:
                if entry.get("isLocked"):
                    continue
                yield entry
            if len(entries) < page_size:
                break
            page += 1

    def list_collections(
        self, collection_id: Optional[str] = None
    ) -> Iterable[Dict]:
        """Iterate collections within a collection (root when None)."""
        page = 1
        page_token: Optional[str] = None
        while True:
            page_value: Optional[str] = page_token or str(page)
            payload: Dict[str, object] = {
                "collectionId": collection_id,
                "page": page_value,
                "pageSize": self.page_size,
                "mode": "onlyCollections",
                "includePermissionsInfo": False,
            }
            try:
                response = self._post("/getCollectionContent", payload)
            except APIError as exc:
                self._log_api_error("/getCollectionContent", payload, exc)
                break
            items = response.get("items") or []
            for item in items:
                if item.get("entity") == "collection":
                    yield item
            page_token = response.get("nextPageToken")
            if page_token:
                page = int(page_token) if str(page_token).isdigit() else page
                continue
            if len(items) < self.page_size:
                break
            page += 1

    def list_entries_relations(
        self,
        entry_ids: List[str],
        scope: Optional[str] = None,
        link_direction: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Iterable[Dict]:
        """Iterate entries relations for given entry ids."""
        page_token: Optional[str] = None
        payload: Dict[str, object] = {
            "entryIds": entry_ids,
            "includePermissionsInfo": False,
        }
        if scope:
            payload["scope"] = scope
        if link_direction:
            payload["linkDirection"] = link_direction
        if limit:
            payload["limit"] = limit
        while True:
            if page_token:
                payload["pageToken"] = page_token
            elif "pageToken" in payload:
                payload.pop("pageToken", None)
            response = self._post("/getEntriesRelations", payload)
            relations = response.get("relations") or []
            for relation in relations:
                yield relation
            page_token = response.get("nextPageToken")
            if not page_token:
                break

    def list_workbooks(
        self, collection_id: Optional[str] = None
    ) -> Iterable[Dict]:
        """Iterate workbooks."""
        page = 1
        page_size = self.page_size
        while True:
            payload = {
                "collectionId": collection_id,
                "page": page,
                "pageSize": page_size,
                "includePermissionsInfo": False,
            }
            try:
                response = self._post("/getWorkbooksList", payload)
            except APIError as exc:
                self._log_api_error("/getWorkbooksList", payload, exc)
                break
            workbooks = response.get("workbooks") or []
            if not workbooks:
                break
            for workbook in workbooks:
                yield workbook
            if len(workbooks) < page_size:
                break
            page += 1

    def get_workbook(self, workbook_id: str) -> Dict:
        """Fetch workbook details."""
        payload = {"workbookId": workbook_id, "includePermissionsInfo": False}
        return self._post("/getWorkbook", payload)

    def get_collection(self, collection_id: str) -> Dict:
        """Fetch collection details."""
        payload = {"collectionId": collection_id, "includePermissionsInfo": False}
        return self._post("/getCollection", payload)

    def list_dashboards(self, include_data: bool = False) -> Iterable[Dict]:
        """Iterate dashboard entries."""
        page = 1
        page_size = self.page_size
        while True:
            response = self.list_entries(
                scope="dash",
                page=page,
                page_size=page_size,
                include_data=include_data,
            )
            entries = response.get("entries") or []
            if not entries:
                break
            for entry in entries:
                if entry.get("isLocked"):
                    continue
                if entry.get("scope") != "dash":
                    continue
                yield entry
            if len(entries) < page_size:
                break
            page += 1

    def get_dashboard(self, dashboard_id: str, branch: str = "saved") -> Dict:
        """Fetch dashboard details."""
        payload = {
            "dashboardId": dashboard_id,
            "branch": branch,
            "includeLinks": True,
        }
        return self._post("/getDashboard", payload)

    def get_editor_chart(
        self, chart_id: str, workbook_id: Optional[str] = None
    ) -> Dict:
        """Fetch editor chart details."""
        payload: Dict[str, Optional[str]] = {
            "chartId": chart_id,
            "includeLinks": True,
            "branch": "saved",
        }
        if workbook_id:
            payload["workbookId"] = workbook_id
        return self._post("/getEditorChart", payload)

    def get_ql_chart(
        self, chart_id: str, workbook_id: Optional[str] = None
    ) -> Dict:
        """Fetch QL chart details."""
        payload: Dict[str, Optional[str]] = {
            "chartId": chart_id,
            "includeLinks": True,
            "branch": "saved",
        }
        if workbook_id:
            payload["workbookId"] = workbook_id
        return self._post("/getQLChart", payload)

    def get_wizard_chart(
        self, chart_id: str, workbook_id: Optional[str] = None
    ) -> Dict:
        """Fetch wizard chart details."""
        payload: Dict[str, Optional[str]] = {
            "chartId": chart_id,
            "includeLinks": True,
            "branch": "saved",
        }
        if workbook_id:
            payload["workbookId"] = workbook_id
        return self._post("/getWizardChart", payload)

    def get_dataset(self, dataset_id: str, workbook_id: Optional[str] = None) -> Dict:
        """Fetch dataset details."""
        payload: Dict[str, Optional[str]] = {"datasetId": dataset_id}
        if workbook_id:
            payload["workbookId"] = workbook_id
        return self._post("/getDataset", payload)

    def test_connection(self) -> Dict:
        """Simple connectivity test."""
        return self.list_entries(scope="dash", page=1, page_size=1)
