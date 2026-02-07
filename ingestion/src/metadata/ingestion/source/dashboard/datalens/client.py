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
from typing import Dict, Iterable, Optional

from metadata.ingestion.ometa.client import ClientConfig, LimitsException, REST
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
        if self.request_delay_seconds <= 0:
            return
        now = time.monotonic()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            remaining = self.request_delay_seconds - elapsed
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_ts = time.monotonic()

    def _post(self, path: str, payload: Dict) -> Dict:
        retries_left = self.rate_limit_max_retries
        while True:
            self._sleep_if_needed()
            try:
                return self.client.post(path, json=payload) or {}
            except LimitsException:
                try:
                    self.client._limits_reached.delete(path)
                except Exception:
                    pass
                if retries_left <= 0:
                    raise
                logger.warning(
                    "Rate limit hit for %s. Sleeping %ss (retries left: %s)",
                    path,
                    self.rate_limit_retry_seconds,
                    retries_left,
                )
                time.sleep(self.rate_limit_retry_seconds)
                retries_left -= 1

    def list_entries(
        self,
        scope: str,
        page: int = 1,
        page_size: Optional[int] = None,
        include_links: bool = True,
        include_data: bool = False,
    ) -> Dict:
        """Fetch entries by scope."""
        payload = {
            "scope": scope,
            "page": page,
            "pageSize": page_size or self.page_size,
            "includeLinks": include_links,
            "includeData": include_data,
            "includePermissionsInfo": False,
        }
        return self._post("/getEntries", payload)

    def list_dashboards(self) -> Iterable[Dict]:
        """Iterate dashboard entries."""
        page = 1
        page_size = self.page_size
        while True:
            response = self.list_entries(scope="dash", page=page, page_size=page_size)
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

    def test_connection(self) -> Dict:
        """Simple connectivity test."""
        return self.list_entries(scope="dash", page=1, page_size=1)
