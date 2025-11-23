from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel, Field, ValidationError


class NodeAction(BaseModel):
    action: str
    display: str


class NodeDataItem(BaseModel):
    data: str
    name: str
    type: str
    icon: Optional[str] = None
    hasTrend: bool   # directly map to JSON key


class AnalyticsResponse(BaseModel):
    message: str
    node: str
    name: Optional[str] = None
    icon: Optional[str] = None
    hasChildren: Optional[bool] = None
    data: Optional[List[NodeDataItem]] = None
    actions: Optional[List[NodeAction]] = None


class AnalyticsResponseEnvelope(BaseModel):
    responses: List[AnalyticsResponse]


class AnalyticsApiClient:
    """
    Thin client for Niagara Analytics Web API.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 10,
        verify_ssl: bool = True,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.auth = (username, password)
        self._verify_ssl = verify_ssl

    def _post(self, payload: Dict[str, Any]) -> AnalyticsResponseEnvelope:
        resp = self._session.post(
            self._base_url,
            json=payload,
            timeout=self._timeout,
            verify=self._verify_ssl,
        )
        resp.raise_for_status()
        try:
            return AnalyticsResponseEnvelope.parse_obj(resp.json())
        except ValidationError as exc:
            raise ValueError("Failed to parse analytics response envelope") from exc

    def get_node(self, node: str) -> AnalyticsResponse:
        """
        Returns the first response in the envelope for a GetNode call.
        """
        payload = {"requests": [{"message": "GetNode", "node": node}]}
        envelope = self._post(payload)
        if not envelope.responses:
            raise ValueError("Analytics response envelope contained no entries")

        return envelope.responses[0]
