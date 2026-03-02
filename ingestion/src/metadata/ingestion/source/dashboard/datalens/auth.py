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
Auth helpers for DataLens
"""

import base64
import json
import re
import time
from typing import Any, Dict, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

IAM_TOKEN_URL = "https://iam.api.cloud.yandex.net/iam/v1/tokens"
IAM_AUDIENCE = IAM_TOKEN_URL
ALL_ASTERISKS_REGEX = re.compile(r"^\*+$")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _normalize_private_key(private_key: str) -> str:
    if "\\n" in private_key and "\n" not in private_key:
        return private_key.replace("\\n", "\n")
    return private_key


def _get_required_string(data: Dict, *keys: str) -> str:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError(f"Missing required key. Expected one of: {keys}")


def _escape_control_chars_in_json_strings(raw_json: str) -> str:
    """
    Escape raw control chars inside JSON string values.
    """
    escaped: list[str] = []
    in_string = False
    is_escaped = False

    for char in raw_json:
        if in_string:
            if is_escaped:
                escaped.append(char)
                is_escaped = False
                continue
            if char == "\\":
                escaped.append(char)
                is_escaped = True
                continue
            if char == '"':
                escaped.append(char)
                in_string = False
                continue
            if ord(char) < 32:
                control_map = {
                    "\n": "\\n",
                    "\r": "\\r",
                    "\t": "\\t",
                    "\b": "\\b",
                    "\f": "\\f",
                }
                escaped.append(control_map.get(char, f"\\u{ord(char):04x}"))
                continue
            escaped.append(char)
            continue

        escaped.append(char)
        if char == '"':
            in_string = True

    return "".join(escaped)


def _parse_service_account_json(service_account_json: Any) -> Dict[str, str]:
    if isinstance(service_account_json, dict):
        parsed = service_account_json
    elif isinstance(service_account_json, str):
        raw_json = service_account_json.strip()
        try:
            parsed = json.loads(raw_json)
        except Exception:
            try:
                parsed = json.loads(_escape_control_chars_in_json_strings(raw_json))
            except Exception as exc:
                raise ValueError(
                    "Invalid serviceAccountJson: should be valid JSON"
                ) from exc
    else:
        raise ValueError(
            "Invalid serviceAccountJson: should be valid JSON object string"
        )

    try:
        if not isinstance(parsed, dict):
            raise ValueError("Invalid serviceAccountJson: should be a JSON object")
    except Exception as exc:
        raise ValueError("Invalid serviceAccountJson: should be a JSON object") from exc

    key_id = _get_required_string(parsed, "id", "key_id", "keyId")
    service_account_id = _get_required_string(
        parsed, "service_account_id", "serviceAccountId"
    )
    private_key = _normalize_private_key(
        _get_required_string(parsed, "private_key", "privateKey")
    )

    return {
        "key_id": key_id,
        "service_account_id": service_account_id,
        "private_key": private_key,
    }


def _create_service_account_jwt(service_account: Dict[str, str]) -> str:
    now = int(time.time())
    headers = {"alg": "PS256", "typ": "JWT", "kid": service_account["key_id"]}
    payload = {
        "aud": IAM_AUDIENCE,
        "iss": service_account["service_account_id"],
        "iat": now,
        "exp": now + 3600,
    }

    encoded_header = _b64url_encode(
        json.dumps(headers, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    encoded_payload = _b64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{encoded_header}.{encoded_payload}".encode("utf-8")

    key = serialization.load_pem_private_key(
        service_account["private_key"].encode("utf-8"), password=None
    )
    signature = key.sign(
        signing_input,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=hashes.SHA256().digest_size,
        ),
        hashes.SHA256(),
    )
    encoded_signature = _b64url_encode(signature)
    return f"{encoded_header}.{encoded_payload}.{encoded_signature}"


def create_iam_token_from_service_account_json(
    service_account_json: str, verify_ssl: bool = True, timeout: int = 30
) -> str:
    service_account = _parse_service_account_json(service_account_json)
    signed_jwt = _create_service_account_jwt(service_account)

    response = requests.post(
        IAM_TOKEN_URL,
        json={"jwt": signed_jwt},
        timeout=timeout,
        verify=verify_ssl,
    )
    response.raise_for_status()
    payload = response.json() if response.text else {}

    iam_token = payload.get("iamToken") or payload.get("iam_token")
    if not iam_token:
        raise ValueError(
            f"Failed to get IAM token from Yandex IAM response: {payload}"
        )
    return iam_token


def resolve_iam_token(
    iam_token: Optional[str],
    service_account_json: Optional[str],
    verify_ssl: bool = True,
    timeout: int = 30,
) -> str:
    normalized_token = iam_token.strip() if isinstance(iam_token, str) else iam_token
    if (
        isinstance(normalized_token, str)
        and normalized_token
        and not ALL_ASTERISKS_REGEX.fullmatch(normalized_token)
    ):
        try:
            _parse_service_account_json(normalized_token)
            return create_iam_token_from_service_account_json(
                normalized_token, verify_ssl=verify_ssl, timeout=timeout
            )
        except ValueError:
            # Not a service account JSON payload; treat as a plain IAM token.
            pass
        return normalized_token

    normalized_sa_json = (
        service_account_json.strip()
        if isinstance(service_account_json, str)
        else service_account_json
    )
    if (
        isinstance(normalized_sa_json, str)
        and normalized_sa_json
        and not ALL_ASTERISKS_REGEX.fullmatch(normalized_sa_json)
    ):
        return create_iam_token_from_service_account_json(
            normalized_sa_json, verify_ssl=verify_ssl, timeout=timeout
        )

    raise ValueError(
        "Either iamToken or serviceAccountJson must be provided for DataLens connection"
    )
