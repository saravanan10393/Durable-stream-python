"""
JSON processing utilities for the Durable Streams Protocol.

This module handles JSON mode operations including:
- Validation
- Array flattening (batch operations)
- Response formatting
"""

import json
from typing import Any

from .store import InvalidJsonError, EmptyArrayError


def process_json_append(data: bytes) -> bytes:
    """
    Process JSON data for append in JSON mode.

    - Validates JSON
    - Extracts array elements if data is an array (flattens one level)
    - Always appends trailing comma for easy concatenation

    Args:
        data: The JSON bytes to process

    Returns:
        Processed bytes with trailing comma

    Raises:
        InvalidJsonError: If JSON is invalid
        EmptyArrayError: If array is empty
    """
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise InvalidJsonError("Invalid UTF-8 encoding in JSON data")

    # Validate JSON
    try:
        parsed: Any = json.loads(text)
    except json.JSONDecodeError as e:
        raise InvalidJsonError(f"Invalid JSON: {e}")

    # If it's an array, extract elements and join with commas
    if isinstance(parsed, list):
        if len(parsed) == 0:
            raise EmptyArrayError("Empty arrays are not allowed")
        elements = [json.dumps(item, separators=(",", ":")) for item in parsed]
        result = ",".join(elements) + ","
    else:
        # Single value - add trailing comma
        result = text.strip() + ","

    return result.encode("utf-8")


def format_json_response(data: bytes) -> bytes:
    """
    Format JSON mode response by wrapping in array brackets.
    Strips trailing comma before wrapping.

    Args:
        data: Concatenated JSON data with trailing commas

    Returns:
        Properly formatted JSON array
    """
    if len(data) == 0:
        return b"[]"

    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        # If we can't decode, return empty array
        return b"[]"

    # Strip trailing comma if present
    text = text.rstrip()
    if text.endswith(","):
        text = text[:-1]

    wrapped = f"[{text}]"
    return wrapped.encode("utf-8")
