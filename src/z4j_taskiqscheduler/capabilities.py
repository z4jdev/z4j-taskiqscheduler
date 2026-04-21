"""Capabilities for the taskiq scheduler adapter."""

from __future__ import annotations

DEFAULT_CAPABILITIES: frozenset[str] = frozenset(
    {"list", "read", "delete"},
)

__all__ = ["DEFAULT_CAPABILITIES"]
