"""Classic durable Matrix sync configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nio.durable import DurableSyncConfig

if TYPE_CHECKING:
    from mindroom.config.main import Config


def bot_ingestion_config(
    config: Config,
    *,
    timeout_ms: int,
    sync_filter: dict[str, object],
) -> DurableSyncConfig:
    """Build the durable Classic source settings for one bot."""
    if config.matrix_sync.mode != "classic":
        message = "Only Classic Matrix sync is supported"
        raise ValueError(message)
    return DurableSyncConfig(sync_timeout_ms=timeout_ms, sync_filter=sync_filter)
