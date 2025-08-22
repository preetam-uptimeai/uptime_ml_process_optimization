"""Telemetry logging utilities."""

import structlog
from typing import Any, Dict


def get_logger(name: str = None) -> structlog.BoundLogger:
    """Get a structured logger instance."""
    return structlog.get_logger(name)


def log_optimization_event(
    event_type: str,
    data: Dict[str, Any],
    logger: structlog.BoundLogger = None
) -> None:
    """Log optimization-related events."""
    if logger is None:
        logger = get_logger()
    
    logger.info(
        "optimization_event",
        event_type=event_type,
        **data
    )


def log_api_request(
    method: str,
    endpoint: str,
    status_code: int,
    duration_ms: float,
    logger: structlog.BoundLogger = None
) -> None:
    """Log API request information."""
    if logger is None:
        logger = get_logger()
    
    logger.info(
        "api_request",
        method=method,
        endpoint=endpoint,
        status_code=status_code,
        duration_ms=duration_ms
    )
