"""
Centralized structlog configuration with color-coded output (following worker-py pattern).
"""

import structlog
import logging
import sys
from pathlib import Path
from datetime import datetime


def configure_structlog(log_level: str = "INFO", enable_file_logging: bool = True) -> None:
    """
    Configure structlog with color-coded console output and optional file logging.
    
    Args:
        log_level: Log level (DEBUG, INFO, WARN, ERROR, CRITICAL)
        enable_file_logging: Whether to enable file logging
    """
    # Create logs directory if file logging is enabled
    if enable_file_logging:
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
    
    # Convert log level string to logging constant
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARN": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    log_level_int = level_map.get(log_level.upper(), logging.INFO)
    
    # Configure processors for console (with colors)
    console_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f", utc=False),
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
            ]
        ),
        # Color-coded console output
        structlog.dev.ConsoleRenderer(colors=True)
    ]
    
    # Configure processors for file (JSON format)
    file_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f", utc=True),
        structlog.processors.CallsiteParameterAdder(
            [
                structlog.processors.CallsiteParameter.MODULE,
                structlog.processors.CallsiteParameter.FILENAME,
                structlog.processors.CallsiteParameter.FUNC_NAME,
                structlog.processors.CallsiteParameter.LINENO,
                structlog.processors.CallsiteParameter.THREAD_NAME,
            ]
        ),
        structlog.processors.JSONRenderer()
    ]
    
    # Setup standard library logging to work with structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level_int,
    )
    
    # Configure structlog for console output
    structlog.configure(
        processors=console_processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level_int),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True
    )
    
    # Setup file logging if enabled
    if enable_file_logging:
        log_file = log_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
        
        # Create file handler with JSON format
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level_int)
        
        # Configure a separate structlog configuration for file output
        file_logger = structlog.wrap_logger(
            logging.getLogger("file_logger"),
            processors=file_processors,
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            cache_logger_on_first_use=True
        )
        
        # Add file handler to root logger
        logging.getLogger().addHandler(file_handler)


def get_logger(name: str = None) -> structlog.stdlib.BoundLogger:
    """
    Get a structlog logger instance.
    
    Args:
        name: Logger name (optional)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


# Convenience function for backward compatibility
def setup_logging(debug: bool = False, log_file: str = None) -> structlog.stdlib.BoundLogger:
    """
    Setup logging with structlog (backward compatibility function).
    
    Args:
        debug: Enable debug level logging
        log_file: Ignored (kept for compatibility)
        
    Returns:
        Configured structlog logger
    """
    log_level = "DEBUG" if debug else "INFO"
    configure_structlog(log_level=log_level, enable_file_logging=True)
    return get_logger("process_optimization")
