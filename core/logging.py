"""
Logging configuration for the Spark ETL application.
Sets up loguru for application logging with appropriate handlers and formatters.
Optimized for production use with proper formatting and rotation.
"""
import os
import sys
import logging
from loguru import logger
from typing import Optional

def setup_logging(log_level: Optional[str] = None) -> None:
    """
    Set up comprehensive logging configuration.
    
    Args:
        log_level: Override the log level from settings
    """
    # Import here to avoid circular import
    from core.config import get_settings
    
    settings = get_settings()
    log_level = log_level or settings.LOG_LEVEL
    
    # Create logs directory
    os.makedirs("logs", exist_ok=True)
    
    # Remove default loguru handler
    logger.remove()
    
    # Console handler with colors and emojis for development
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=log_level,
        colorize=True,
        backtrace=True,
        diagnose=True
    )
    
    # File handler for persistent logs (production)
    logger.add(
        "logs/etl_app.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
        level=log_level,
        rotation="100 MB",
        retention="30 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Error-only file handler
    logger.add(
        "logs/etl_errors.log",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
        level="ERROR",
        rotation="50 MB",
        retention="60 days",
        compression="zip",
        backtrace=True,
        diagnose=True
    )
    
    # Configure standard logging to use loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1
            
            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )
    
    # Replace standard logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    # Suppress noisy loggers
    for noisy_logger in ["py4j", "pyspark", "urllib3", "boto3", "botocore"]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)
    
    logger.info(f"🚀 Logging initialized with level: {log_level}")