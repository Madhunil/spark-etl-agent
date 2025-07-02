"""
Logging configuration for the Spark ETL application.
Sets up loguru for application logging with appropriate handlers and formatters.
"""
import os
import sys
import logging
from loguru import logger

def setup_logging(log_level=None):
    """
    Set up logging configuration for the application.
    
    Args:
        log_level (str, optional): Override the log level from settings
    """
    # Import here to avoid circular import
    from core.config import get_settings
    
    # Get settings
    settings = get_settings()
    
    # Use provided log level or fall back to settings
    log_level = log_level or settings.LOG_LEVEL
    
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Remove default loguru handler
    logger.remove()
    
    # Add a console handler with desired format and level
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True,
    )
    
    # Add a file handler for persistent logs
    logger.add(
        "logs/app.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=log_level,
        rotation="50 MB",
        retention="10 days",
        compression="zip",
    )
    
    # Configure standard logging to use our loguru handler
    class InterceptHandler(logging.Handler):
        """
        Handler for intercepting standard logging and routing it to loguru.
        """
        def emit(self, record):
            # Get corresponding Loguru level if it exists
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            
            # Find caller from where originated the logged message
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1
            
            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )
    
    # Replace all existing standard logging handlers with our interceptor
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    # Configure library-specific logging levels
    for lib_logger in ["py4j", "pyspark", "findspark"]:
        logging.getLogger(lib_logger).setLevel(logging.WARNING)
    
    logger.debug(f"Logging initialized with level: {log_level}")