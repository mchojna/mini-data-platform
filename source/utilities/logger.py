"""Provides a class for creating and configuring loggers."""

import logging


class Logger:
    """
    Class for creating named loggers with standardized configuration.
    """

    @staticmethod
    def get_logger(name: str, level=logging.DEBUG) -> logging.Logger:
        """Creates and returns a logger with the given name and logging level."""
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if not logger.handlers:
            # Handler (console)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)

            # Formatter
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)

            # Add handler
            logger.addHandler(console_handler)

        return logger
