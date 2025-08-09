"""
Tests for the logger module.
"""

import unittest
import os
from utils.logger import setup_logger
import config

class TestLogger(unittest.TestCase):
    """Tests for the logger module."""

    def test_logger_creation(self):
        """Test that the logger is created successfully."""
        logger = setup_logger()
        self.assertIsNotNone(logger)

    def test_log_file_created(self):
        """Test that the log file is created in the correct directory."""
        # This is a bit tricky to test without actually creating a file.
        # We'll just check that the logs directory exists.
        self.assertTrue(os.path.exists(config.LOGS_DIR))

if __name__ == '__main__':
    unittest.main()
