import logging
import os
from unittest.mock import patch

from src.app_logger import get_file_handler, get_logger, get_stream_handler, s_log_format
from src.config import PARENT_DIR

path_file = os.path.join(PARENT_DIR, "tmp")
try:
    os.mkdir(path_file)
    print(f"Директория '{path_file}' создана успешно.")
except FileExistsError:
    print(f"Директория '{path_file}' уже существует.")

# # Фикстура для удаления созданных лог-файлов после теста
# @pytest.fixture(autouse=True)
# def cleanup_logs():
#     yield
#     shutil.rmtree(path_file, ignore_errors=True)


def test_get_file_handler():
    handler = get_file_handler(os.path.join(path_file, "test.log"))
    assert isinstance(handler, logging.FileHandler)
    assert handler.level == logging.INFO
    assert handler.formatter._fmt == s_log_format


def test_get_stream_handler():
    handler = get_stream_handler()
    assert isinstance(handler, logging.StreamHandler)
    assert handler.level == logging.ERROR
    assert handler.formatter._fmt == s_log_format


def test_get_logger():
    log_path = os.path.join(path_file, "test.log")
    logger = get_logger(log_path)
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    handlers = logger.handlers
    assert any(isinstance(h, logging.FileHandler) for h in handlers)


def test_logging_message(tmpdir):
    log_path = os.path.join(path_file, "test.log")
    logger = get_logger(os.path.join(path_file, log_path))
    logger.info("Test message")

    # Проверяем, что сообщение было записано в файл
    with open(log_path, "r", encoding="utf-8") as f:
        logs = f.read()
        assert "Test message" in logs


def test_log_levels():
    logger = get_logger(os.path.join(path_file, "level_test"))
    with patch.object(logger, "info") as mock_info, patch.object(logger, "warning") as mock_warning:
        logger.info("Info level message")
        logger.warning("Warning level message")
        mock_info.assert_called_once_with("Info level message")
        mock_warning.assert_called_once_with("Warning level message")


def test_error_level_logged_to_console(caplog):
    logger = get_logger(os.path.join(path_file, "console_test"))
    logger.error("This should go to console")
    captured = caplog.text
    assert "This should go to console" in captured


def test_file_handler_creation():
    log_file = os.path.join(path_file, "test_file_handler.log")
    handler = get_file_handler(log_file)
    try:
        handler.emit(logging.LogRecord("test", logging.INFO, "", 0, "Test message", (), None))
        assert os.path.exists(log_file)
    finally:
        handler.close()
