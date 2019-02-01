# 標準ライブラリ
import os
import logging

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")


class CsvSaver:
    def __init__(self, filename):
        self.filename = filename

    def save_data(self, result):
        pass
