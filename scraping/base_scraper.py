# 標準ライブラリ
import os
import logging
from abc import ABC
from abc import abstractmethod

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")


class BaseScraper(ABC):
    def __init__(self, freeword: str) -> None:
        self.freeword = freeword

    def __repr__(self):
        pass

    def __str__(self):
        pass

