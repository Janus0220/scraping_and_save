# 標準ライブラリ
import os
import logging

# サードパーティライブラリ
import pandas as pd
from pymongo import MongoClient

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")


class MongoSaver:
    def __init__(self, db_name, db_section):
        self.db_name = db_name
        self.db_section = db_section
        self.collection = MongoClient()[db_name][db_section]

    def save_many_data(self, result):
        self.collection.insert_many(result)
        logger.info("Mongodb(db_name: {}, db_section: {})へのデータの{}件の書き込みを終了します。".format(self.db_name,
                                                                                       self.db_section, len(result)))

    def save_data(self, result):
        self.collection.insert_one(result)
        logger.info("Mongodb(db_name: {}, db_section: {})へのデータの{}件の書き込みを終了します。".format(self.db_name,
                                                                                       self.db_section, len(result)))

    def conv_all_data_to_dataframe(self, save_dict, key, value, num=None):
        logger.info("Mongodb(db_name: {}, db_section: {})のデータをデータフレームへ変換します。".format(self.db_name,
                                                                                     self.db_section))
        if not num:
            num = self.collection.find({key: value}).count()
        for value in self.collection.find({""}).limit(num):
            for key in value.keys():
                save_dict[key].append(value[key])
        return pd.DataFrame(save_dict)
