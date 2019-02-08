# 標準ライブラリ
import os
import logging
import sys

# サードパーティライブラリ

# 自作ライブラリ
sys.path.append("..")
from data_getter.hotpepper_beauty_getter import DataGetterFromHotPepperBeauty
from data_saver.google_document_saver import GoogleDocumentWriter
from scraping.base_scraper import BaseScraper

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")


class HotPepperBeautyScraper(BaseScraper):
    def __init__(self, freeword: str, search_gender: str, db_name: str, db_section: str,
                 drive_auth: bool):
        super().__init__(freeword=freeword)
        self.inst_getter = DataGetterFromHotPepperBeauty(freeword=freeword, search_gender=search_gender,
                                                         db_name=db_name, db_section=db_section)
        self.drive_auth = drive_auth

    def get_and_save_data(self, search_length, op_spread_sheet_id, worksheet_name):
        per = 10
        for i in range(1, search_length, per):
            result = self.inst_getter.get_page(search_start=i, search_end=i+per)
            GoogleDocumentWriter(drive_auth=self.drive_auth).write_existing_data(data=result,
                                                                                 op_spread_sheet_id=op_spread_sheet_id,
                                                                                 worksheet_name=worksheet_name)


def main():
    formatter = '%(levelname)s - %(asctime)s - From %(name)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=formatter, filename=os.path.join(MODULE_PATH, "log",
                                                                                    "hot_pepper_beauty_scraper.log"))
    logger.info("{}を実行します。".format(__name__))

    # 引数
    freeword = "東京都"
    search_gender = "ALL"
    db_name = "HotPepperBeauty"
    db_section = "test1"
    drive_auth = False
    search_length = 20
    op_spread_sheet_id = "1qGrze9VlVpGq0zgeK3_jUd7ewkGbEv26M1jOj9beqtE"
    worksheet_name = "ホットペッパービューティーDB"

    HotPepperBeautyScraper(freeword=freeword, search_gender=search_gender, db_name=db_name, db_section=db_section,
                           drive_auth=drive_auth).get_and_save_data(search_length=search_length,
                                                                    op_spread_sheet_id=op_spread_sheet_id,
                                                                    worksheet_name=worksheet_name)
    logger.info("{}を終了します。".format(__name__))


if __name__ == '__main__':
    main()
