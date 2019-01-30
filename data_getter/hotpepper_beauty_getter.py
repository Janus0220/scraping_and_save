# 標準ライブラリ
import requests
import logging
import time

# サードパーティライブラリ
from urlextract import URLExtract
import lxml.html
import lxml
import pymongo

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class DataGetterFromHotPepperBeauty:
    """HotPepperBeautyをスクレイピングするクラスです。"""
    def __init__(self, freeword: str, price_from: int, price_to: int, search_gender: str, db_section: str) -> None:
        self.base_url = "https://beauty.hotpepper.jp/CSP/bt/freewordSearch/?"
        self.freeword = freeword
        self.price_from = price_from
        self.price_to = price_to
        self.search_gender = search_gender
        self.db_section = db_section

    def get_page(self, search_length: int) -> None:
        for search_num in range(1, search_length):
            error_num = 0
            while True:
                try:
                    req = requests.get(self.base_url, params={"freeword": self.freeword,
                                                              "smcPriceFrom": self.price_from,
                                                              "smcPriceTo": self.price_to,
                                                              "searchGender": self.search_gender,
                                                              "pn": search_num})
                    logger.info("HotPepperBeautyの{}ページ目(url: {})を取得しています。".format(search_num, req.url))
                    if int(req.status_code) != 200:
                        logger.warning("Error {}: ページの取得が出来ませんでした。".format(req.status_code))
                        error_num += 1
                        time.sleep(5)

                    else:
                        html = lxml.html.fromstring(req.text)
                        # TODO HotPepperBeauty用に書き変える。
                        nextlist = [i.get("href") for i in html.cssselect("h3.r > a")]
                        error_num = 0
                        break

                except ConnectionError:
                    logger.warning("Connection Errorが発生しました。")
                    error_num += 1
                    time.sleep(10 * 60)

            if not nextlist:
                logger.info("HotPepperBeautyの検索結果が尽きました。")
                return None

            for url_base in nextlist:
                try:
                    url = URLExtract().find_urls(url_base)[0].split("&sa=")[0]

                except IndexError:
                    logger.warning("Urlを認識できませんでした。")
                    continue

                logger.info("現在url:{}を取得しています。".format(url))
                result = self.get_data(url, freeword=self.freeword)
                if (result["title"] != 0) and (result["address"] != 0):
                    self.save_mongodb(title=result["title"], address=result["address"],
                                      regular_holiday=result["regular_holiday"], site_url=result["site_url"],
                                      num_seat=result["num_seat"], num_staff=result["num_staff"],
                                      job_url=result["job_url"])

    def save_mongodb(self, title, address, regular_holiday, site_url, num_seat, num_staff, job_url) -> None:
        client = pymongo.MongoClient()
        db = client["HotPepperBeauty"]
        collection = db[self.db_section]
        collection.insert({"keyword": self.freeword, "title": title, "address": address,
                           "regular_holiday": regular_holiday, "site_url": site_url, "num_seat": num_seat,
                           "num_staff": num_staff, "job_url": job_url})
        logger.info("Mongodbに取得したデータの保存を完了しました。現在{}件データが存在します。".
                    format(self._get_db_count(section=self.db_section)))

    @staticmethod
    def get_data(url: str, freeword: str) -> dict:
        ## TODO HotPepperBeauty用に書き変える
        result_dict = {"keyword": freeword, "title": None, "address": None, "regular_holiday": None, "site_url": None,
                       "num_seat": None, "num_staff": None, "job_url": None}
        error_num = 0
        while True:
            if error_num >= 10:
                logger.warning("同一のURLに対するエラーが20回続いたので、このURLからの取得を終了します。")
                return result_dict

            try:
                req = requests.get(url)
                if int(req.status_code) != 200:
                    logger.error("Error {}: このページを取得出来ません。".format(req.status_code))
                    return result_dict

                else:
                    ## TODO HotPepperBeauty用に書き換える
                    """
                    result_dict["title"] = req.text
                    result_dict["address"] = 
                    result_dict["regular_holiday"] =
                    result_dict["site_url"] = 
                    result_dict["num_seat"] =
                    result_dict["num_staff"] =
                    result_dict["job_url"] = 
                    """
                    return result_dict

            except ConnectionError:
                logger.warning("Connection Errorが発生しました。")
                error_num += 1
                time.sleep(5)

    @staticmethod
    def _get_db_count(section: str) -> None:
        client = pymongo.MongoClient()
        db = client["HotPepperBeauty"]
        collection = db[section]
        return collection.find().count()

    @classmethod
    def get_and_save_all_data(cls, keywords: list, price_from: int, price_to: int, search_gender: str,
                              db_section: str, search_length: int) -> None:
        for keyword in keywords:
            logger.info("現在キーワード{}を検索しています。".format(keyword))
            inst = cls(freeword=keyword, price_from=price_from, price_to=price_to, search_gender=search_gender,
                       db_section=db_section)
            inst.get_page(search_length=search_length)
            logger.info("キーワード{}を終了します。".format(keyword))

