# 標準ライブラリ
import sys
import datetime
import requests
import logging
import math
import time

# サードパーティライブラリ
from urlextract import URLExtract
import pandas as pd
import lxml.html
import lxml

# 自作のライブラリ
sys.path.append("..")
from data_saver.mongodb_saver import MongoSaver

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DataGetterFromHotPepperBeauty:
    """HotPepperBeautyをスクレイピングするクラスです。"""

    def __init__(self, freeword: str, search_gender: str, db_name: str, db_section: str) -> None:
        self.base_url = "https://beauty.hotpepper.jp/CSP/bt/freewordSearch/?"
        self.freeword = freeword
        self.date = datetime.datetime.now().strftime("%y-%m-%d-%H-%M")
        self.search_gender = search_gender
        self.inst_saver = MongoSaver(db_name=db_name, db_section=db_section)

    def get_page(self, search_length: int) -> pd.DataFrame():
        """　~/freewordSearch/のURLから各情報を取得します。この領域は、robots.txtからスクレイピング自体がDISALLOWであるので、
        　　 十分に間隔を開けて、スクレイピングする事を心掛けます。"""
        save_dict = {"取得日時": [], "検索語句": [], "店名": [], "住所": [], "定休日": [],
                     "お店のホームページ": [], "席数": [], "スタッフ数": [], "スタッフ募集": [],
                     "ホットペッパービューティ上のHP": [], "電話番号": [], "口コミ総数": [], "総合": [],
                     "雰囲気": [], "接客サービス": [], "技術・仕上がり": [], "メニュー・料金": []}

        for search_num in range(1, search_length):
            iter_num = 1
            num_store = 0
            error_num = 0
            while True:
                try:
                    req = requests.get(self.base_url, params={"freeword": self.freeword,
                                                              "searchGender": self.search_gender,
                                                              "pn": search_num})
                    logger.info("HotPepperBeautyの{}ページ目(url: {})を取得しています。".format(search_num, req.url))
                    if int(req.status_code) != 200:
                        logger.warning("Error {}: ページの取得が出来ませんでした。".format(req.status_code))
                        error_num += 1
                        time.sleep(5)

                    else:
                        html = lxml.html.fromstring(req.text)
                        if iter_num == 1:
                            num_store = int([i.text_content() for i in html.cssselect("span.numberOfResult")][0])
                        nextlist = [i.get("href") for i in html.cssselect("h3.slcHead.cFix > a")]
                        break

                except ConnectionError:
                    logger.warning("Connection Errorが発生しました。")
                    error_num += 1
                    time.sleep(10 * 60)

                except Exception as e:
                    logger.error("関数get_pageにおいて予期せぬエラーが発生したため{}ページ目で"
                                 "取得を終了します。".format(search_num))
                    logger.error("詳細な内容\n{}".format(e.args))
                    return self.inst_saver.conv_all_data_to_dataframe(save_dict=save_dict, key="取得日時",
                                                                      value=self.date)

            for url in nextlist:
                try:
                    logger.info("現在url:{}を取得しています。".format(url))
                    result = self.get_data(url, freeword=self.freeword, date=self.date)
                except Exception as e:
                    logger.error("関数get_dataにおいて予期せぬエラーが発生したため{}ページ目で"
                                  "取得を終了します。".format(search_num))
                    logger.error("詳細な内容\n{}".format(e.args))
                    return self.inst_saver.conv_all_data_to_dataframe(save_dict=save_dict, key="取得日時",
                                                                      value=self.date)

                if (result["店名"] != 0) and (result["住所"] != 0):
                    self.inst_saver.save_data(result=result)
                    logger.info("店名: {店名}の情報の取得を完了しました。".format(**result))
                    # 個々のページを取得する間のブレークタイム
                    time.sleep(2)

            iter_num += 1
            if int(math.ceil(num_store / 30)) < iter_num or search_length <= iter_num:
                logger.info("HotPepperBeautyの検索結果が尽きました。")
                return self.inst_saver.conv_all_data_to_dataframe(save_dict=save_dict, key="取得日時", value=self.date)
            # 一括のページを取得する間のブレークタイム
            logger.info("新たな一括ページを取得するために、2秒間スリープします。")
            time.sleep(5)

        # if文しなかったときのための予備
        return self.inst_saver.conv_all_data_to_dataframe(save_dict=save_dict, key="取得日時", value=self.date)

    def get_data(self, url: str, freeword: str, date: str) -> dict:
        result_dict = {"取得日時": date, "検索語句": freeword, "店名": None, "住所": None, "定休日": None,
                       "お店のホームページ": None, "席数": None, "スタッフ数": None, "スタッフ募集": None,
                       "ホットペッパービューティ上のHP": None, "電話番号": None, "口コミ総数": 0, "総合": 0,
                       "雰囲気": 0, "接客サービス": 0, "技術・仕上がり": 0, "メニュー・料金": 0}
        error_num = 0
        extractor = URLExtract()
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
                    html = lxml.html.fromstring(req.text)
                    result_tmp = {i.text_content(): j.text_content() for i, j in
                                  zip(html.cssselect("th.w120"), html.cssselect("th.w120 ~ td"))}
                    result_dict["店名"] = [i.text_content() for i in html.cssselect("p.detailTitle > a")][0]
                    result_dict["ホットペッパービューティ上のHP"] = url
                    kuchikomi_dict = self.get_kuchikomi(url.split("?")[0] + "review/")
                    result_tmp.update(kuchikomi_dict)

                    for key in result_tmp.keys():
                        if key in result_dict.keys():
                            if key == "電話番号":
                                result_dict[key] = self.get_tel(url.split("?")[0] + "tel/")
                            elif key == "お店のホームページ" or key == "ホットペッパービューティ上のHP" or key == "スタッフ募集":
                                result_dict[key] = extractor.find_urls(result_tmp[key])[0]
                            else:
                                result_dict[key] = result_tmp[key]

                    return result_dict

            except ConnectionError:
                logger.warning("Connection Errorが発生しました。")
                error_num += 1
                time.sleep(5)

    @staticmethod
    def get_tel(url):
        error_num = 0
        while True:
            if error_num >= 10:
                logger.warning("同一のURL({})に対するエラーが20回続いたので、このURLからの取得を終了します。".format(url))
                return None

            try:
                req = requests.get(url)
                if int(req.status_code) != 200:
                    logger.error("Error {}: このページ{}を取得出来ません。".format(req.status_code, url))
                    return None

                else:
                    html = lxml.html.fromstring(req.text)
                    logger.info("電話番号の取得に成功しました。")
                    return [i.text_content() for i in html.cssselect("td.fs16.b")][0].split("\xa0")[0]

            except ConnectionError:
                logger.warning("{}でConnection Errorが発生しました。".format(url))
                error_num += 1
                time.sleep(5)

            except IndexError:
                logger.info("{}は電話番号が存在しません。".format(url))
                return None

    @staticmethod
    def get_kuchikomi(url):
        result_dict = {"総合": 0, "雰囲気": 0, "接客サービス": 0, "技術・仕上がり": 0, "メニュー・料金": 0}
        total_kuchikomi = 0
        iter_num = 1
        error_num = 0

        while True:
            review_url = url + "PN{}.html".format(iter_num)
            if error_num >= 10:
                logger.warning("同一のURL({})に対するエラーが20回続いたので、このURLからの取得を終了します。".format(review_url))
                return result_dict

            try:
                req = requests.get(review_url)
                if int(req.status_code) != 200:
                    logger.error("Error {}: このページ{}を取得出来ませんでした。".format(req.status_code, review_url))
                    error_num += 1
                    continue

                else:
                    html = lxml.html.fromstring(req.text)
                    if iter_num == 1:
                        total_kuchikomi = int([i.text_content() for i in html.cssselect("span.numberOfResult")][0])
                        result_dict["口コミ総数"] = total_kuchikomi

                    logger.info("URL {} における口コミ({}ページ目)を取得しています。".format(review_url, iter_num))
                    result_dict["総合"] += sum([int(i.text_content()) for i in html.cssselect("span.mL5.mR10.fgPink")])
                    result_dict["雰囲気"] += sum([int(j.text_content()) for i, j in
                                               enumerate(html.cssselect("span.mL10.fgPink.b")) if (i + 1) % 4 == 1])
                    result_dict["接客サービス"] += sum([int(j.text_content()) for i, j in
                                                  enumerate(html.cssselect("span.mL10.fgPink.b")) if (i + 1) % 4 == 2])
                    result_dict["技術・仕上がり"] += sum([int(j.text_content()) for i, j in
                                                   enumerate(html.cssselect("span.mL10.fgPink.b")) if (i + 1) % 4 == 3])
                    result_dict["メニュー・料金"] += sum([int(j.text_content()) for i, j in
                                                   enumerate(html.cssselect("span.mL10.fgPink.b")) if (i + 1) % 4 == 0])

            except ConnectionError:
                logger.warning("{}でConnection Errorが発生しました。".format(review_url))
                error_num += 1
                time.sleep(5)
                continue

            except IndexError:
                logger.info("{}は口コミが存在しません。".format(url))
                return result_dict

            except requests.exceptions.ChunkedEncodingError:
                logger.warning("{}の取得中に{}が発生しました。".format(review_url,
                                                          "requests.exceptions.ChunkedEncodingError"))
                error_num += 1
                time.sleep(5)
                continue

            iter_num += 1
            time.sleep(1)
            if int(math.ceil(total_kuchikomi / 30)) < iter_num:
                result_dict["総合"] = result_dict["総合"] / total_kuchikomi
                result_dict["雰囲気"] = result_dict["雰囲気"] / total_kuchikomi
                result_dict["接客サービス"] = result_dict["接客サービス"] / total_kuchikomi
                result_dict["技術・仕上がり"] = result_dict["技術・仕上がり"] / total_kuchikomi
                result_dict["メニュー・料金"] = result_dict["メニュー・料金"] / total_kuchikomi
                return result_dict

    @classmethod
    def get_and_save_all_data(cls, keywords: list, search_gender: str, search_length: int, db_name: str,
                              db_section: str) -> None:
        for keyword in keywords:
            logger.info("現在キーワード{}を検索しています。".format(keyword))
            inst = cls(freeword=keyword, search_gender=search_gender, db_section=db_section, db_name=db_name)
            inst.get_page(search_length=search_length)
            logger.info("キーワード{}を終了します。".format(keyword))


def main():
    keywords_list = ["東京都"]
    search_gender = "ALL"
    search_length = 1000
    db_name = "HotPepperBeauty"
    db_section = "test"
    DataGetterFromHotPepperBeauty.get_and_save_all_data(keywords=keywords_list, search_gender=search_gender,
                                                        search_length=search_length, db_section=db_section,
                                                        db_name=db_name)


if __name__ == '__main__':
    formatter = '%(levelname)s - %(asctime)s - From %(name)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=formatter)
    logger.info("{}を実行します。".format(__name__))
    main()
