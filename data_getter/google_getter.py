# 標準ライブラリ
import argparse
import requests
import logging
import time

# サードパーティライブラリ
from readability.readability import Document
from urlextract import URLExtract
from termcolor import cprint
import lxml.html
import lxml
import pymongo

# 自作ライブラリ
from data_saver import mongodb_saver

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class DataGetterFromGoogle:
    def __init__(self, keyword: str, section: str) -> None:
        self.base_url = "https://www.google.com/search?"
        self.section = section
        self.keyword = keyword.split(" ")

    def get_page(self, search_length: int) -> None:
        for search_num in range(0, search_length, 10):
            error_num = 0
            while True:

                if error_num >= 20:
                    logger.warning("同一のURLに対するエラーが20回続いたので、このURLからの取得を終了します。")
                    return None

                try:
                    req = requests.get(self.base_url, params={"q": self.keyword, "start": search_num})
                    logging.info("現在Google検索の{}ページ目(url: {})を取得しています。".format(int(search_num / 10),
                                                                              req.url))
                    cprint("Now Get Google pages {}: {}".format(int(search_num / 10), req.url), "yellow")
                    if int(req.status_code) == 503:
                        logger.error("Error 503: Googleの異常アクセス検知に検知されました。")
                        cprint("Google detected the abnormal network traffic", "red")
                        time.sleep(60 * 60)

                    elif int(req.status_code) != 200:
                        logger.warning("Error {}: ページの取得が出来ませんでした。".format(req.status_code))
                        cprint("Now Get {} Error".format(req.status_code), "red")
                        error_num += 1
                        time.sleep(5)

                    else:
                        html = lxml.html.fromstring(req.text)
                        nextlist = [i.get("href") for i in html.cssselect("h3.r > a")]
                        error_num = 0
                        break

                except ConnectionError:
                    logger.warning("Connection Errorが発生しました。")
                    cprint("Now Get ConnectionError: Error_num{}".format(error_num), "red")
                    error_num += 1
                    time.sleep(10 * 60)

            if not nextlist:
                logger.info("Google検索結果が尽きました。")
                cprint("Results of the search has run out", "red")
                return None

            for url_base in nextlist:
                try:
                    url = URLExtract().find_urls(url_base)[0].split("&sa=")[0]

                except IndexError:
                    logger.info("Google検索結果が尽きました。")
                    cprint("Can't recognize url: {}".format(url_base), color="red")
                    continue

                logger.info("現在url:{}を取得しています。".format(url))
                cprint("Now specific page :{}".format(url), color="green")
                title, content = self.get_data(url)
                if (title != 0) and (content != 0):
                    pass
                    # TODO mongodbに保存するためのクラスを作成する。
                    # save_mongodb(section=self.section, title=title, content=content)

            logger.info("Googleの異常アクセス検知から逃れるため、15秒待っています。")
            cprint("Now Waiting 15 secs for avoiding Google`s server detection...", "yellow")
            time.sleep(15)

    @staticmethod
    def get_data(url: str) -> tuple:
        error_num = 0
        while True:

            if error_num >= 10:
                logger.warning("同一のURLに対するエラーが20回続いたので、このURLからの取得を終了します。")
                cprint("Finished Because error_num reached 10 times", "red")
                return 0, 0

            try:
                req = requests.get(url)
                if int(req.status_code) == 503:
                    logger.error("Error 503: このクラアントではアクセスを禁止されています。")
                    cprint("Google detected the abnormal network traffic", "red")
                    time.sleep(60 * 60)

                elif int(req.status_code) != 200:
                    logger.error("Error {}: このページを取得出来ません。".format(req.status_code))
                    cprint("Now Get StatusCode{}: Error_num{}".format(req.status_code, error_num), "red")
                    return 0, 0

                else:
                    html = req.text
                    break

            except ConnectionError:
                logger.warning("Connection Errorが発生しました。")
                cprint("Now Get ConnectionError: Error_num{}".format(error_num), "red")
                error_num += 1
                time.sleep(5)
        try:
            document = Document(html)
            content_html = document.summary()
            content_text = lxml.html.fromstring(content_html).text_content().strip()
            short_title = document.short_title()
            return short_title, content_text

        except:
            return 0, 0

    @staticmethod
    def _get_db_count(section: str) -> None:
        client = pymongo.MongoClient()
        db = client["Google"]
        collection = db[section]
        return collection.find().count()

    @classmethod
    def get_and_save_all_data(cls, keywords: list, section: str, search_length: int) -> None:
        for keyword in keywords:
            logger.info("現在キーワード{}を検索しています。".format(keyword))
            inst = cls(keyword=keyword, section=section)
            inst.get_page(search_length=search_length)
            logger.info("キーワード{}を終了します。".format(keyword))


def handle_commandline():
    parser = argparse.ArgumentParser(
        prog="google_getter.py",
        usage="google_getter.py keyword section search_length",
        description="""このスクリプトは、googleから取得するスクリプトです。""",
        epilog="end",
        add_help=True

    )
    parser.add_argument("-s", "--section",
                        help="保存するmongodbのセクションを決定するパラメーターです。dbnameはGoogleで統一されています。",
                        type=str, default="google_search")
    parser.add_argument("-sl", "--search_length",
                        help="どれくらいの検索件数を取得するかを指定するパラメーターです。",
                        type=int, default=100)
    parser.add_argument("-k", "--keyword", help="取得するデータの開始日を指定するパラメーターです。", nargs="+", type=str)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = handle_commandline()
    DataGetterFromGoogle.get_and_save_all_data(keywords=[" ".join(args.keyword)], section=args.section,
                                               search_length=args.search_length)
