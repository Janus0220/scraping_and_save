# 標準ライブラリ
from configparser import ConfigParser
from requests import exceptions
from datetime import datetime
from itertools import chain
import argparse
import logging
import time
import json
import os

# サードパーティライブラリ
from requests_oauthlib import OAuth1Session
from pymongo import MongoClient
from termcolor import cprint
import pymongo
import pandas as pd

# 自作ライブラリ
from utils.about_settings import make_setting

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class TwitterGetter:
    def __init__(self, section: str):
        self.section = section
        while True:
            os.chdir(MODULE_PATH)
            config = ConfigParser()
            try:
                config.read("setting.ini")
                self.consumer_key = config[section]["CONSUMER_KEY"]
                self.consumer_secret = config[section]["CONSUMER_SECRET"]
                self.access_token = config[section]["ACCESS_TOKEN"]
                self.access_token_secret = config[section]["ACCESS_TOKEN_SECRET"]
            except KeyError:
                logging.warning("{}の情報が設定ファイルに存在しません。".format("Twitter_api"))
                make_setting(section="Twitter", key=["CONSUMER_KEY", "CONSUMER_SERCRET",
                                                     "ACCESS_TOKEN", "ACCESS_TOKEN_SECRET"])

    def get_twitter(self, keyword: str, max_id: int, since_id: int, url="Normal", pr=True) -> list:
        """
        :param keyword: 取得するキーワードを指定するパラメーターです。
        :param max_id: max_idは、データを取得する際に以前にどこまで取得したかを示すTweetIDを指定するパラメーターです。
        :param since_id: since_idは、データを取得する際にどこから取得するかを示すTweetIDを指定するパラメーターです。
        :param url: urlはどのAPIからデータを取得するかを指定するパラメーターです。無料のAPIを使用する場合はNormalを指定してください。
        :param pr: prは取得したデータの経過を出力するかどうかを指定するパラメーターです。
        :return: result: リスト型で要素には辞書型のデータが存在しています。
        """
        error_time = 0

        if url == "Normal":
            url = "https://api.twitter.com/1.1/search/tweets.json"
            count = 100
        elif url == "30_days":
            url = "https://api.twitter.com/1.1/tweets/search/30day/my_env_name.json"
            count = 100
        elif url == "full":
            url = "https://api.twitter.com/1.1/tweets/search/fullarchive/my_env_name.json"
            count = 500
        else:
            raise ValueError("URL argument is not correct")

        params = {"q": " ".join([str(keyword), "#".format(str(keyword)), "exclude:retweets", "exclude:nativeretweets"]),
                  "count": count, "result_type": "mixed", "Language": "ja", "max_id": max_id, "since_id": since_id}

        twitter = OAuth1Session(self.consumer_key, self.consumer_secret, self.access_token,
                                self.access_token_secret)
        result = {}

        while True:
            error_time += 1
            try:
                req = twitter.get(url, params=params)
                if req.status_code == 200:
                    timeline = json.loads(req.text)
                    result = []
                    for i, tweet in enumerate(timeline["statuses"]):
                        result_tmp = {"user": tweet["user"]["name"], "text": tweet["text"], "time": tweet["created_at"],
                                      "id": tweet["id"], "keyword": keyword}
                        result.append(result_tmp)
                        if pr & (i % 50 == 0):
                            cprint("#{}".format(tweet["text"]), color="yellow")
                    break

                elif req.status_code == 429:
                    logging.info("リクエストの上限に達しました。15分間待ちます。")
                    cprint("Reached limited requests... Wait 15 mins", color="yellow")
                    time.sleep(15 * 60)

                else:
                    logging.error("Error {}が発生しました。".format(req.status_code))
                    raise ValueError("ERROR: {}".format(req.status_code))

            except exceptions.ConnectionError:
                logging.warning("Connection Errorが発生しました。")
                cprint("Connection Error.... Check your Network Environment: ErrorTime{}".format(error_time), "red")
                time.sleep(5 * 60)
                if not error_time % 20:
                    logging.error("Connection Errorのため作業を終了します。")
                    cprint("10 hours have passed... Finished this process.", "red")
                    break
                continue

        logging.info("ツイートID{}からキーワード@{}のデータを取得しました。".format(keyword, max_id))
        cprint("Getting data about @{} from tweets between {} is succeeded".format(keyword, max_id), "yellow")
        return result

    def save_twitter(self, result, set_index=True):
        # TODO MongoDBに保存するクラスを作成する。
        client = MongoClient()
        db = client["collect_data"]
        collection = db[self.section]
        if set_index:
            collection.create_index("text")

        collection.insert_many(result)
        logging.info("Mongodbへのデータの書き込みが完了しました。")

    @staticmethod
    def to_excel(result, keyword):
        # TODO Excelに保存するクラスを作成する。
        result_all = {"user": [], "text": [], "time": [], "id": [], "keyword": []}
        for tmp in chain.from_iterable(result):
            for key in result_all.keys():
                result_all[key].append(str(tmp[key]))

        df = pd.DataFrame(result_all)
        str_now = datetime.strftime(datetime.now(), "%y_%m_%d_%h")
        df.to_excel("Twitter__keyword＠{}_date{}.xlsx".format(keyword, str_now))
        logging.info("データ{}を{}へ保存しました。".format("Twitter_tweetdata__keyword{}_date{}.xlsx".format(keyword,
                                                                                                 str_now), DATA_PATH))

    @staticmethod
    def _get_db_data(section):
        client = MongoClient()
        db = client["collect_data"]
        collection = db[section]
        return collection.find().count()

    @staticmethod
    def find_data(section, keyword, num, url=None):
        client = MongoClient()
        db = client["collect_data"]
        collection = db[section]
        result = {"time":[], "user":[], "text":[], "keyword":[]}
        for value in collection.find({"keyword": keyword}).sort('id', pymongo.DESCENDING).limit(num):
            result["time"].append(value["time"])
            result["user"].append(value["user"])
            result["text"].append(value["text"])
            result["keyword"].append(value["keyword"])
        df = pd.DataFrame(result)
        if not url:
            df.to_excel("tweet_data_keyword#{}.xlsx".format(keyword))
        return df

    @staticmethod
    def _get_recent_id(section, key_word):
        client = MongoClient()
        db = client["collect_data"]
        collection = db[section]
        recent_id = [i for i in collection.find({"keyword": key_word}).sort('id', pymongo.DESCENDING).limit(1)]
        if not recent_id:
            return 0
        else:
            return recent_id[0]["id"]

    @classmethod
    def get_and_save_all_data(cls, section, key_word, pr, excel=False):
        cprint("Collecting data about @{} is initializing... Now Total_columns is {}".format(key_word,
                                                                                            cls._get_db_data(section)),
               color="blue")
        before_columns_num = cls._get_db_data(section)
        loop_num = 0
        max_id = -1
        before_max_id = 10
        total_tweets = 0
        set_unique = True
        since_id = cls._get_recent_id(section=section, key_word=key_word)
        result_for_csv = []
        while True:
            if not loop_num:
                set_unique = False

            c_inst = cls(section)
            result = c_inst.get_twitter(keyword=key_word, max_id=max_id, since_id=since_id, pr=pr)

            if (not result) or (before_max_id == max_id):
                cprint("Finished collecting data.....  Get {} columns".format(cls._get_db_data(section) -
                                                                             before_columns_num), "blue")
                os.chdir(DATA_PATH)
                cls.to_excel(keyword=key_word, result=result_for_csv)
                break
            else:
                if excel:
                    result_for_csv.append(result)

                c_inst.save_twitter(result, set_index=set_unique)
                total_tweets += len(result)
                loop_num += 1
                before_max_id = max_id
                max_id = result[-1]["id"]
        return cls._get_db_data(section) - before_columns_num


def handle_commandline():
    parser = argparse.ArgumentParser(
        prog="twitter_getter.py",
        usage="twitter_getter.py keyword section",
        description="""検索語keywordをmongodbに保存する。この時、sectionで指定した場所にdbを保存する。""",
        epilog="end",
        add_help=True

    )
    parser.add_argument("keyword", help="取得するキーワードを指定するパラメーターです。", type=str)
    parser.add_argument("section", help="取得したデータのmongodbにおける保存するsectionを指定するパラメーターです。", type=str)
    parser.add_argument("-p", "--print", help="取得したデータを出力するかどうかを指定するパラメーターです。",
                        type=bool, choices=[True, False], default=False)
    parser.add_argument("-e", "--excel", help="excelに出力するかどうかを決定するパラメーターです。",
                        type=bool, choices=[True, False], default=False)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = handle_commandline()
    TwitterGetter.get_and_save_all_data(section=args.section, key_word=args.keyword, pr=args.print, excel=args.excel)
