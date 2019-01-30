# 標準ライブラリ
from configparser import ConfigParser
import argparse
import datetime
import os
import time
import logging

# サードパーティライブラリ
from redash_dynamic_query import RedashDynamicQuery
import pandas as pd

# 自作ライブラリ
from utils.about_settings import make_setting

# ロガーの設置
logger = logging.getLogger(__name__)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class RedashQueryGetter:
    """
    このクラスは、Redashからデータを取得するためのクラスです。
    Arguments
    :param start_date: str 開始日を指定するパラメーター
    :param end_date: str　終了日を指定するパラメーター
    :param query_id: int RedashのクエリIDを指定するパラメーター
    :param client: str 設定ファイルの取得項目を指定するパラメーター
    :return None
    """

    def __init__(self, start_date: str, end_date: str, query_id: int, client: str) -> None:
        # 設定ファイルから設定データの取得
        logger.info("setting.iniから情報を取得しています。")
        while True:
            os.chdir(MODULE_PATH)
            config = ConfigParser()
            try:
                config.read("setting.ini")
                api_key = config[client]["api_key"]
                data_source_id = int(config[client]["data_source_id"])
                end_point = config[client]["end_point"]
                break
            except KeyError:
                logger.warning("setting.iniにデータが存在しません。")
                make_setting(section=client, key=["end_point", "api_key", "data_source_id"])

        # 3ヶ月以上の場合、データベース側の負荷も考えて処理を分割するために3ヶ月ごとに分ける。
        self.start_date, self.end_date, self._sep_date = self._split_date(args_start_date=start_date,
                                                                          args_end_date=end_date)
        self.redash = RedashDynamicQuery(endpoint=end_point, apikey=api_key, data_source_id=data_source_id)
        self.query_id = query_id
        self.client = client

    def _get_query(self):
        """
        開始日と終了日の期間におけるデータを取得し、pandasのデータフレームを出力するインスタンスメソッドです。
        :return: pandas.DataFrame
        """
        result = None
        # 3ヶ月以上の場合、処理を複数回に分けるが、3ヶ月以内の時、単独の処理とする。
        if self._sep_date:
            # TODO プロセスベースの並列処理を後で実装する。おそらく実行時間としては必要がないが学習のため
            for i, date in enumerate(self._sep_date):
                bind = {"start_date": date[0], "end_date": date[1], "gender": "M-F"}
                result_tmp = self.redash.query(query_id=self.query_id, bind=bind, as_csv=True)
                result_tmp_csv = self._conv_csv_to_dataframe(result_tmp)
                logger.info("{}のクエリID{}のデータにおいて{}から{}までの{}列のデータを取得しました。".format(self.client,
                                                                                self.query_id, date[0], date[1],
                                                                                len(result_tmp_csv)))
                if not i:
                    result = result_tmp_csv
                else:
                    result = pd.concat([result_tmp_csv, result], ignore_index=True)
                time.sleep(10)
            logger.info("{}のクエリID{}のデータにおいて合計{}列のデータを取得しました。".format(self.client,
                                                                     self.query_id, len(result)))
            return result.reset_index(drop=True)
        # 単独の処理
        else:
            bind = {"start_date": self.start_date, "end_date": self.end_date, "gender": "M-F"}
            result_tmp = self.redash.query(query_id=self.query_id, bind=bind, as_csv=True)
            result = self._conv_csv_to_dataframe(result_tmp)
            logger.info("{}のクエリID{}のデータにおいて合計{}列のデータを取得しました。".format(self.client,
                                                                     self.query_id, len(result)))
            return result.reset_index(drop=True)

    @staticmethod
    def _save_df(dir_name: str, result: pd.DataFrame(), client: str, query_id: int,
                 name="Redash_data", excel=True) -> None:
        """
        取得したデータフレームを指定されたディレクトリに保存する静的メソッドです。
        :param dir_name: str
        :param result: pd.DataFrame
        :param client: str
        :param query_id: int
        :param name: str
        :param excel: bool
        :return: None
        """
        os.chdir(dir_name)
        date_now = datetime.datetime.now().strftime("%Y-%m-%d")
        save_data_name = "{}_{}_{}_{}".format(name, client, query_id, date_now)
        if excel:
            result.to_excel(save_data_name + ".xlsx")
        else:
            result.to_csv(save_data_name + ".csv")

    @staticmethod
    def _conv_csv_to_dataframe(result: str) -> pd.DataFrame():
        """
        取得した文字列をパースして、一時的に同ディレクトリのデータに保存し、pandas.read_csv()で取得する静的メソッドです。
        :param result: str
        :return: pandas.DataFrame
        """
        os.chdir(DATA_PATH)
        with open("data_tmp.csv", "w") as csvfile:
            csvfile.write(result)
        result_csv = pd.read_csv("data_tmp.csv")
        return result_csv

    @staticmethod
    def _split_date(args_start_date: str, args_end_date: str) -> tuple:
        """
        開始日、終了日が三カ月以上ならば、それを分割する静的メソッドです。
        :param args_start_date:
        :param args_end_date:
        :return:
        """
        # 日時の計算のためにdatetimeに変換する。
        try:
            start_date = datetime.datetime.strptime(args_start_date, "%Y-%m-%d")
            end_date = datetime.datetime.strptime(args_end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("'%Y-%m-%d'のフォーマットに調整して下さい。")

        # 三カ月以上の取得は、サーバーの負担になるので取得する日付を分割する。
        if (end_date - start_date).days >= 90:
            st_date = start_date
            result_list = []
            while True:
                ed_date = st_date + datetime.timedelta(days=60)
                result_list.append([st_date, ed_date])
                st_date = ed_date + datetime.timedelta(days=1)
                if (ed_date - end_date).days >= 0:
                    result_list[-1][1] = end_date
                    break
            _sep_date = result_list
        else:
            _sep_date = []
        return start_date, end_date, _sep_date

    @staticmethod
    def save_mongodb():
        pass

    @classmethod
    def get_and_save_all_data(cls, query_id: int, client: str, start_date: str, end_date: str, dir_path=None) \
            -> pd.DataFrame():
        """
        データ取得までの一連の処理を実装したクラスメソッドです。
        :param query_id:
        :param client:
        :param start_date:
        :param end_date:
        :param dir_path:
        :return:
        """
        logger.info("Redashからデータを取得しています。")
        inst = cls(query_id=query_id, start_date=start_date, end_date=end_date, client=client)
        result = inst._get_query()
        if dir_path:
            try:
                os.chdir(dir_path)
                logger.info("{}にデータを保存します。".format(dir_path))
                cls._save_df(dir_path, result, client=client, query_id=query_id)
            except FileNotFoundError:
                os.chdir(DATA_PATH)
                logger.info("{}に保存が失敗したので、{}にデータを保存します。".format(dir_path, DATA_PATH))
                cls._save_df(DATA_PATH, result, client=client, query_id=query_id)
        logger.info("Redashからデータ取得を完了しました。")
        return result


def handle_commandline():
    parser = argparse.ArgumentParser(
        prog="redash_getter.py",
        usage="redash_getter.py start_date end_date query_id client --dir_name dir_path",
        description="""このスクリプトは、Redashからデータをpandasデータフレーム形式でデータを取得します。
        またオプションで取得したデータフレームをcsv, xlsx形式で保存する事が可能です。
        引数として、データ取得開始日、データ取得終了日、クライアント、RedashのクエリID、保存するディレクトリ名を取ります。""",
        epilog="end",
        add_help=True

    )
    parser.add_argument("start_date", help="取得するデータの開始日を指定するパラメーターです。", type=str)
    parser.add_argument("end_date", help="取得するデータの終了日を指定するパラメーターです。", type=str)
    parser.add_argument("query_id", help="取得するクエリ番号を指定するパラメーターです。", type=int)
    parser.add_argument("client", help="取得するデータのクライアントを指定するパラメータです。MatchとPairsを想定しています",
                        type=str, choices=["match", "pairs"])
    parser.add_argument("-d", "--dir_name",
                        help="取得したデータフレームを保存するディレクトリを指定し、指定しない場合保存しません。",
                        type=str, default=None)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = handle_commandline()
    RedashQueryGetter.get_and_save_all_data(query_id=args.query_id, client=args.client, start_date=args.start_date,
                                            end_date=args.end_date, dir_path=args.dir_name)
