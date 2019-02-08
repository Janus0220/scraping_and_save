# coding:utf-8
# 標準ライブラリ
import os
import sys
import re
import datetime
import logging

# サードパーティライブラリ
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import gspread
import openpyxl
import numpy as np

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")

# 自作ライブラリ
sys.path.append(MODULE_PATH)
from data_getter.redash_getter import RedashQueryGetter
from data_getter.google_drive_getter import GoogleDriveGetter


# ロギングの基本設定
logger = logging.getLogger(__name__)


class GoogleDocumentWriter:
    def __init__(self, drive_auth=False):
        # 認証を行う。
        credentials = self.get_credentials()
        self.client = gspread.authorize(credentials)
        if drive_auth:
            self.drive_instance = GoogleDriveGetter()

    def write_data(self, op_spread_sheet_id, data, worksheet_name="シート1", spread_sheet_path=None):
        # パスの中に目的のファイルが存在するかどうかを確認する。
        parents_id = "root"
        if not op_spread_sheet_id:
            for parent, child in zip(spread_sheet_path.split("/"), spread_sheet_path.split("/")[1:]):
                try:
                    parents_id = self.drive_instance.get_data_list(parents_id=parents_id)[child]
                except AttributeError:
                    logger.error("Google Driveの認証設定の項目がFalseです。インスタンス作成時にdrive_authをTrueにして下さい。")
                    raise ValueError("Drive Authorization is Essential! You have to check 'drive_auth=True' when init")
                except KeyError:
                    logger.error("指定されたパスの{}から{}が発見できませんでした。".format(parent, child))
                    raise ValueError("Can't Find {} in {}".format(os.path.basename(spread_sheet_path),
                                                                  spread_sheet_path))
            # スプレッドシートIDの獲得
            spread_sheet_id = parents_id
        else:
            spread_sheet_id = op_spread_sheet_id

        # スプレッドシートの獲得
        gsfile = self.client.open_by_key(spread_sheet_id)

        # ワークシートの獲得
        worksheet = gsfile.worksheet(worksheet_name)

        # 以前に書かれた項目の削除
        worksheet.clear()

        # 行列の形をexcelの順序にする
        excel_column = openpyxl.utils.get_column_letter(int(np.shape(data)[1]))
        excel_row = np.shape(data)[0]

        # セルの値をまとめて更新する
        data = data.fillna(0)
        cell_list = worksheet.range("A1:{}{}".format(excel_column, excel_row))
        for cell in cell_list:
            if cell.row-1 == 0:
                cell.value = data.columns[cell.col-1]

            elif isinstance(data.iloc[cell.row-1][cell.col-1], np.int64):
                cell.value = int(data.iloc[cell.row-1][cell.col-1])

            elif isinstance(data.iloc[cell.row-1][cell.col-1], float):
                cell.value = float(data.iloc[cell.row-1][cell.col-1])

            elif isinstance(data.iloc[cell.row-1][cell.col-1], str):
                if "00:00:00" in re.split("[_ ]", data.iloc[cell.row - 1][cell.col - 1]):
                    cell.value = "_".join([i for i in re.split("[_ ]", data.iloc[cell.row - 1][cell.col - 1])
                                           if i != "00:00:00"])
                else:
                    cell.value = str(data.iloc[cell.row - 1][cell.col - 1])
            else:
                logger.warning("{}は想定されていない型です。".format(type(data.iloc[cell.row - 1][cell.col - 1])))
                cell.value = str(data.iloc[cell.row - 1][cell.col - 1])

        # セルの値の更新を確定する。
        worksheet.update_cells(cell_list=cell_list)
        logger.info("ファイルID:{} に書き込みが完了しました。".format(spread_sheet_id))

    def write_continuing_data(self, op_spread_sheet_id, data, worksheet_name="シート1", spread_sheet_path=None):
        pass

    @staticmethod
    def get_credentials():
        # 認証を行う。
        scorps = "https://www.googleapis.com/auth/spreadsheets"
        client_secret_path = os.path.join(SETTINGS_PATH, 'client_secrets.json')
        application_name = 'Google Sheets API Python Quickstart'
        credential_path = os.path.join(SETTINGS_PATH, 'sheets.googleapis.com-python-quickstart.json')
        store = Storage(credential_path)
        credentials = store.get()

        if not credentials or credentials.invalid:
            logger.warning("認証情報を格納するjsonファイルが存在しません。")
            flow = client.flow_from_clientsecrets(client_secret_path, scorps)
            flow.user_agent = application_name
            credentials = tools.run_flow(flow, store)
            logger.info("認証情報を格納するjsonファイルを{}に格納しました。".format(credential_path))

        logger.info("Google Spread SheetのAPI認証に成功しました。")
        return credentials

    @staticmethod
    def output_date_list(now):
        date_list = []
        dif_day = 1
        while len(date_list) < 3:
            day_tmp = now - datetime.timedelta(days=dif_day)
            if day_tmp.weekday() == 4:
                date_list.append([(day_tmp - datetime.timedelta(days=6)).strftime("%Y-%m-%d")
                                 , day_tmp.strftime("%Y-%m-%d")])
            dif_day += 1
        return date_list

    @classmethod
    def add_redash_data_for_lion_ss(cls, op_spread_sheet_id, query_id):
        inst = cls(drive_auth=False)
        client = "pairs"
        now = datetime.datetime.now()
        date_list = cls.output_date_list(now)
        sheet_name = ["redash393(前週)", "redash393(前々週)", "redash393(前々々週)"]
        for date, sheet in zip(date_list, sheet_name):
            logger.info("現在{}から{}までのredashクエリ{}のデータを{}に書き込んでいます。".format(query_id, sheet, date[0],
                                                                          date[1]))
            redash_data = RedashQueryGetter.get_and_save_all_data(start_date=date[0],
                                                                  end_date=date[1],
                                                                  query_id=query_id,
                                                                  client=client)
            inst.write_data(spread_sheet_path=" ", op_spread_sheet_id=op_spread_sheet_id,
                            worksheet_name=sheet, data=redash_data)


def main():
    formatter = '%(levelname)s - %(asctime)s - From %(name)s : %(message)s'
    logging.basicConfig(level=logging.INFO, format=formatter, filename=os.path.join(MODULE_PATH, "log",
                                                                                    "google_document_writer.log"))
    logger.info("{}を実行します。".format(__name__))

    # 引数
    op_spread_sheet_id_tmp = "1UkiTC263xPc_VNuvartwnkFyTqoSlTtijWsL4LVQjCU"
    query_id_tmp = "393"

    # クラスメゾット
    GoogleDocumentWriter.add_redash_data_for_lion_ss(op_spread_sheet_id=op_spread_sheet_id_tmp, query_id=query_id_tmp)
    logger.info("{}を終了します。".format(__name__))


if __name__ == '__main__':
    main()
