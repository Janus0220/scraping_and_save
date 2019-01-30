# 標準ライブラリ
import os
import logging

# サードパーティライブラリ
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# ロガーの設置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# モジュールへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# settingへのパス
SETTINGS_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings")
# データ保管場所へのパス
DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tmp")


class GoogleDriveGetter:
    def __init__(self):
        # OAuth認証を行う
        gauth = GoogleAuth(settings_file=os.path.join(SETTINGS_PATH, "setting.yaml"))
        gauth.LoadClientConfigFile(os.path.join(SETTINGS_PATH, "client_secrets.json"))
        drive = GoogleDrive(gauth)
        # OAuth認証が成功したインスタンスを用いる。
        self.drive = drive

    def upload_exsisting_file(self, data_path, dir_id, data_type):
        # ファイルが存在するのかまたは指定されたファイルが存在しなければValueErrorを発生させる。
        if not os.path.isfile(data_path):
            logger.error("アップロードするファイルが存在しません。")
            raise ValueError("This is not a file or Not exists")

        logger.info("現在ファイル{}を{}にアップロードしています。".format(os.path.basename(data_path), dir_id))
        file_tmp = self.drive.CreateFile({"title": os.path.basename(data_path),
                                          "parents": [{"id": dir_id}],
                                          "mimeType": data_type})
        file_tmp.SetContentFile(data_path)
        file_tmp.Upload()
        logger.info("{}のGoogle Driveへのアップロードが完了しました。".format(data_path))

    def get_data_list(self, data_type=None, parents_id="root"):
        if not data_type:
            data_list = self.drive.ListFile(
                {"q": "trashed=false and '{}' in parents".format(parents_id), "maxResults": 50}).GetList()
        else:
            data_list = self.drive.ListFile({"q": "trashed=false and mimeType='{}' and '{}' in parents"
                                            .format(parents_id, data_type), "maxResults": 50}).GetList()
        return {data_list[i]["title"]: data_list[i]["id"] for i in range(len(data_list))}

    def download_data(self, data_name, data_type, dir_name, data_dir):
        # TODO rootディレクトリから一階層までのディレクトリからしかファイルを取得できないのを改善する。
        # Rootのディレクトリを取得する。
        data_list_for_dir = self.get_data_list(data_type="application/vnd.google-apps.folder")

        # Rootのディレクトリの中に指定されたディレクトリが存在するかどうかを判定する。
        if not dir_name in data_list_for_dir:
            logger.error("Googleドライブ上に{}というディレクトリが存在しませんでした。".format(dir_name))
            ValueError("DIR {} doesn't exists......".format(dir_name))

        parents_id = data_list_for_dir[dir_name]

        # 指定されたディレクトリの中のファイル名を取得する。
        data_list_for_file = self.get_data_list(data_type=data_type, parents_id=parents_id)

        # 指定されたディレクトリの中にdata_nameが存在するかどうかを判定する。
        if not data_name in data_list_for_file:
            ValueError("File {} doesn't exists.......".format(data_name))
        else:
            os.chdir(data_dir)
            file_id = data_list_for_file[data_name]
            file = self.drive.CreateFile({'id': file_id})
            file.GetContentFile(data_name)
            logger.info("Google Driveから{}を{}にダウンロードしました。".format(data_name, data_dir))


if __name__ == '__main__':
    data_path_tmp = r"C:/Users/智矢/Downloads/12月13日分析.pdf"
    dir_id_tmp = "1rYuZONcAOm9msxPJtr9O20nUKD6LACvu"
    data_type_tmp = "application/pdf"
    inst = GoogleDriveGetter()
    GoogleDriveGetter().upload_exsisting_file(data_path=data_path_tmp, dir_id=dir_id_tmp, data_type=data_type_tmp)
