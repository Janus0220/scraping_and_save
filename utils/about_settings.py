# 標準ライブラリ
import os
import logging

# サードパーティライブラリ
import configparser

# モジュール、データディレクトリへのパス
MODULE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# ロガーの設置
logger = logging.getLogger(__name__)


def make_setting(section, key):
    logger.info("{}のsetting.iniを作成します。".format(section))
    os.chdir(MODULE_PATH)
    config = configparser.ConfigParser()
    try:
        config.read("setting.ini")
    except FileNotFoundError:
        logger.info("setting.iniが{}に存在しません。".format(MODULE_PATH))
        setting_file = open("setting.ini", "w")
        setting_file.close()
        config.read("setting.ini")

    config[section] = {}
    for key_tmp in key:
        tmp = input("{}を入力して下さい。: ".format(key_tmp))
        config[section][key_tmp] = str(tmp)
        logger.info("section: {}, key: {}にvalue: {}を設定しました。".format(section, key_tmp, tmp))

    with open(os.path.join(MODULE_PATH, "setting.ini"), "w") as configfile:
        config.write(configfile)

    logger.info("{}のsetting.iniの作成を終了します。".format(section))