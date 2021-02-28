import os
import shutil
import glob
import io
import codecs
import datetime
import tempfile
import zipfile
import csv
import re
import base64
import json
import logging
logging.basicConfig(level=logging.INFO)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import googleapiclient
from googleapiclient.http import MediaFileUpload
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client.client import GoogleCredentials


class JRDBToGCS():
    def __init__(self, account, password, debug=False):
        self.account = account
        self.password = password
        self._metadata = {}
        if debug:
            logging.basicConfig(level=logging.DEBUG)

    def get_race_date_list(self):
        # レース日一覧の取得
        # 指定した日が開催日かどうかチェックするために利用
        r = requests.get(
            'http://www.jrdb.com/member/datazip/Kab/index.html',
            auth=(self.account, self.password)
            )
        logging.debug(r.status_code)
        soup = BeautifulSoup(r.content, 'html.parser')
        return [a.get_text()[3:9] for a in soup.find_all('ul')[1].find_all('a')]

    def is_race_date(self, date):
        # 指定した日が開催日かどうか
        yymmdd = str(date.year)[2:] + str(date.month).zfill(2) + str(date.day).zfill(2)
        if yymmdd not in self.get_race_date_list():
            logging.info("Not race day. skip: {}".format(yymmdd))
            return False
        else:
            return True

    def get_and_extract_zip(self, data_type, date, dest_path):
        # ファイル種別毎にDL+解答
        yymmdd = str(date.year)[2:] + str(date.month).zfill(2) + str(date.day).zfill(2)
        if not self.is_race_date(date):
            return False

        master_data_first_dirs = {"cza": "Cs", "kza": "Ks"}
        if data_type in master_data_first_dirs.keys():
            first_dir = master_data_first_dirs[data_type]
        else:
            first_dir=data_type[0].upper() + data_type[1:]
        remote_path = 'http://www.jrdb.com/member/datazip/{first_dir}/{year}/{TYPE}{YYMMDD}.zip'.format(
            first_dir=first_dir,
            year=date.year,
            TYPE=data_type.upper(),
            YYMMDD=yymmdd
        )
        logging.info('download from: {}'.format(remote_path))
        r = requests.get(
            remote_path,
            auth=(self.account, self.password)
            )
        if r.status_code != 200:
            raise RuntimeError('error: status_code={}, url={}'.format(r.status_code, r.url))
        with tempfile.NamedTemporaryFile(mode='w+b', suffix='.zip') as fp:
            fp.write(r.content)
            shutil.copyfileobj(r.raw, fp)
            if not zipfile.is_zipfile(fp):
                raise RuntimeError('error: file format is not valid')
            with zipfile.ZipFile(fp, mode='r') as z:
                z.extractall(path=dest_path)
        return True

    @property
    def metadata(self):
        if len(self._metadata) == 0: #初回のみ
            for table in ['sed', 'srb', 'kta']:
                self._metadata[table] = pd.read_gbq('''
                    SELECT
                        *
                    FROM
                        metadata.{}
                    ORDER BY index
                    '''.format(table), project_id='ca5-jrdb', dialect='standard')
        return self._metadata

    def convert_text_to_csv(self, src_fp, dest_fp, metadata):
        logging.debug('convert sjis.txt -> utf8.csv')
        writer = csv.writer(dest_fp, quoting=csv.QUOTE_ALL) #デフォルトのQUOTE_MINIMALだとうまく行かない場合がある
        writer.writerow(metadata.name.values)
        while True:
            raw_line = src_fp.readline()
            if not raw_line:
                break
            csv_line = []
            b_total = 0
            for b in metadata.byte.values:
                try:
                    cell = raw_line[b_total:b_total+int(b)].rstrip()
                    csv_line.append(cell)
                except Exception as e:
                    logging.error(e)
                    logging.error(raw_line[b_total:b_total+int(b)])
                    break
                finally:
                    b_total += int(b)
            writer.writerow(csv_line)

    def download_and_convert_and_upload(self, zip_type, date):
        with tempfile.TemporaryDirectory() as tmpdir:
            if not self.get_and_extract_zip(zip_type, date, tmpdir):
                return
            # 参考: https://qiita.com/kai_kou/items/4b754c61ac225daa0f7d
            credentials = GoogleCredentials.get_application_default()
            gcs_service = build(
                'storage',
                'v1',
                http=credentials.authorize(Http()),
                cache_discovery=False)
            
            for src_path in glob.glob(os.path.join(tmpdir, '*')):
                with open(src_path, 'r',  encoding='cp932') as src_fp, tempfile.NamedTemporaryFile(mode='w') as converted_fp:
                    src_file_name =  os.path.basename(src_path)
                    file_type = src_file_name[0: re.search('[0-9]', src_file_name).span()[0]].lower()
                    logging.debug('file_type: {}'.format(file_type))
                    if self.metadata.get(file_type, None) is not None:
                        self.convert_text_to_csv(src_fp, converted_fp, self.metadata[file_type])
                        dest_file_name = src_file_name.replace('.txt', '.csv')
                    else:
                        logging.info('skip {}'.format(file_type))
                        continue
                    media = MediaFileUpload(converted_fp.name, 
                                            mimetype='text/plain',
                                            resumable=True)
                    date_str = date.strftime('%Y-%m-%d')
                    file_prefix = re.search(r'^([A-Z]+)', dest_file_name).group().lower()
                    dest_full_path = os.path.join('raw', f'dt={date_str}', f'zip_type={zip_type}', f'file_type={file_prefix}', f'{dest_file_name}')
                    gcs_request = gcs_service.objects().insert(bucket='ca5-jrdb',
                                                        name=dest_full_path,
                                                        media_body=media)
                    gcs_response = None
                    while gcs_response is None:
                        try:
                            _, gcs_response = gcs_request.next_chunk()
                        except Exception as e:
                            logging.error(e)
                            break
                    logging.info('Upload complete: {}'.format(dest_full_path))


def main(data, context):
    logging.info('data:')
    logging.info(data)
    logging.info('context:')
    logging.info(context)
    '''
    dataの入り方例:
    {
        '@type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage',
        'attributes': None,
        'data': 'スケジューラのペイロードで指定した文字列をBASE64変換したもの'
    }
    '''
    decoded_data = json.loads(base64.b64decode(data['data']).decode())

    jrdb_to_gcs = JRDBToGCS(decoded_data['account'], decoded_data['password'], debug=data.get('debug', False))
    today = datetime.datetime.now()
    if decoded_data['mode'] == 'confirmed': #前週の確定値取得 (前週木-今週水)
        start_date = today - datetime.timedelta(days=7)
        end_date = today - datetime.timedelta(days=1)
    elif decoded_data['mode'] == 'previous': #前日情報(=翌日の速報値)
        start_date = today + datetime.timedelta(days=1)
        end_date = start_date + datetime.timedelta(days=1)
    else: # マニュアルモード
        start_date = datetime.datetime.strptime(decoded_data['start_date'], '%Y-%m-%d')
        if not data.get('start_date', None):
            end_date = start_date + datetime.timedelta(days=1)
        else:
            end_date = datetime.datetime.strptime(decoded_data['end_date'], '%Y-%m-%d')
    logging.info(f'start_date:{start_date}, end_date:{end_date}')

    current_date = start_date
    while current_date <= end_date:
        if jrdb_to_gcs.is_race_date(current_date):
            print('start download: {}'.format(current_date))
            for zip_type in [
                # 前日データ
                #'paci',

                # 成績データ
                'sed', #'skb', 
                #'tyb', 'hjc'

                # masterデータ
                # 直近のものしか残ってないかも
                'kza', 'cza', 'kta'
                ]:
                try:
                    jrdb_to_gcs.download_and_convert_and_upload(zip_type, current_date)
                except Exception as e:
                    logging.error(e)
                    logging.error('failed: zip_type={}'.format(zip_type))
                    pass
        current_date += datetime.timedelta(days=1)


def test(data, context):
    print("data:")
    print(data)
    decoded_data = json.loads(base64.b64decode(data['data']).decode())
    print("decoded_data:")
    print(decoded_data)
    print("context:")
    print(context)
    jrdb_to_gcs = JRDBToGCS(decoded_data['account'], decoded_data['password'], debug=True)
    print("get_race_date_list:")
    print(jrdb_to_gcs.get_race_date_list())
    print("check_date:")
    start_date = datetime.datetime.strptime(decoded_data['start_date'], '%Y-%m-%d')
    print(jrdb_to_gcs.is_race_date(start_date))
    print("get_and_extract_zip:")
    data_type = "sed"
    print(jrdb_to_gcs.get_and_extract_zip(data_type, start_date, '/tmp/jrdbtest'))
    start_date_ymd = start_date.strftime('%y%m%d')
    print("property:")
    print(jrdb_to_gcs.metadata)
    print("convert_text_to_csv:")
    with open(f'/tmp/jrdbtest/SED{start_date_ymd}.txt', 'r', encoding='cp932') as src_fp: # inputはcp932指定
        with open(f'/tmp/jrdbtest/SED{start_date_ymd}_utf8.csv', 'w') as dest_fp: # outputはutf8(default)
            print(jrdb_to_gcs.convert_text_to_csv(src_fp, dest_fp, jrdb_to_gcs.metadata['sed']))
    print("download_and_convert_and_upload:")
    print(jrdb_to_gcs.download_and_convert_and_upload('sed', start_date))
