# 필요한 모듈 불러오기
import requests
from urllib.parse import urlencode, quote_plus
import pandas as pd
import datetime
import numpy as np
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys


# 도시 목록 [도시이름, x좌표, y좌표] / 초단기, 동네예보
cities = [['busan', 98, 76], ['chungbuk', 69, 107], ['chungnam', 68, 100], ['daegu', 89, 90], ['daejeon', 67, 100],
          ['gangwon', 73, 134], ['gwangju', 58, 74], ['gyeongbuk', 89, 91], ['gyeonggi', 60, 120],
          ['gyeongnam', 91, 77], ['incheon', 55, 124], ['jeju', 52, 38], ['jeonbuk', 63, 89], ['jeonnam', 51, 67],
          ['sejong', 66, 103], ['seoul', 60, 127], ['ulsan', 102, 84]]


# 동네(단기)예보 (https://www.data.go.kr/data/15057682/openapi.do)
def get_village():
    # base_time(발표시각) 설정
    # 예보 갱신 시간 : 매일 05시,11시,17시,23시
    now_ = datetime.datetime.now()
    if now_ < now_.replace(hour=5, minute=0, second=0, microsecond=0):
        base_time = (now_ - datetime.timedelta(days=1)).replace(hour=23, minute=0, second=0, microsecond=0)
    elif now_ < now_.replace(hour=11, minute=0, second=0, microsecond=0):
        base_time = now_.replace(hour=5, minute=0, second=0, microsecond=0)
    elif now_ < now_.replace(hour=17, minute=0, second=0, microsecond=0):
        base_time = now_.replace(hour=11, minute=0, second=0, microsecond=0)
    elif now_ < now_.replace(hour=23, minute=0, second=0, microsecond=0):
        base_time = now_.replace(hour=17, minute=0, second=0, microsecond=0)
    else:
        base_time = now_.replace(hour=23, minute=0, second=0, microsecond=0)

    # 도시별 데이터프레임을 저장할 리스트
    dfs = []

    for i in range(0, len(cities)):
        # 작업 현황 파악을 위한 출력
        print(cities[i][0], ': Getting village forecast data')

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst'
        key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '999',
                                       quote_plus('dataType'): 'JSON',
                                       quote_plus('base_date'): base_time.strftime("%Y%m%d"),
                                       quote_plus('base_time'): base_time.strftime("%H%M"),
                                       quote_plus('nx'): cities[i][1],
                                       quote_plus('ny'): cities[i][2]})

        response = requests.get(url + queryParams)
        json_response = response.json()
        df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
        df_temp = df_temp[df_temp['category'] == 'T3H'].drop('category', axis=1)

        # index 초기화 (0부터 시작하도록)
        df_temp = df_temp.reset_index(drop=True)

        # 도시명을 나타내는 'city' column 추가
        df_temp.insert(4, 'city', cities[i][0])
        # column 순서, 이름을 format 에 맞게 변경
        df_temp = df_temp[['baseDate', 'baseTime', 'fcstDate', 'fcstTime', 'city', 'nx', 'ny', 'fcstValue']]
        temp_column = ['base_date', 'base_time', 'target_date', 'target_time', 'city', 'city_x', 'city_y', 'forecast_temp']
        df_temp.columns = temp_column

        # Data type 변환 (시간값을 str 에서 datetime type 으로 변환)
        df_temp['base_date'] = df_temp['base_date'].apply(lambda x : datetime.datetime.strptime(x, '%Y%m%d').date())
        df_temp['base_time'] = df_temp['base_time'].apply(lambda x: datetime.datetime.strptime(x, '%H%M').time())
        df_temp['target_date'] = df_temp['target_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date())
        df_temp['target_time'] = df_temp['target_time'].apply(lambda x: datetime.datetime.strptime(x, '%H%M').time())
        df_temp['city_x'] = df_temp['city_x'].apply(lambda x: float(x))
        df_temp['city_y'] = df_temp['city_y'].apply(lambda x: float(x))
        df_temp['forecast_temp'] = df_temp['forecast_temp'].apply(lambda x: float(x))

        # 완성된 데이터프레임을 dfs 리스트에 추가
        dfs.append(df_temp)

        # 작업 현황 파악을 위한 출력
        print(cities[i][0], ': Village forecast data collected')

    # 도시별 데이터를 종합한 데이터프레임
    df_village = pd.concat(dfs)
    # index 초기화 (0부터 시작하도록)
    df_village = df_village.reset_index(drop=True)

    # base_date, base_time, target_date 에 정렬 후, 인덱스 재설정
    df_village = df_village.set_index(['base_date', 'base_time', 'target_date', 'target_time'])
    df_village = df_village.sort_index(axis=0)
    df_village.reset_index(level=['base_date', 'base_time', 'target_date', 'target_time'], inplace=True)

    # 'base_date' and 'target_date' columns get converted to pandas._libs.tslibs.timestamps.Timestamp type
    # convert them back to datetime.date type
    df_village['base_date'] = df_village['base_date'].astype(str)
    df_village['base_date'] = df_village['base_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    df_village['target_date'] = df_village['target_date'].astype(str)
    df_village['target_date'] = df_village['target_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())

    # return the collected df_village dataframe
    return df_village


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'forecast_village'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False, chunksize=10000)
    print('{}.csv is added to MySQL\n'.format(data_name))

    # connect to MySQL
    table_name = 'SMP.eric_{}'.format(data_name)
    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)

    # set datatype and features / set an unique key
    try:
        cursor = cnx.cursor()

        # set datatype and features
        query_string = "ALTER TABLE {} " \
                       "CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT, " \
                       "CHANGE COLUMN `base_date` `base_date` DATE NOT NULL, " \
                       "CHANGE COLUMN `base_time` `base_time` TIME NOT NULL, " \
                       "CHANGE COLUMN `target_date` `target_date` DATE NOT NULL, " \
                       "CHANGE COLUMN `target_time` `target_time` TIME NOT NULL, " \
                       "CHANGE COLUMN `city` `city` VARCHAR(20) NOT NULL, " \
                       "CHANGE COLUMN `city_x` `city_x` INT NOT NULL, " \
                       "CHANGE COLUMN `city_y` `city_y` INT NOT NULL, " \
                       "CHANGE COLUMN `forecast_temp` `forecast_temp` FLOAT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx " \
                       "(base_date, base_time, target_date, target_time, city);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Unique Key(uidx) is set\n')

    except mysql.connector.Error as error:
        print('Failed set datatype and features of MySQL table {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info(), '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# update MySQL_village
def updateMySQL():
    table_name = 'SMP.eric_forecast_village'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)

    # update MySQL data
    try:
        cursor = cnx.cursor()
        # get new dataframe
        df_village = get_village()
        print('\nVillage forecast data:\n', df_village)

        # insert each row of df_ultra to MySQL
        for index, row in df_village.iterrows():
            # insert into table
            query_string = 'INSERT INTO {} (base_date, base_time, target_date, target_time, city, city_x, city_y, ' \
                           'forecast_temp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, row.values.tolist())

            # commit : make changes persistent to the database
            cnx.commit()
            # print status
            print('New data inserted into MySQL table.')

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table. {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info(), '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# delete rows in MySQL
def deleteMySQL():
    table_name = 'SMP.eric_forecast_mid'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

        # delete the target
        cursor = cnx.cursor()
        cursor.execute(cursor.execute("DELETE FROM {}".format(table_name)))
        cnx.commit()
        print('Deletion completed.')

    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)

    except:
        print("Unexpected error:", sys.exc_info(), '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# main function
def main():
    # MySQL
    toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()