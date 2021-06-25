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


# 초단기예보 (https://www.data.go.kr/data/15057682/openapi.do)
def get_ultra():
    # base_time(발표시각) 설정
    # 매시간 30분에 생성되며 약 10분마다 최신 정보로 업데이트됨
    # 현재시각에서 가장 가까운 30분을 base_time 으로 자동 설정
    base_time = datetime.datetime.now()

    # 도시별 데이터프레임을 저장할 리스트
    dfs = []

    # for every city in the list
    for i in range(0, len(cities)):
        # 작업 현황 파악을 위한 출력
        print(cities[i][0], ': Getting ultra-fast forecast data')

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getUltraSrtFcst'
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
        result_code = json_response['response']['header']['resultCode']

        # result_code(3) : If base_time is set to future time
        if result_code == 3:
            print(cities[i][0], ': Error in calling API (No data)\n')
        # result_code(99) : If base_time is set to time that is past more than a day
        elif result_code == 99:
            print(cities[i][0], ': Error in calling API (API provides data for the last 1 day)\n')
        # result_code(0) : If data is appropriately collected
        else:
            df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
            df_temp = df_temp[df_temp['category'] == 'T1H'].drop('category', axis=1)

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

            # 작업 현황 출력
            print(cities[i][0], ': Ultra-fast forecast data collected')

    # if there are any dataframe collected, merge them
    if len(dfs) > 1:
        # 도시별 데이터를 종합한 데이터프레임
        df_ultra = pd.concat(dfs)
        # index 초기화 (0부터 시작하도록)
        df_ultra = df_ultra.reset_index(drop=True)

        # base_date, base_time, target_date 에 정렬 후, 인덱스 재설정
        df_ultra = df_ultra.set_index(['base_date', 'base_time', 'target_date', 'target_time'])
        df_ultra = df_ultra.sort_index(axis=0)
        df_ultra.reset_index(level=['base_date', 'base_time', 'target_date', 'target_time'], inplace=True)

        # 'base_date' and 'target_date' columns get converted to pandas._libs.tslibs.timestamps.Timestamp type
        # convert them back to datetime.date type
        df_ultra['base_date'] = df_ultra['base_date'].astype(str)
        df_ultra['base_date'] = df_ultra['base_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
        df_ultra['target_date'] = df_ultra['target_date'].astype(str)
        df_ultra['target_date'] = df_ultra['target_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())

        return df_ultra

    else:
        return 'No data is collected'


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'forecast_ultra'

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
                       "CHANGE COLUMN `forecast_temp` `forecast_temp` FLOAT NULL DEFAULT NULL, " \
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


# update MySQL_ultra
def updateMySQL():
    table_name = 'SMP.eric_forecast_ultra'

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
        df_ultra = get_ultra()

        if type(df_ultra) == str:
            print('updateMySQL cancelled : No data is collected\n')

        else:
            print('\nUltra forecast data:\n', df_ultra)

            # insert each row of df_ultra to MySQL
            for index, row in df_ultra.iterrows():
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

    # get_ultra().to_csv('test.csv', index=True, header=True)


if __name__ == '__main__':
    main()