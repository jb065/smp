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

# 도시 목록 [도시이름, 도시코드]
cities_mid = [['busan', '11H20201'], ['chungbuk', '11C10301'], ['chungnam', '11C20401'], ['daegu', '11H10701'],
              ['daejeon', '11C20401'], ['gangwon', '11D10301'], ['gwangju', '11F20501'], ['gyeongbuk', '11H10701'],
              ['gyeonggi', '11B20601'], ['gyeongnam', '11H20301'], ['incheon', '11B20201'], ['jeju', '11G00201'],
              ['jeonbuk', '11F10201'], ['jeonnam', '21F20804'], ['sejong', '11C20404'], ['seoul', '11B10101'],
              ['ulsan', '11H20101']]


# 중기기온예보 (https://www.data.go.kr/data/15059468/openapi.do)
def get_mid():
    # tmFc 설정
    # 중기예보는 매일 6시, 18시에 발표되기 때문에, 현재시간이 아닌 해당시간을 input 으로 넣어야한다
    now_ = datetime.datetime.now()
    if now_ < now_.replace(hour=6, minute=0, second=0, microsecond=0):
        tmFc = (now_ - datetime.timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
    elif now_ < now_.replace(hour=18, minute=0, second=0, microsecond=0):
        tmFc = now_.replace(hour=6, minute=0, second=0, microsecond=0)
    else:
        tmFc = now_.replace(hour=18, minute=0, second=0, microsecond=0)

    # 작업 현황 출력
    print('Getting mid forecast based on', tmFc, '\n')

    # 도시별 데이터프레임을 저장할 리스트
    dfs = []

    for i in range(0, len(cities_mid)):
        # 작업 현황 파악을 위한 출력
        print(cities_mid[i][0], ': Getting mid forecast data')

        url = 'http://apis.data.go.kr/1360000/MidFcstInfoService/getMidTa'
        key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

        # 첫번째 지역일 경우, 새로운 데이터프레임 설정
        if i == 0:
            # 저장할 데이터프레임 columns 설정 (tmFc, regId, D+3, D+4 ... D+10)
            cols = ['city', 'base_time']
            for j in range(3, 11):
                cols.append((tmFc + datetime.timedelta(days=j)).strftime("%Y%m%d"))
            df_result = pd.DataFrame(columns=cols)

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '999',
                                       quote_plus('dataType'): 'JSON',
                                       quote_plus('regId'): cities_mid[i][1],  # 목표지점
                                       quote_plus('tmFc'): tmFc.strftime("%Y%m%d%H%M")})  # 발표시각

        response = requests.get(url + queryParams)
        json_response = response.json()
        df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
        df_temp = df_temp[
            ['taMin3', 'taMax3', 'taMin4', 'taMax4', 'taMin5', 'taMax5', 'taMin6', 'taMax6', 'taMin7', 'taMax7',
             'taMin8', 'taMax8', 'taMin9', 'taMax9', 'taMin10', 'taMax10']]

        # 날짜별 데이터값을 갖는 리스트
        temp_data = df_temp.loc[0].tolist()

        # format 에 맞는 데이터프레임 생성
        column_format = ['base_date', 'base_time', 'target_date', 'city', 'city_code', 'temp_min', 'temp_max']
        df_format = pd.DataFrame(columns=column_format)

        # 데이터 추가
        target_date = tmFc.date() + relativedelta(days=3)
        for j in range(0, 8):
            new_data = [tmFc.date(), tmFc.time(), target_date, cities_mid[i][0], cities_mid[i][1], temp_data[2 * j], temp_data[2 * j + 1]]
            df_format.loc[len(df_format)] = new_data
            target_date = target_date + relativedelta(days=1)
        dfs.append(df_format)

        print(cities_mid[i][0], ': Mid forecast data collected')

    # 도시별 데이터를 종합한 데이터프레임
    df_mid = pd.concat(dfs)
    # index 초기화 (0부터 시작하도록)
    df_mid = df_mid.reset_index(drop=True)

    # Data type 변환 (datetime, float type 으로 변환)
    df_mid['temp_min'] = df_mid['temp_min'].apply(lambda x: float(x))
    df_mid['temp_max'] = df_mid['temp_max'].apply(lambda x: float(x))

    # base_date, base_time, target_date 에 정렬 후, 인덱스 재설정
    df_mid = df_mid.set_index(['base_date', 'base_time', 'target_date'])
    df_mid = df_mid.sort_index(axis=0)
    df_mid.reset_index(level=['base_date', 'base_time', 'target_date'], inplace=True)

    # 'base_date' and 'target_date' columns get converted to pandas._libs.tslibs.timestamps.Timestamp type
    # convert them back to datetime.date type
    df_mid['base_date'] = df_mid['base_date'].astype(str)
    df_mid['base_date'] = df_mid['base_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    df_mid['target_date'] = df_mid['target_date'].astype(str)
    df_mid['target_date'] = df_mid['target_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())

    # return the collected dataframe
    return df_mid


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'forecast_mid'

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
                       "CHANGE COLUMN `city` `city` VARCHAR(20) NOT NULL, " \
                       "CHANGE COLUMN `city_code` `city_code` VARCHAR(8) NOT NULL, " \
                       "CHANGE COLUMN `temp_min` `temp_min` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `temp_max` `temp_max` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx " \
                       "(base_date, base_time, target_date, city);".format(table_name)
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


# update MySQL_mid
def updateMySQL():
    table_name = 'SMP.eric_forecast_mid'

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
        df_mid = get_mid()
        print('\nMid forecast data:\n', df_mid)

        # insert each row of df_ultra to MySQL
        for index, row in df_mid.iterrows():
            # insert into table
            query_string = 'INSERT INTO {} (base_date, base_time, target_date, city, city_code, temp_min, temp_max) ' \
                           'VALUES (%s, %s, %s, %s, %s, %s, %s);'.format(table_name)
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