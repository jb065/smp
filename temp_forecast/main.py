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
    # 30분 단위로 발표하지만, 45분 이후에 호출할 수 있다
    # 현재시각에서 45분을 뺀 값을 발표시각으로 설정하여 호출할 것
    base_time = datetime.datetime.now() - datetime.timedelta(minutes=45)

    # 도시별 데이터프레임을 저장할 리스트
    dfs = []

    for i in range(0, len(cities)):
        # 작업 현황 파악을 위한 출력
        print('Getting ultra-fast forecast data for', cities[i][0])

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getUltraSrtFcst'
        key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '100',
                                       quote_plus('dataType'): 'JSON',
                                       quote_plus('base_date'): base_time.strftime("%Y%m%d"),
                                       quote_plus('base_time'): base_time.strftime("%H%M"),
                                       quote_plus('nx'): cities[i][1],
                                       quote_plus('ny'): cities[i][2]})

        response = requests.get(url + queryParams)
        json_response = response.json()
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
        print('Ultra-fast forecast data collected for', cities[i][0])

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
        print('Getting village forecast data for', cities[i][0])

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst'
        key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '100',
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
        print('Village forecast data collected for', cities[i][0])

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


# 중기기온예보 (https://www.data.go.kr/data/15059468/openapi.do)
def get_mid():
    # 도시 목록 [도시이름, 도시코드]
    cities_mid = [['busan', '11H20201'], ['chungbuk', '11C10301'], ['chungnam', '11C20401'], ['daegu', '11H10701'],
              ['daejeon', '11C20401'], ['gangwon', '11D10301'], ['gwangju', '11F20501'], ['gyeongbuk', '11H10701'],
              ['gyeonggi', '11B20601'], ['gyeongnam', '11H20301'], ['incheon', '11B20201'], ['jeju', '11G00201'],
              ['jeonbuk', '11F10201'], ['jeonnam', '21F20804'], ['sejong', '11C20404'], ['seoul', '11B10101'],
              ['ulsan', '11H20101']]

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
        print('Getting mid forecast data for', cities_mid[i][0])

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
                                       quote_plus('numOfRows'): '100',
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
    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    data_name = 'forecast_ultra'
    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False)
    print('{}.csv is added to MySQL'.format(data_name))

    data_name = 'forecast_village'
    csv_data = pd.read_csv('{}.csv'.format(data_name))
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False)
    print('{}.csv is added to MySQL'.format(data_name))

    data_name = 'forecast_mid'
    csv_data = pd.read_csv('{}.csv'.format(data_name))
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False)
    print('{}.csv is added to MySQL'.format(data_name))


# update MySQL_ultra
def update_MySQL_ultra():
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
        print('\nUltra forecast data:\n', df_ultra)

        # insert each row of df_ultra to MySQL
        for index, row in df_ultra.iterrows():
            # insert into table
            query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, [index + 1] + row.values.tolist())

            # commit : make changes persistent to the database
            cnx.commit()
            # print status
            print('New data inserted into MySQL table.')

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table. {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info()[0], '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# update MySQL_village
def update_MySQL_village():
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
            query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, [index + 1] + row.values.tolist())

            # commit : make changes persistent to the database
            cnx.commit()
            # print status
            print('New data inserted into MySQL table.')

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table. {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info()[0], '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# update MySQL_mid
def update_MySQL_mid():
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
            query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, [index + 1] + row.values.tolist())

            # commit : make changes persistent to the database
            cnx.commit()
            # print status
            print('New data inserted into MySQL table.')

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table. {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info()[0], '\n')

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

    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)

    except:
        print("Unexpected error:", sys.exc_info()[0], '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# 동네(단기)예보 (https://www.data.go.kr/data/15057682/openapi.do)
def test_village():
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
        print('Getting village forecast data for', cities[i][0])

        url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getVilageFcst'
        key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                       quote_plus('pageNo'): '1',
                                       quote_plus('numOfRows'): '100',
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
        print('Village forecast data collected for', cities[i][0])

    # 도시별 데이터를 종합한 데이터프레임
    df_village = pd.concat(dfs)

    df_village.to_csv('test.csv', index=True, header=True)

    """
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
    """


# main function
def main():
    # MySQL
    # toMySQL()
    # update_MySQL_ultra()
    # update_MySQL_village()
    update_MySQL_mid()
    # deleteMySQL()

    # TEST
    # get_ultra().to_csv('test_ultra.csv', index=False, header=True)
    # get_village().to_csv('test_village.csv', index=False, header=True)
    # get_mid().to_csv('test_mid.csv', index=False, header=True)
    # test_village()


if __name__ == '__main__':
    main()