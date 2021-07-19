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
import time
import pytz


# 도시 목록 [도시이름, x좌표, y좌표] / 초단기, 동네예보
cities = [['busan', 98, 76], ['chungbuk', 69, 107], ['chungnam', 68, 100], ['daegu', 89, 90], ['daejeon', 67, 100],
          ['gangwon', 73, 134], ['gwangju', 58, 74], ['gyeongbuk', 89, 91], ['gyeonggi', 60, 120],
          ['gyeongnam', 91, 77], ['incheon', 55, 124], ['jeju', 52, 38], ['jeonbuk', 63, 89], ['jeonnam', 51, 67],
          ['sejong', 66, 103], ['seoul', 60, 127], ['ulsan', 102, 84]]


def create_csv():
    df = get_ultra()
    df.to_csv('forecast_ultra.csv', index=False, header=True)


# returns template of dataframe based on base_time
def get_template(base_time):
    # number of forecasted values
    num_forecast = 6
    target_time = base_time + datetime.timedelta(minutes=30)
    target_column = []
    for i in range(0, num_forecast):
        target_column = target_column + [target_time + datetime.timedelta(hours=i)] * len(cities)

    # create dataframe
    df = pd.DataFrame(index=np.arange(len(cities) * num_forecast),
                      columns=['base_date', 'base_time', 'target_date', 'target_time', 'city', 'city_x', 'city_y', 'forecast_temp'])
    df['base_date'] = base_time.date()
    df['base_time'] = base_time.time()
    df['target_date'] = [t.date() for t in target_column]
    df['target_time'] = [t.time() for t in target_column]
    df['city'] = [city[0] for city in cities] * num_forecast
    df['city_x'] = [city[1] for city in cities] * num_forecast
    df['city_y'] = [city[2] for city in cities] * num_forecast
    df['forecast_temp'] = None

    return df


# 초단기예보 (http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst)
def get_ultra():
    # base_time(발표시각) 설정
    # 매시간 30분에 생성되며 약 10분마다 최신 정보로 업데이트됨
    # 현재시각에서 가장 가까운 과거 30분을 base_time 으로 자동 설정
    now_ = datetime.datetime.now(pytz.timezone('Asia/Seoul'))
    now_ = datetime.datetime.combine(now_.date(), now_.time())
    if now_.minute < 30:
        base_time = now_.replace(minute=30, second=0, microsecond=0) - relativedelta(hours=1)
    else:
        base_time = now_.replace(minute=30, second=0, microsecond=0)

    df_template = get_template(base_time)
    retry_error_code = ['01', '02', '03', '04', '05']

    # try collecting data from API for 5 times
    for j in range(0, 5):
        print('Trial {} : Getting forecast_ultra based on'.format(j+1), base_time, '\n')
        api_error = False
        dfs = []

        # for every city in the list
        for i in range(0, len(cities)):
            print(cities[i][0], ': Getting ultra-fast forecast data')

            url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst'
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

            # result_code(00) : If data is appropriately collected
            if result_code == '00':
                # make a dataframe 'df_temp' that contains new data
                df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
                df_temp = df_temp[df_temp['category'] == 'T1H'].drop('category', axis=1)
                df_temp = df_temp.reset_index(drop=True)

                # set names and order of columns
                df_temp.insert(4, 'city', cities[i][0])
                df_temp = df_temp[['baseDate', 'baseTime', 'fcstDate', 'fcstTime', 'city', 'nx', 'ny', 'fcstValue']]
                df_temp.columns = ['base_date', 'base_time', 'target_date', 'target_time', 'city', 'city_x', 'city_y',
                                   'forecast_temp']

                # Data type 변환 (시간값을 str 에서 datetime type 으로 변환)
                df_temp['base_date'] = df_temp['base_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date())
                df_temp['base_time'] = df_temp['base_time'].apply(lambda x: datetime.datetime.strptime(x, '%H%M').time())
                df_temp['target_date'] = df_temp['target_date'].apply(lambda x: datetime.datetime.strptime(x, '%Y%m%d').date())
                df_temp['target_time'] = df_temp['target_time'].apply(lambda x: datetime.datetime.strptime(x, '%H%M').time())
                df_temp['city_x'] = df_temp['city_x'].apply(lambda x: int(x))
                df_temp['city_y'] = df_temp['city_y'].apply(lambda x: int(x))
                df_temp['forecast_temp'] = df_temp['forecast_temp'].apply(lambda x: float(x))

                # 완성된 데이터프레임을 dfs 리스트에 추가
                dfs.append(df_temp)
                print(cities[i][0], ': forecast_ultra data collected')
                api_error = False

            # error worth retry : retry after 2 min
            elif result_code in retry_error_code:
                print(cities[i][0], ': API Error Code {}\n'.format(result_code))
                api_error = True
                break

            # error not worth retry : return empty dataframe
            else:
                print(cities[i][0], ': Error Code {}. Critical API Error. Cancel calling API .\n'.format(result_code))
                return df_template

        # if there is an error in API, retry after 2 minutes
        if api_error:
            print('Trial {} : Failed. Error during calling API. Automatically retry in 2 min'.format(j + 1), '\n')
            time.sleep(120)
        else:
            break

    if not api_error:
        # 도시별 데이터를 종합한 데이터프레임
        df_ultra = pd.concat(dfs)
        df_ultra = df_ultra.reset_index(drop=True)

        # base_date, base_time, target_date 에 정렬 후, 인덱스 재설정
        df_ultra = df_ultra.set_index(['base_date', 'base_time', 'target_date', 'target_time'])
        df_ultra = df_ultra.sort_index(axis=0)
        df_ultra.reset_index(level=['base_date', 'base_time', 'target_date', 'target_time'], inplace=True)

        # combine the template dataframe and the collected data, then return it
        df_result = df_template.combine_first(df_ultra)
        df_result = df_result.where(pd.notnull(df_result), None)
        return df_result

    # if error in API still happens after 5 trials, return df_template (dataframe without data)
    else:
        return df_template


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'forecast_ultra'
    table_name = 'SMP.eric_{}'.format(data_name)

    # get MySQL connection information by calling getMySQLInfo() function
    MySQL_info = getMySQLInfo()
    host_name = MySQL_info['host_name']
    port = MySQL_info['port']
    db_name = MySQL_info['db_name']
    id = MySQL_info['id']
    pw = MySQL_info['pw']

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:{}/{}'.format(id, pw, host_name, port, db_name), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False, chunksize=10000)
    print('{}.csv is added to MySQL\n'.format(data_name))

    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=host_name, database=db_name)
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
    print('Updating {}'.format(table_name))

    # get MySQL connection information by calling getMySQLInfo() function
    MySQL_info = getMySQLInfo()
    host_name = MySQL_info['host_name']
    port = MySQL_info['port']
    db_name = MySQL_info['db_name']
    id = MySQL_info['id']
    pw = MySQL_info['pw']

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=host_name, database=db_name)
    except mysql.connector.Error as error:
        if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif error.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(error)

    # update MySQL data
    try:
        # get new data
        cursor = cnx.cursor()
        new_data = get_ultra()
        print('\nUltra forecast data:\n', new_data)

        # insert each row of df_ultra to MySQL
        for index, row in new_data.iterrows():
            try:
                # insert into table
                row_data = row.values.tolist()
                query_string = 'INSERT INTO {} (base_date, base_time, target_date, target_time, city, city_x, city_y, '\
                               'forecast_temp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ' \
                               'ON DUPLICATE KEY UPDATE ' \
                               'forecast_temp = IF(forecast_temp IS NULL, %s, forecast_temp);'.format(table_name)
                cursor.execute(query_string, row_data + row_data[7:8])
                cnx.commit()

                # check for changes in the MySQL table
                if cursor.rowcount == 0:
                    print('Data already exists in the MySQL table. No change was made.', row_data)
                elif cursor.rowcount == 1:
                    print('New data inserted into MySQL table.', row_data)
                elif cursor.rowcount == 2:
                    print('Null data is updated.', row_data)
                else:
                    print('Unexpected row count.', row_data)

            except mysql.connector.Error as error:
                print('Failed to insert into MySQL table. {}'.format(error))

            except:
                print("Unexpected error:", sys.exc_info(), '\n')

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
    table_name = 'SMP.eric_forecast_ultra'
    print('Deleting data in {}'.format(table_name))

    # get MySQL connection information by calling getMySQLInfo() function
    MySQL_info = getMySQLInfo()
    host_name = MySQL_info['host_name']
    port = MySQL_info['port']
    db_name = MySQL_info['db_name']
    id = MySQL_info['id']
    pw = MySQL_info['pw']

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=host_name, database=db_name)

        # delete the target
        cursor = cnx.cursor()
        cursor.execute("DELETE FROM {} WHERE id > 222;".format(table_name))
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


# get MySQL information from 'MySQL_info.txt'
def getMySQLInfo():
    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        host_name = text_file.readline().strip()
        port = text_file.readline().strip()
        db_name = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    MySQL_info = {'host_name': host_name, 'port': port, 'db_name': db_name, 'id': id, 'pw': pw}
    return MySQL_info


# main function
def main():
    # get a csv file
    # create_csv()

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()