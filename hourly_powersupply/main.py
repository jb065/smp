import datetime
import numpy as np
import requests
from urllib.parse import urlencode, quote_plus
import pandas as pd
import xml.etree.ElementTree as ET
import os
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys
import time
import pytz


# merge csv files
def merge_csv(csv_merged):
    # csv_merged : csv file of merged data

    # list of dataframes
    dfs = []
    print('Merging csv files to {}'.format(csv_merged))

    # for all csv files
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for f in files:
        filename, ext = os.path.splitext(f)
        if ext == '.csv':
            df = pd.read_csv(f)
            df.columns = ['ctime', 'supply_capacity', 'demand', 'peak_demand', 'reserve', 'reserve_margin',
                            'operational_reserve', 'operational_reserve_ratio']

            # collect data for each hour
            num_index = len(df.index)
            for i in range(0, num_index):
                if df.loc[i].at['ctime'] % 10000 != 0:
                    df = df.drop(i)

            # 'cdate' : str -> datetime.date | 'ctime' : str -> datetime.time
            df.insert(0, 'cdate', df['ctime'].tolist())
            df['cdate'] = df['cdate'].apply(lambda x: datetime.datetime.strptime(str(x)[0:14], '%Y%m%d%H%M%S').date())
            df['ctime'] = df['ctime'].apply(lambda x: datetime.datetime.strptime(str(x)[0:14], '%Y%m%d%H%M%S').time())

            dfs.append(df)
            print(filename, ': formatted')

    # df_merged : merged dataframe with 'id' column
    df_merged = pd.concat(dfs)
    df_merged = df_merged.reset_index(drop=True)
    df_merged.index = np.arange(1, len(df_merged) + 1)
    df_merged.index.name = 'id'

    # save as a new csv file
    df_merged.to_csv(csv_merged, index=True, header=True)
    print('Merging completed to file', csv_merged)


# returns a list of times with wrong data
def filter_wrong_time(df):
    df_past = df  # 확인하고 싶은 데이터프레임
    wrong_time = []  # 누락된 시간의 인덱스을 저장할 리스트

    # 데이터프레임 서칭 후, 적절하지 않은 시간대 확인하여 wrong_time 리스트에 저장
    for i in range(0, len(df_past.index) - 1):
        date1 = df_past.loc[i].at['cdate']
        date2 = df_past.loc[i + 1].at['cdate']
        time1 = df_past.loc[i].at['ctime']
        time2 = df_past.loc[i + 1].at['ctime']
        datetime1 = datetime.datetime.combine(date1, time1)
        datetime2 = datetime.datetime.combine(date2, time2)

        # 확인중인 시간대와 다음 시간대의 차이가 1시간이 아니면, 리스트에 추가
        if (datetime2 - datetime1) != datetime.timedelta(hours=1, minutes=0, seconds=0):
            wrong_time.append(i)

    # 누락된 시간대의 인덱스 출력
    print('Index with wrong_time:', wrong_time)

    # 누락된 시간대의 인덱스 리스트 반환
    return wrong_time


# 데이터에서 누락된 시간대 수정
def fix_wrong_time(csv_name):
    # csv_name : name of the csv file

    df = pd.read_csv(csv_name)
    print('Fixing time values in', csv_name)

    # 'cdate' : str -> datetime.date | 'ctime' : str -> datetime.time
    df['cdate'] = df['cdate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    df['ctime'] = df['ctime'].apply(lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time())

    # list of wrong time values
    wrong_time = filter_wrong_time(df)

    if len(wrong_time) == 0:
        print('All time values are appropriate.')
        return

    # fix the time values that are inappropriate until all time values are appropriate
    while len(wrong_time) != 0:
        # wrong_time 리스트에 저장되어 있는 시간대 다음 시간을 데이터프레임에 추가
        for i in range(0, len(wrong_time)):
            date1 = df.loc[wrong_time[i] + i].at['cdate']
            time1 = df.loc[wrong_time[i] + i].at['ctime']
            datetime1 = datetime.datetime.combine(date1, time1)
            datetime2 = datetime1 + datetime.timedelta(hours=1, minutes=0, seconds=0)  # 누락된 시간
            # 누락된 시간의 데이터를 저장하는 new_data 리스트에 date 와 time 추가
            new_data = [0, datetime2.date(), datetime2.time()]
            # 나머지 값은 'nan' 값 부여
            for j in range(0, len(df.columns) - 3):
                new_data.append(np.NaN)
            # new_data 를 누락된 시간의 row 에 추가
            temp = df[df.index > (wrong_time[i] + i)]
            df = df[df.index <= (wrong_time[i] + i)]
            df.loc[len(df)] = new_data
            df = df.append(temp, ignore_index=True)

        wrong_time = filter_wrong_time(df)  # 수정 후, 누락된 시간이 있는지 다시 한번 확인 (연속으로 2개가 누락된 경우를 방지)

    # 날짜가 수정되면 id column 없애고 index 를 id column 으로 설정
    df = df.drop('id', axis=1)
    df.index = np.arange(1, len(df) + 1)
    df.index.name = 'id'

    df.to_csv(csv_name, index=True, header=True)

    # 작업 현황 파악을 위한 출력
    print('A new dataframe with appropriate time values is saved in a csv file,', csv_name)


# 새로운 데이터 업데이트
def update():
    url = 'https://openapi.kpx.or.kr/openapi/sukub5mMaxDatetime/getSukub5mMaxDatetime'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='
    queryParams = '?' + urlencode({quote_plus('ServiceKey'): key})

    # API 를 통해 데이터 불러와 ElementTree 로 파싱
    response = requests.get(url + queryParams)
    tree = ET.ElementTree(ET.fromstring(response.text))
    retry_error_code = ['01', '02', '03', '04', '05']
    target_time = datetime.datetime.now(pytz.timezone('Asia/Seoul')).replace(minute=0, second=0, microsecond=0)
    target_time = datetime.datetime.combine(target_time.date(), target_time.time())

    # try collecting data from API for 5 times
    for j in range(0, 5):
        print('Trial {} : Getting forecast_ultra based on'.format(j+1), target_time,)
        api_error = False
        result_code = tree.find('.//resultCode').text

        # result_code(00) : If data is appropriately collected
        if result_code == '00':
            get_time = datetime.datetime.strptime(tree.find('.//baseDatetime').text, '%Y%m%d%H%M%S')

            # collect data if the base_time is appropriate
            if get_time == target_time:
                new_data = [get_time.date(), get_time.time(), float(tree.find('.//suppAbility').text),
                            float(tree.find('.//currPwrTot').text), float(tree.find('.//forecastLoad').text),
                            float(tree.find('.//suppReservePwr').text), float(tree.find('.//suppReserveRate').text),
                            float(tree.find('.//operReservePwr').text), float(tree.find('.//operReserveRate').text)]
                return new_data
            else:
                # 5th trial : return empty data
                if j == 4:
                    print('Trial {} : Failed. Inappropriate base time. Returning empty data'.format(j + 1), '\n')
                    return [target_time.date(), target_time.time(), None, None, None, None, None, None, None]

                # 1-4th trial : retry after 30 sec
                else:
                    print('Trial {} : Failed. Inappropriate base time. Automatically retry in 30 sec'.format(j + 1), '\n')
                    time.sleep(30)

        # error worth retry
        elif result_code in retry_error_code:
            # 5th trial : return empty data
            if j == 4:
                print('Trial {} : Failed. API Error Code {}. Returning empty data'.format(j + 1, result_code), '\n')
                return [target_time.date(), target_time.time(), None, None, None, None, None, None, None]

            # 1-4th trial : retry after 30 sec
            else:
                print('Trial {} : Failed. API Error Code {}. Automatically retry in 30 sec'.format(j + 1, result_code), '\n')
                time.sleep(30)

        # error not worth retry : return empty dataframe
        else:
            print('Trial {} : Critical API Error {}. Cancel calling API .\n'.format(j + 1, result_code))
            return [target_time.date(), target_time.time(), None, None, None, None, None, None, None]


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'hourly_powersupply'
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
                       "CHANGE COLUMN `cdate` `cdate` DATE NOT NULL, " \
                       "CHANGE COLUMN `ctime` `ctime` TIME NOT NULL, " \
                       "CHANGE COLUMN `supply_capacity` `supply_capacity` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `demand` `demand` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `peak_demand` `peak_demand` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `reserve` `reserve` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `reserve_margin` `reserve_margin` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `operational_reserve` `operational_reserve` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `operational_reserve_ratio` `operational_reserve_ratio` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate, ctime);".format(table_name)
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


# update MySQL
def updateMySQL():
    table_name = 'SMP.eric_hourly_powersupply'
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
        new_data = update()
        print('New data to be added :', new_data, '\n')

        # if the time of the data is not o'clock, cancel the update
        if new_data[1].minute != 0:
            print('Update cancelled : Update should occur at every hour\n')

        else:
            query_string = 'INSERT INTO {} (cdate, ctime, supply_capacity, demand, peak_demand, reserve, ' \
                           'reserve_margin, operational_reserve, operational_reserve_ratio) VALUES ' \
                           '(%s, %s, %s, %s, %s, %s, %s, %s, %s)' \
                           'ON DUPLICATE KEY UPDATE ' \
                           'supply_capacity = IF(supply_capacity IS NULL, %s, supply_capacity), ' \
                           'demand = IF(demand IS NULL, %s, demand), ' \
                           'peak_demand = IF(peak_demand IS NULL, %s, peak_demand), ' \
                           'reserve = IF(reserve IS NULL, %s, reserve), ' \
                           'reserve_margin = IF(reserve_margin IS NULL, %s, reserve_margin), ' \
                           'operational_reserve = IF(operational_reserve IS NULL, %s, operational_reserve), ' \
                           'operational_reserve_ratio = IF(operational_reserve_ratio IS NULL, %s, operational_reserve_ratio);'.format(table_name)
            cursor.execute(query_string, new_data + new_data[2:9])
            cnx.commit()

            # check for changes in the MySQL table
            if cursor.rowcount == 0:
                print('Data already exists in the MySQL table. No change was made.', new_data)
            elif cursor.rowcount == 1:
                print('New data inserted into MySQL table.', new_data)
            elif cursor.rowcount == 2:
                print('Null data is updated.', new_data)
            else:
                print('Unexpected row count.', new_data)

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info(), '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# delete rows in MySQL
def deleteMySQL():
    table_name = 'SMP.eric_hourly_powersupply'
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
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 56558".format(table_name)))
        cnx.commit()
        print('Deletion completed.\n')

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
    # 과거 데이터 통합 및 형식 변형
    # merge_csv('hourly_powersupply.csv')
    # fix_wrong_time('hourly_powersupply.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()


# Manual
# version : 2021-06-29
# 1. Download csv files for each 3 months
#    (https://openapi.kpx.or.kr/sukub.do)
# 2. Save each of them like '20150101-20150331'
# 3. Move the files to the directory same as main.py
# 4. Run 'merge' to create a merged csv file
# 5. Run 'fix_wrong_time' on the csv file created