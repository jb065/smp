import requests
from urllib.parse import urlencode, quote_plus
import pandas as pd
import os
import datetime
import numpy as np
from dateutil.relativedelta import relativedelta
from functools import reduce
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys
import time
import pytz


# API url & key : 기상청_지상(종관, ASOS) 시간자료 조회서비스(https://www.data.go.kr/data/15057210/openapi.do)
url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

# list of cities
cities = [['busan', 159], ['chungbuk', 131], ['chungnam', 133], ['daegu', 143], ['daejeon', 133], ['gangwon', 101],
          ['gwangju', 156], ['gyeongbuk', 143], ['gyeonggi', 119], ['gyeongnam', 155], ['incheon', 112],
          ['jeju', 184], ['jeonbuk', 146], ['jeonnam', 165], ['sejong', 239], ['seoul', 108], ['ulsan', 152]]


# get the past data and format it into an appropriate csv file
def get_past_data(end_date):
    # end_date : (str) the last date for the data to be collected (e.g. '20210415')
    target_date = datetime.datetime.strptime(end_date, '%Y%m%d')

    # for every city in the list
    for city in cities:
        city_name = city[0]
        city_code = city[1]
        print('Getting hourly_temp data for', city_name)

        startDt = datetime.datetime(2015, 1, 1)
        endDt = startDt + relativedelta(months=1) - relativedelta(days=1)
        file_name = 'hourly_temp_' + city_name + '.csv'
        df_past = pd.DataFrame()

        # for 'sejong' city, collect data starting from 2019-05-31 11:00
        if city_name == 'sejong':
            queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                           quote_plus('pageNo'): '1',
                                           quote_plus('numOfRows'): '999',
                                           quote_plus('dataType'): 'JSON',
                                           quote_plus('dataCd'): 'ASOS',
                                           quote_plus('dateCd'): 'HR',
                                           quote_plus('startDt'): '20190531',
                                           quote_plus('startHh'): '11',
                                           quote_plus('endDt'): '20190531',
                                           quote_plus('endHh'): '23',
                                           quote_plus('stnIds'): city_code})

            response = requests.get(url + queryParams)
            json_response = response.json()
            df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
            df_temp = df_temp[['tm', 'ta']]
            df_temp.columns = 'ctime', city_name
            df_past = df_temp

            # collect data starting from 2019-06-01 in the next iteration of while loop
            startDt = datetime.datetime(2019, 6, 1)
            endDt = startDt + relativedelta(months=1) - relativedelta(days=1)

        # get past_data for each month
        while startDt < target_date:
            queryParams = '?' + urlencode({quote_plus('ServiceKey'): key,
                                           quote_plus('pageNo'): '1',
                                           quote_plus('numOfRows'): '999',
                                           quote_plus('dataType'): 'JSON',
                                           quote_plus('dataCd'): 'ASOS',
                                           quote_plus('dateCd'): 'HR',
                                           quote_plus('startDt'): startDt.strftime("%Y%m%d"),
                                           quote_plus('startHh'): '00',
                                           quote_plus('endDt'): endDt.strftime("%Y%m%d"),
                                           quote_plus('endHh'): '23',
                                           quote_plus('stnIds'): city_code})

            response = requests.get(url + queryParams)
            json_response = response.json()
            df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
            df_temp = df_temp[['tm', 'ta']]
            df_temp.columns = 'ctime', city_name
            print(startDt.date(), endDt.date())

            # increment startDt and endDt
            startDt = startDt + relativedelta(months=1)
            endDt = startDt + relativedelta(months=1) - relativedelta(days=1)
            if endDt > target_date:
                endDt = target_date
                print('here')

            # add df_temp (dataframe of each month) to df_past (dataframe of each city)
            if startDt == datetime.datetime(year=2015, month=1, day=1):
                df_past = df_temp
            else:
                df_past = df_past.append(df_temp, ignore_index=True)

        # convert 'cdate' column to datetime.date
        df_past['ctime'] = df_past['ctime'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M'))

        # save as a new csv file
        print('{} : hourly_temp data collected'.format(city_name))
        print('Collected Data :\n', df_past)
        df_past.to_csv(file_name, index=False, header=True)


# add empty data before 2019-05-31
def fix_sejong():
    df = pd.read_csv('hourly_temp_sejong.csv')
    print('Sejong : Adding empty data before 2019-05-29 11:00')

    last_time = datetime.datetime.strptime(df.loc[0].at['ctime'], '%Y-%m-%d %H:%M:%S')
    new_time = datetime.datetime(2015, 1, 1)
    df_top = pd.DataFrame(columns=['ctime', 'sejong'])

    while new_time < last_time:
        new_data = [new_time, np.NaN]
        df_top.loc[len(df_top)] = new_data
        new_time = new_time + datetime.timedelta(hours=1)
        print(new_time)

    df_top = df_top.append(df, ignore_index=True)
    print('Sejong : Empty data added before 2019-05-29 11:00')
    print(df_top)
    df_top.to_csv('hourly_temp_sejong.csv', index=False, header=True)


# csv 파일에서 잘못된 시간 수정 (delete duplicates, add omitted dates)
def fix_time():
    for p in range(0, len(cities)):
        # city_name : name of the city
        city_name = cities[p][0]
        # csv_to_fix : 수정할 csv 파일
        csv_to_fix = 'hourly_temp_{}.csv'.format(city_name)
        df_to_fix = pd.read_csv(csv_to_fix)  # df의 'time' column 값은 str 형식으로 전달될 것

        # 작업 현황 파악을 위한 출력
        print('Fixing dates for', csv_to_fix)

        # 'time' column 값 str 에서 datetime type 으로 전환
        for n in range(0, len(df_to_fix['ctime'])):
            df_to_fix.at[n, 'ctime'] = datetime.datetime.strptime(df_to_fix['ctime'][n], '%Y-%m-%d %H:%M:%S')

        # 잘못된 시간의 인덱스를 저장하는 리스트
        wrong_time = filter_wrong_time(df_to_fix)
        print('wrong_time =', wrong_time)

        # 잘못된 시간이 없을 경우
        if len(wrong_time) == 0:
            print('No time with wrong data.')
        # 잘못된 시간이 있을 경우
        else:
            # 잘못된 시간이 있을 경우, 시간 찾아 수정하기
            # wrong_time 에 값을 갖고 있는 동안 (잘못된 시간이 있는동안)
            while len(wrong_time) != 0:
                # 데이터가 2개 이상인 시간 삭제
                to_delete = []  # 데이터가 2개 이상인 시간를 저장할 리스트
                for i in range(0, len(df_to_fix['ctime']) - 1):
                    # 데이터가 2개 이상인 시간의 인덱스를 'to_delete' 리스트에 저장
                    if df_to_fix['ctime'][i] == df_to_fix['ctime'][i + 1]:
                        to_delete.append(i + 1)

                # 작업 현황 파악을 위한 출력
                print('to_delete =', to_delete)

                # 해당 날짜 데이터 삭제
                df_to_fix = df_to_fix.drop(to_delete, axis=0)

                # 데이터프레임을 csv 에 넣었다가 다시 데이터프레임으로 빼오기
                df_to_fix.to_csv(csv_to_fix, index=False, header=True)
                df_to_fix = pd.read_csv(csv_to_fix)
                # 'time' column 값 str 에서 datetime type 으로 전환
                for k in range(0, len(df_to_fix['ctime'])):
                    df_to_fix.at[k, 'ctime'] = datetime.datetime.strptime(df_to_fix['ctime'][k], '%Y-%m-%d %H:%M:%S')

                # 데이터가 없는 시간 추가
                no_data = []  # 데이터가 없는 시간을 저장할 리스트
                # 데이터가 없는 시간의 인덱스를 no_data 리스트에 추가
                for m in range(0, len(df_to_fix['ctime']) - 1):
                    # 데이터가 없는 시간의 인덱스를 'no_data' 리스트에 저장
                    if df_to_fix['ctime'][m] != df_to_fix['ctime'][m + 1] - datetime.timedelta(hours=1):
                        no_data.append(m)

                # 작업 현황 파악을 위한 출력
                print('no_data =', no_data)

                # 데이터가 없는 날짜에 데이터 추가
                for j in range(0, len(no_data)):
                    # 추가할 새로운 데이터를 리스트 형식으로 저장 ['time', nan]
                    new_data = []
                    new_data.append(df_to_fix['ctime'][no_data[j] + j] + datetime.timedelta(hours=1))
                    new_data.append(np.NaN)

                    # 새로운 데이터 row 삽입
                    temp = df_to_fix[df_to_fix.index > (no_data[j] + j)]
                    df_to_fix = df_to_fix[df_to_fix.index <= (no_data[j] + j)]
                    df_to_fix.loc[len(df_to_fix)] = new_data
                    df_to_fix = df_to_fix.append(temp, ignore_index=True)

                # 수정된 데이터프레임 'filter_wrong_time' 으로 다시 확인
                wrong_time = filter_wrong_time(df_to_fix)

            # 수정된 데이터프레임 csv 파일에 다시 입력
            df_to_fix.to_csv(csv_to_fix, index=False, header=True)
            print("Times fixed for", csv_to_fix)


# 데이터프레임에서 잘못된 시간의 인덱스를 리스트 형식으로 반환
def filter_wrong_time(df):
    df_to_filter = df  # 확인하고 싶은 데이터프레임
    wrong_time = []  # 누락된 시간의 인덱스을 저장할 리스트

    # 데이터프레임 서칭 후, 적절하지 않은 시간대 확인하여 wrong_time 리스트에 저장
    for i in range(0, len(df_to_filter.index) - 1):
        if df_to_filter['ctime'][i] != df_to_filter['ctime'][i + 1] - datetime.timedelta(hours=1):
            wrong_time.append(i + 1)

    # 잘못된 시간의 인덱스를 리스트로 반환
    return wrong_time


# merge dataframes of cities in the correct format and save it as a csv file
def merge():
    # list of dataframes
    dfs = []
    print('Merging csv files of each city')

    # for every city in the list 'cities', get the dataframes of csv files and add them to the list 'dfs'
    for city in cities:
        df_city = pd.read_csv('hourly_temp_{}.csv'.format(city[0]))
        dfs.append(df_city)

    # merge dataframes of all cities
    df_merged = reduce(lambda left, right: pd.merge(left, right, on='ctime'), dfs)
    print('df_merged : \n', df_merged)

    # list of column names of transposed dataframe
    time_list = df_merged['ctime'].tolist()
    time_list.insert(0, 'city')

    # transpose the dataframe and assign new column names
    df_merged.set_index('ctime', inplace=True)
    df_transposed = df_merged.transpose()
    df_transposed = df_transposed.reset_index()
    df_transposed = df_transposed.rename(columns={'index': 'city'})
    df_transposed.columns = time_list
    print('df_transposed : \n', df_transposed)

    # insert city_code column
    code_list = []
    for i in range(0, len(cities)):
        code_list.append(cities[i][1])
    df_transposed.insert(1, 'city_code', code_list)

    # extract dataframe for each hour (55490 columns total)
    # list of columns
    col_list = df_transposed.columns.tolist()
    # list that stores dataframes of each hour
    dfs = []

    # for data of every hour
    for i in range(2, len(col_list)):
        # stores the 'time' value of data
        data_time = col_list[i]
        # create a new dataframe of selected columns
        df_time = df_transposed[['city', 'city_code', data_time]]
        # insert 'cdate' & 'ctime' columns with corresponding values
        df_time.insert(0, 'ctime', datetime.datetime.strptime(data_time, '%Y-%m-%d %H:%M:%S').time())
        df_time.insert(0, 'cdate', datetime.datetime.strptime(data_time, '%Y-%m-%d %H:%M:%S').date())
        # change the column name 'YYYY-mm-dd HH:MM:SS' to 'temp'
        df_time = df_time.rename(columns={data_time: 'temp'})

        # add the dataframe to the list
        dfs.append(df_time)
        print(data_time)

    # merge all the dataframes and add 'id' column
    df_result = pd.concat(dfs)
    df_result.insert(0, 'id', np.arange(1, len(df_result.index) + 1))

    # save as a new csv file
    print('Merging csv files of each city completed. Saved as hourly_temp.csv')
    print('df_result : \n', df_result)
    df_result.to_csv('hourly_temp.csv', index=False, header=True)


# returns template of dataframe based on base_date
def get_template(base_date):
    df = pd.DataFrame(index=np.arange(len(cities) * 24), columns=['cdate', 'ctime', 'city', 'city_code', 'temp'])

    ctime_column = []
    for i in range(0, 24):
        for j in range(0, len(cities)):
            ctime_column.append(datetime.time(i, 0, 0))

    city_name = []
    city_code = []
    for city in cities:
        city_name.append(city[0])
        city_code.append(city[1])

    df['cdate'] = base_date
    df['ctime'] = ctime_column
    df['city'] = city_name * 24
    df['city_code'] = city_code * 24
    df['temp'] = None

    return df


# 새로운 데이터 업데이트
def update():
    # 새로운 데이터를 가져올 날짜 (호출하는 날의 전날)
    target_date = (datetime.datetime.now(pytz.timezone('Asia/Seoul')) - relativedelta(days=1)).date()
    param_date = target_date.strftime('%Y%m%d')

    df_template = get_template(target_date)
    retry_error_code = ['01', '02', '03', '04', '05']

    # api input parameter 설정
    url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

    # try collecting data from API for 5 times
    for j in range(0, 5):
        print('Trial {} : Getting hourly_temp based on'.format(j+1), target_date, '\n')
        api_error = False
        dfs = []

        # for every city in the list
        for i in range(0, len(cities)):
            queryParams = '?' + urlencode({quote_plus('ServiceKey'): key, quote_plus('pageNo'): '10',
                                           quote_plus('numOfRows'): '100', quote_plus('dataType'): 'JSON',
                                           quote_plus('dataCd'): 'ASOS', quote_plus('dateCd'): 'HR',
                                           quote_plus('startDt'): param_date, quote_plus('startHh'): '00',
                                           quote_plus('endDt'): param_date, quote_plus('endHh'): '23',
                                           quote_plus('stnIds'): cities[i][1]})

            response = requests.get(url + queryParams)
            json_response = response.json()
            result_code = json_response['response']['header']['resultCode']

            # result_code(00) : If data is appropriately collected
            if result_code == '00':
                # check if the date of the new data is appropriate
                base_date = datetime.datetime.strptime(json_response['response']['body']['items']['item'][0]['tm'], '%Y-%m-%d %H:%M').date()

                if base_date != target_date:
                    print('Wrong month for new data. Returning empty data.')
                    return df_template
                else:
                    df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
                    df_temp = df_temp[['tm', 'stnId', 'ta']]
                    df_temp.columns = 'time', 'city_code', 'temp'
                    df_temp.insert(1, 'city', cities[i][0])
                    dfs.append(df_temp)
                    print(cities[i][0], ': hourly_temp collected')

            # error worth retry : retry after 2 min
            elif result_code in retry_error_code:
                print(cities[i][0], ': API Error Code {}\n'.format(result_code))
                api_error = True
                break

            # error not worth retry : return empty dataframe
            else:
                print(cities[i][0], ': Critical API Error. Returning empty data.\n')
                return df_template

        # if there is an error in API, retry after 2 minutes
        if api_error:
            print('Trial {} : Failed. Error during calling API. Automatically retry in 2 min'.format(j + 1), '\n')
            time.sleep(120)
        else:
            break

    # 모든 도시의 데이터를 갖는 데이터프레임
    df_new_merged = pd.concat(dfs, axis=1, join='inner')

    # list that stores sliced dataframes
    dfs_time = []

    # slice the dataframe by hour and city, then store it in dfs_time list
    for j in range (0, len(df_new_merged.index)):
        df_slice = df_new_merged.iloc[j:j+1]
        for k in range (0, len(cities)):
            dfs_time.append(df_slice.iloc[:, range(4 * k, 4 * k + 4)])
    # merge all the sliced dataframes into df_new dataframe
    df_new = pd.concat(dfs_time)

    # df_result 데이터프레임의 날짜와 시간을 분리
    # 'time' column(인덱스) 을 'date' 와 'time' column 으로 나누고, 해당 값을 datetime.date/time type 으로 변환
    tm_column = df_new['time'].tolist()
    date_column = []
    time_column = []
    for i in range(0, len(tm_column)):
        date_column.append(datetime.datetime.strptime(tm_column[i], '%Y-%m-%d %H:%M').date())
        time_column.append(datetime.datetime.strptime(tm_column[i], '%Y-%m-%d %H:%M').time())
    # delete the existing 'time' column, and insert 'ctime' and 'cdate' columns instead
    df_new = df_new.drop('time', axis=1)
    df_new.insert(0, 'ctime', time_column)
    df_new.insert(0, 'cdate', date_column)
    df_new.reset_index(drop=True, inplace=True)

    # combine the template dataframe and the collected data, convert nan to None, then return it
    df_result = df_template.combine_first(df_new)
    df_result = df_result.where(pd.notnull(df_result), None)
    return df_result


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'hourly_temp'
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

    print('{}.csv is added to MySQL'.format(data_name))

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

    # set datatype and features / set an unique key
    try:
        cursor = cnx.cursor()

        # set datatype and features
        query_string = "ALTER TABLE {} " \
                       "CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT, " \
                       "CHANGE COLUMN `cdate` `cdate` DATE NOT NULL, " \
                       "CHANGE COLUMN `ctime` `ctime` TIME NOT NULL, " \
                       "CHANGE COLUMN `city` `city` VARCHAR(20) NOT NULL, " \
                       "CHANGE COLUMN `city_code` `city_code` INT NOT NULL, " \
                       "CHANGE COLUMN `temp` `temp` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate, ctime, city);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Unique Key(uidx) is set')

    except mysql.connector.Error as error:
        print('Failed set datatype and features of MySQL table {}'.format(error))

    except:
        print("Unexpected error:", sys.exc_info())

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed')


# update MySQL
def updateMySQL():
    table_name = 'SMP.eric_hourly_temp'
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

    # insert the new data to the table by taking each row
    try:
        # get new data
        cursor = cnx.cursor()
        new_data = update()
        print('New data to be added :\n', new_data, '\n')
        
        for index, row in new_data.iterrows():
            try:
                # insert into table
                row_data = row.values.tolist()
                query_string = 'INSERT INTO {} (cdate, ctime, city, city_code, temp) ' \
                               'VALUES (%s, %s, %s, %s, %s) ' \
                               'ON DUPLICATE KEY UPDATE ' \
                               'temp = IF(temp IS NULL, %s, temp);'.format(table_name)
                cursor.execute(query_string, row_data + row_data[4:5])
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
                print("Unexpected error:", sys.exc_info())

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table. {}'.format(error))

    except:
        print("Unexpected error:", sys.exc_info())

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed')


# delete rows in MySQL
def deleteMySQL():
    table_name = 'SMP.eric_hourly_temp'
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
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 955944".format(table_name)))
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
        print("Unexpected error:", sys.exc_info())

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed')


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
    # Organize past data
    # get_past_data('20210630')
    # fix_sejong()
    # fix_time()
    # merge()

    # MySQL
    toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()
