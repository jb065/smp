# 필요한 모듈 불러오기
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


# 시간별 기온 - 기상청_지상(종관, ASOS) 시간자료 조회서비스(https://www.data.go.kr/data/15057210/openapi.do)
url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

# 도시 리스트
cities = [['busan', 159], ['chungbuk', 131], ['chungnam', 133], ['daegu', 143], ['daejeon', 133], ['gangwon', 101],
          ['gwangju', 156], ['gyeongbuk', 143], ['gyeonggi', 119], ['gyeongnam', 155], ['incheon', 112],
          ['jeju', 184], ['jeonbuk', 146], ['jeonnam', 165], ['sejong', 239], ['seoul', 108], ['ulsan', 152]]


# 과거 데이터 수집
def get_past_data(end_month):
    # end_month : (str) 데이터를 수집하려는 마지막 달 (e.g. '202104')

    # for every city in the list
    for i in range(1, len(cities)):
        city_name = cities[i][0]
        city_code = cities[i][1]
        # 작업 현황 파악을 위한 출력
        print('Getting hourly_temp data for', city_name)

        startDt = datetime.datetime(2015, 1, 1)                            # api 호출 parameter 'startDt'
        endDt = startDt + relativedelta(months=1) - relativedelta(days=1)  # api 호출 parameter 'endDt'
        file_name = 'hourly_temp_' + city_name + '.csv'  # 저장할 csv 파일명
        df_past = pd.DataFrame()  # 수집한 데이터를 저장할 데이터프레임 생성

        # 세종시의 경우 20190531 데이터부터 수집
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
            df_temp.columns = 'time', city_name  # 다른 도시일 경우 이름 수정
            df_past = df_temp  # df_past : 최종 데이터프레임 / df_temp : 월별 임시 데이터프레임

            # 다음 while loop 에서 20190601 부터의 데이터 수집
            startDt = datetime.datetime(2019, 6, 1)  # api 호출 parameter 'startDt'
            endDt = startDt + relativedelta(months=1) - relativedelta(days=1)  # api 호출 parameter 'endDt'

        # 월별로 나누어 데이터를 수집 (end_month 까지의 데이터 수집)
        while startDt != datetime.datetime.strptime(end_month, '%Y%m') + relativedelta(months=1):
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
            df_temp.columns = 'time', city_name  # 다른 도시일 경우 이름 수정
            print(startDt, endDt)  # 작업 현황 표시

            # startDt, endDt 업데이트 (한달씩 추가)
            startDt = startDt + relativedelta(months=1)
            endDt = startDt + relativedelta(months=1) - relativedelta(days=1)

            # 종합 데이터프레임(df_past) 에 추가
            if startDt == datetime.datetime(year=2015, month=1, day=1):
                df_past = df_temp  # df_past : 최종 데이터프레임 / df_temp : 월별 임시 데이터프레임
            else:
                df_past = df_past.append(df_temp, ignore_index=True)

        # 'time' column 값을 datetime type 으로 변환
        tm_column = df_past['time'].tolist()
        time_column = []
        for i in range(0, len(tm_column)):
            time_column.append(datetime.datetime.strptime(tm_column[i], '%Y-%m-%d %H:%M'))
        df_past['time'] = time_column

        # csv 파일에 저장
        df_past.to_csv(file_name, index=False, header=True)

        # 작업 현황 파악을 위한 출력
        print(df_past)
        print('\nPast data for', city_name, 'is collected.\n')


# 세종시의 데이터에 2019년 이전 날짜 기입
def fix_sejong():
    df = pd.read_csv('hourly_temp_sejong.csv')

    print(df)
    last_time = datetime.datetime.strptime(df.loc[0].at['time'], '%Y-%m-%d %H:%M:%S')
    new_time = datetime.datetime(2015, 1, 1)
    df_top = pd.DataFrame(columns=['time', 'sejong'])

    while new_time < last_time:
        new_data = [new_time, np.NaN]
        df_top.loc[len(df_top)] = new_data
        new_time = new_time + datetime.timedelta(hours=1)
        print(new_time)

    df_top = df_top.append(df, ignore_index=True)
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
        for n in range(0, len(df_to_fix['time'])):
            df_to_fix.at[n, 'time'] = datetime.datetime.strptime(df_to_fix['time'][n], '%Y-%m-%d %H:%M:%S')

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
                for i in range(0, len(df_to_fix['time']) - 1):
                    # 데이터가 2개 이상인 시간의 인덱스를 'to_delete' 리스트에 저장
                    if df_to_fix['time'][i] == df_to_fix['time'][i + 1]:
                        to_delete.append(i + 1)

                # 작업 현황 파악을 위한 출력
                print('to_delete =', to_delete)

                # 해당 날짜 데이터 삭제
                df_to_fix = df_to_fix.drop(to_delete, axis=0)

                # 데이터프레임을 csv 에 넣었다가 다시 데이터프레임으로 빼오기
                df_to_fix.to_csv(csv_to_fix, index=False, header=True)
                df_to_fix = pd.read_csv(csv_to_fix)
                # 'time' column 값 str 에서 datetime type 으로 전환
                for k in range(0, len(df_to_fix['time'])):
                    df_to_fix.at[k, 'time'] = datetime.datetime.strptime(df_to_fix['time'][k], '%Y-%m-%d %H:%M:%S')

                # 데이터가 없는 시간 추가
                no_data = []  # 데이터가 없는 시간을 저장할 리스트
                # 데이터가 없는 시간의 인덱스를 no_data 리스트에 추가
                for m in range(0, len(df_to_fix['time']) - 1):
                    # 데이터가 없는 시간의 인덱스를 'no_data' 리스트에 저장
                    if df_to_fix['time'][m] != df_to_fix['time'][m + 1] - datetime.timedelta(hours=1):
                        no_data.append(m)

                # 작업 현황 파악을 위한 출력
                print('no_data =', no_data)

                # 데이터가 없는 날짜에 데이터 추가
                for j in range(0, len(no_data)):
                    # 추가할 새로운 데이터를 리스트 형식으로 저장 ['time', nan]
                    new_data = []
                    new_data.append(df_to_fix['time'][no_data[j] + j] + datetime.timedelta(hours=1))
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
        if df_to_filter['time'][i] != df_to_filter['time'][i + 1] - datetime.timedelta(hours=1):
            wrong_time.append(i + 1)

    # 잘못된 시간의 인덱스를 리스트로 반환
    return wrong_time


# merge dataframes of cities in the correct format and save it as a csv file
def merge():
    # 도시별 데이터프레임을 담는 리스트
    dfs = []

    # 도시 리스트에 있는 도시의 csv 파일을 하나씩 부르기
    for city in cities:
        # csv 파일을 데이터프레임으로 저장하고 리스트에 추가
        df_city = pd.read_csv('hourly_temp_{}.csv'.format(city[0]))
        dfs.append(df_city)

    # 도시별 데이터프레임을 하나의 데이터프레임에 합치기
    df_combined = reduce(lambda left, right : pd.merge(left, right, on='time'), dfs)
    print('df_combined : \n', df_combined)

    # list of column names of transposed dataframe
    time_list = df_combined['time'].tolist()
    time_list.insert(0, 'city')

    # transpose the dataframe
    df_combined.set_index('time', inplace=True)
    df_transposed = df_combined.transpose()
    df_transposed = df_transposed.reset_index()
    df_transposed = df_transposed.rename(columns={'index':'city'})
    # assign column names
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
        df_time = df_time.rename(columns={data_time:'temp'})

        # add the dataframe to the list
        dfs.append(df_time)

        # print current status
        print(data_time)

    # merge all the dataframes
    df_result = pd.concat(dfs)

    # add 'id' column
    df_result.insert(0, 'id', range(1, len(df_result.index) + 1))

    # print the current status
    print('df_result : \n', df_result)

    # save dataframe as a csv file
    df_result.to_csv('hourly_temp.csv', index=False, header=True)


# 새로운 데이터 업데이트
def update():
    # 새로운 데이터를 가져올 날짜 (호출하는 날의 전날)
    date_to_update = (datetime.datetime.now() - relativedelta(days=1)).date().strftime('%Y%m%d')

    # 도시별 데이터프레임을 담을 리스트
    dfs = []

    # api input parameter 설정
    url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

    for i in range(0, len(cities)):
        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key, quote_plus('pageNo'): '10',
                                       quote_plus('numOfRows'): '100', quote_plus('dataType'): 'JSON',
                                       quote_plus('dataCd'): 'ASOS', quote_plus('dateCd'): 'HR',
                                       quote_plus('startDt'): date_to_update, quote_plus('startHh'): '00',
                                       quote_plus('endDt'): date_to_update, quote_plus('endHh'): '23',
                                       quote_plus('stnIds'): cities[i][1]})

        response = requests.get(url + queryParams)
        json_response = response.json()
        df_temp = pd.DataFrame.from_dict(json_response['response']['body']['items']['item'])
        df_temp = df_temp[['tm', 'stnId', 'ta']]
        df_temp.columns = 'time', 'city_code', 'temp'
        df_temp.insert(1, 'city', cities[i][0])

        dfs.append(df_temp)

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

    # insert 'id' column
    df_new.reset_index(drop=True, inplace=True)

    return df_new


# csv file to MySQL
def toMySQL():
    data_name = 'hourly_temp'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False)

    print('{}.csv is added to MySQL'.format(data_name))

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
                       "CHANGE COLUMN `cdate` `cdate` DATE NOT NULL, " \
                       "CHANGE COLUMN `ctime` `ctime` TIME NOT NULL, " \
                       "CHANGE COLUMN `city` `city` VARCHAR(20) NOT NULL, " \
                       "CHANGE COLUMN `city_code` `city_code` INT NOT NULL, " \
                       "CHANGE COLUMN `temp` `temp` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate, ctime, city);".format(table_name)
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
    table_name = 'SMP.eric_hourly_temp'

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

    # get new data
    cursor = cnx.cursor()
    new_data = update()
    print('New data to be added :\n', new_data, '\n')

    # insert the new data to the table by taking each row
    try:
        for index, row in new_data.iterrows():
            try:
                # insert into table
                query_string = 'INSERT INTO {} (cdate, ctime, city, city_code, temp) ' \
                               'VALUES (%s, %s, %s, %s, %s);'.format(table_name)
                cursor.execute(query_string, row.values.tolist())
                cnx.commit()
                print('New data inserted into MySQL table.')

            except mysql.connector.Error as error:
                print('Failed to insert into MySQL table. {}\n'.format(error))

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
    table_name = 'SMP.eric_hourly_temp'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

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
        print("Unexpected error:", sys.exc_info(), '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# main function
def main():
    # get_past_data('202105')
    # fix_sejong()
    # fix_time()
    # merge()
    # update()

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()
