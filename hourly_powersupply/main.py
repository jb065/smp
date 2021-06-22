## 필요한 모듈 불러오기
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


# 과거 데이터 종합 (사이트에서 받은 파일 그대로 사용)
# CSV 파일 다운로드 : https://openapi.kpx.or.kr/sukub.do (3개월치 씩 나누어 다운로드)
def combine_past_data(csv_to_save):
    # csv_to_save : 새로 저장할 csv 파일명
    merged = []  # 데이터프레임 저장하는 리스트

    # 모든 csv 파일에 대하여
    files = [f for f in os.listdir('.') if os.path.isfile(f)]
    for f in files:
        filename, ext = os.path.splitext(f)
        if ext == '.csv':
            read = pd.read_csv(f)
            # column 이름 영어로 설정
            read.columns = ['time', 'supply_capacity', 'demand', 'peak_demand', 'reserve', 'reserve_margin',
                            'operational_reserve', 'operational_reserve_ratio']
            # 정각 데이터 추출
            temp = read  # 불러온 데이터프레임의 index 개수를 파악하기 위한 복사
            for i in range(0, len(temp.index)):
                if read.loc[i].at['time'] % 10000 != 0:
                    read = read.drop(i)

            # 날짜와 시간값을 date, time 에 각각 datetime.date, datetime.time type 으로 저장
            get_column = read['time'].tolist()
            date_column = []
            time_column = []
            for i in range(0, len(get_column)):
                clock = datetime.datetime.strptime(str(get_column[i]), '%Y%m%d%H%M%S')
                date_column.append(clock.date())
                time_column.append(clock.time())
            read['time'] = time_column
            read.insert(0, 'date', date_column)

            # 작업 현황 파악을 위한 출력
            print(filename)

            # 수정된 데이터프레임을 리스트에 추가
            merged.append(read)

    # 데이터프레임이 수집된 리스트로 종합 데이터프레임 생성 후 csv 파일 제작
    # 생성 후, date, time column 이름 변경
    df_result = pd.concat(merged).rename(columns={'date':'cdate', 'time':'ctime'})

    # index reset 후, index column 이름 'id' 로 설정
    df_result = df_result.reset_index(drop=True)
    df_result.index = np.arange(1, len(df_result) + 1)
    df_result.index.name = 'id'

    # csv 파일에 종합된 데이터프레임 저장
    df_result.to_csv(csv_to_save, index=True, header=True)

    # 작업 현황 파악을 위한 출력
    print('Files are combined')


# 데이터프레임에서 누락된 시간의 인덱스를 리스트 형식으로 반환
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
def fix_wrong_time(csv_to_read, csv_to_save):
    # csv_to_read : 확인하고 싶은 csv 파일명
    # csv_to_read : 새로 저장하고 싶은 csv 파일명
    df_past = pd.read_csv(csv_to_read)  # 확인하고 싶은 csv 을 데이터프레임으로 저장

    # 작업 현황 출력
    print('Fixing time values in file', csv_to_read)

    # 'cdate' 와 'ctime' column 값을 datetime.date, datetime.time type 으로 변환 후 다시 데이터프레임에 적용
    date_column1 = df_past['cdate'].tolist()
    date_column2 = []
    for d in date_column1:
        d = datetime.datetime.strptime(d, '%Y-%m-%d').date()
        date_column2.append(d)
    df_past['cdate'] = date_column2

    time_column1 = df_past['ctime'].tolist()
    time_column2 = []
    for t in time_column1:
        t = datetime.datetime.strptime(t, '%H:%M:%S').time()
        time_column2.append(t)
    df_past['ctime'] = time_column2

    # 잘못된 시간대를 저장하는 리스트
    wrong_time = filter_wrong_time(df_past)

    # 누락된 시간이 없을 경우, function 종료
    if len(wrong_time) == 0:
        return

    # 잘못된 시간이 있을 경우, 시간대 찾아 수정하기
    # wrong_time 에 값을 갖고 있는 동안 (누락된 시간이 있는동안)
    while len(wrong_time) != 0:
        # wrong_time 리스트에 저장되어 있는 시간대 다음 시간을 데이터프레임에 추가
        for i in range(0, len(wrong_time)):
            date1 = df_past.loc[wrong_time[i] + i].at['cdate']
            time1 = df_past.loc[wrong_time[i] + i].at['ctime']
            datetime1 = datetime.datetime.combine(date1, time1)
            datetime2 = datetime1 + datetime.timedelta(hours=1, minutes=0, seconds=0)  # 누락된 시간
            # 누락된 시간의 데이터를 저장하는 new_data 리스트에 date 와 time 추가
            new_data = [0, datetime2.date(), datetime2.time()]
            # 나머지 값은 'nan' 값 부여
            for j in range(0, len(df_past.columns) - 3):
                new_data.append(np.NaN)
            # new_data 를 누락된 시간의 row 에 추가
            temp = df_past[df_past.index > (wrong_time[i] + i)]
            df_past = df_past[df_past.index <= (wrong_time[i] + i)]
            df_past.loc[len(df_past)] = new_data
            df_past = df_past.append(temp, ignore_index=True)

        wrong_time = filter_wrong_time(df_past)  # 수정 후, 누락된 시간이 있는지 다시 한번 확인 (연속으로 2개가 누락된 경우를 방지)

    # 날짜가 수정되면 id column 없애고 index 를 id column 으로 설정
    df_past = df_past.drop('id', axis=1)
    df_past.index = np.arange(1, len(df_past) + 1)
    df_past.index.name = 'id'

    df_past.to_csv(csv_to_save, index=True, header=True)

    # 작업 현황 파악을 위한 출력
    print('A new dataframe with appropriate time values is saved in a csv file,', csv_to_save)


# 새로운 데이터 업데이트
def update():
    url = 'https://openapi.kpx.or.kr/openapi/sukub5mMaxDatetime/getSukub5mMaxDatetime'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

    # land_smp (areaCd = 1)
    queryParams = '?' + urlencode({quote_plus('ServiceKey'): key})

    # API 를 통해 데이터 불러와 ElementTree 로 파싱
    response = requests.get(url + queryParams)
    tree = ET.ElementTree(ET.fromstring(response.text))

    """
    # 새로운 데이터의 시간값이 맞는지 확인
    get_time = datetime.datetime.strptime(tree.find('.//baseDatetime').text, '%Y%m%d%H%M%S')
    compare_date = datetime.datetime.strptime(df_result.loc[len(df_result) - 1].at['cdate'], '%Y-%m-%d').date()
    compare_time = datetime.datetime.strptime(df_result.loc[len(df_result) - 1].at['ctime'], '%H:%M:%S').time()
    to_compare = datetime.datetime.combine(compare_date, compare_time)
    # 기존 데이터 다음 시간대가 아닌 경우, 업데이트 취소
    # if get_time != to_compare + relativedelta(hours=1):
    #     print('Update cancelled : Not correct time for new data')
    #     return
    # 정각이 아닐 경우, 업데이트 취소
    # elif get_time.minute != 0:
    #     print('Update cancelled : Update should occur at every hour')
    #     return
    """

    # 새로운 데이터의 시간값이 맞는 경우, 데이터 정상 수집
    get_time = datetime.datetime.strptime(tree.find('.//baseDatetime').text, '%Y%m%d%H%M%S')
    new_data = [get_time.date(), get_time.time(), float(tree.find('.//suppAbility').text),
                float(tree.find('.//currPwrTot').text), float(tree.find('.//forecastLoad').text),
                float(tree.find('.//suppReservePwr').text), float(tree.find('.//suppReserveRate').text),
                float(tree.find('.//operReservePwr').text), float(tree.find('.//operReserveRate').text)]

    return new_data


# csv file to MySQL
def toMySQL():
    data_name = 'hourly_powersupply'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False)

    print('{}.csv is added to MySQL'.format(data_name))


# update MySQL
def updateMySQL():
    table_name = 'SMP.eric_hourly_powersupply'

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
        # get the last row of the table
        cursor = cnx.cursor()
        cursor.execute(cursor.execute("SELECT * FROM {} ORDER BY id DESC LIMIT 1".format(table_name)))
        last_row = cursor.fetchall()
        last_id = last_row[0][0]

        print('Last row : ', last_row, '\n')

        # get new data by calling update function
        new_data = update()
        print('New data to be added :\n', new_data, '\n')

        # check if the new_data is appropriate
        latest_time = datetime.datetime.combine(last_row[0][1], (datetime.datetime.min + last_row[0][2]).time())
        new_time = datetime.datetime.combine(new_data[0], new_data[1])

        if new_time != latest_time + relativedelta(hours=1):
            print('Update cancelled : Incorrect date for new data')
        elif new_time.minute != 0:
            print('Update cancelled : Update should occur at every hour')
        else:
            # insert the new data to the table
            insert_data = [last_id + 1] + new_data
            print(insert_data)

            # insert into table
            query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, insert_data)
            cnx.commit()
            print('New data inserted into MySQL table.')

    except mysql.connector.Error as error:
        print('Failed to insert into MySQL table {}\n'.format(error))

    except:
        print("Unexpected error:", sys.exc_info()[0], '\n')

    finally:
        if cnx.is_connected():
            cursor.close()
            cnx.close()
            print('MySQL connection is closed\n')


# delete rows in MySQL
def deleteMySQL():
    table_name = 'SMP.eric_hourly_powersupply'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

        # delete the target
        cursor = cnx.cursor()
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id = 56559".format(table_name)))
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


# main function
def main():
    # 과거 데이터 통합 및 형식 변형
    # combine_past_data('hourly_powersupply.csv')
    # fix_wrong_time('hourly_powersupply.csv', 'hourly_powersupply_fixed.csv')

    # 새로운 데이터 업데이트
    # update('hourly_powersupply.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()