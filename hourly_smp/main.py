## 필요한 모듈 불러오기
import requests
from urllib.parse import urlencode, quote_plus
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from functools import reduce
import datetime
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine


# 과거 SMP 데이터 파일(csv) 정리 (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
def organize_past_smp(csv_to_organize):
    # csv_to_organize : 정리하고자 하는 csv 파일명

    # 작업 현황 파악을 위한 출력
    print('Working on', csv_to_organize)

    # 과거 SMP 기록이 담긴 csv 파일을 불러와 데이터프레임으로 저장
    df_past = pd.read_csv(csv_to_organize)

    # df_past 의 column 설정
    # 1. 이름을 한국어에서 영어와 숫자로 변경
    # 2. 최대, 최소, 가중평균 column 삭제
    cols = ['date']
    for i in range(1, 25):
        cols.append(i)
    cols.extend(['max', 'min', 'wa'])
    df_past.columns = cols
    df_past = df_past.drop(['max', 'min', 'wa'], axis=1)

    # df_past 의 'date' column 값 str 에서 datetime.date type 으로 전환
    for i in range(0, len(df_past['date'])):
        df_past.at[i, 'date'] = datetime.datetime.strptime(df_past['date'][i], '%Y/%m/%d').date()

    # df_past 를 df_smp 로 명세서 형식에 맞게 수정
    smp_cols = ['cdate', 'ctime', 'smp']
    df_smp = pd.DataFrame(columns=smp_cols)

    # df_past 에서 날짜 하나씩 데이터 불러와 df_smp 에 저장
    # 저장해야하는 날짜
    correct_date = datetime.datetime(2015, 1, 1).date()
    # 하루의 데이터를 가져와, 시간별로 기입
    for i in range(0, len(df_past.index)):
        get_data = df_past.loc[len(df_past.index) - i - 1].tolist()  # 하루의 데이터를 저장하는 리스트

        # 다음 데이터의 날짜가 옳다면 데이터 그대로 저장
        if get_data[0] == correct_date:
            for j in range(0, 24):
                # datetime.time 의 시간값에 0-23시 값을 주어야해서 24시를 00시로 표기
                if j == 23:
                    to_add = [get_data[0], datetime.time(j - 23, 0, 0), get_data[j + 1]]
                else:
                    to_add = [get_data[0], datetime.time(j + 1, 0, 0), get_data[j + 1]]
                df_smp.loc[len(df_smp)] = to_add
            # df_smp 에 추가된 날짜 출력
            print(get_data[0])
            correct_date = correct_date + relativedelta(days=1)

        # 중복된 날짜가 있다면 저장하지 말 것
        elif get_data[0] == correct_date - relativedelta(days=1):
            print('Duplicate date:', get_data[0])

        # 누락된 날짜가 있다면 새로운 데이터의 날짜 전날까지 빈데이터 추가 후 새로운 데이터 추가
        else:
            # 새로운 날짜 전날까지 빈데이터 추가
            while get_data[0] == correct_date + relativedelta(days=1):
                for k in range(0, 24):
                    # datetime.time 의 시간값에 0-23시 값을 주어야해서 24시를 00시로 표기
                    if k == 23:
                        to_add = [correct_date, datetime.time(k - 23, 0, 0), np.NaN]
                    else:
                        to_add = [correct_date, datetime.time(k + 1, 0, 0), np.NaN]
                    df_smp.loc[len(df_smp)] = to_add
                print('Omitted date:', correct_date)
                correct_date = correct_date + relativedelta(days=1)
            # 새로운 데이터 추가
            for m in range(0, 24):
                # datetime.time 의 시간값에 0-23시 값을 주어야해서 24시를 00시로 표기
                if m == 23:
                    to_add = [get_data[0], datetime.time(m - 23, 0, 0), get_data[m + 1]]
                else:
                    to_add = [get_data[0], datetime.time(m + 1, 0, 0), get_data[m + 1]]
                df_smp.loc[len(df_smp)] = to_add
            # df_smp 에 추가된 날짜 출력
            print(get_data[0])
            correct_date = correct_date + relativedelta(days=1)

    # 완성된 df_smp 출력
    print(df_smp)

    # 다시 csv 파일에 저장 (육지)
    # df_smp.to_csv('hourly_land_smp_formatted.csv', index=False, header=True)
    # 다시 csv 파일에 저장 (제주)
    # df_smp.to_csv('hourly_jeju_smp_formatted.csv', index=False, header=True)
    df_smp.to_csv(csv_to_organize, index=False, header=True)


# 육지, 제주 데이터 합치기
def merge_land_jeju(csv_land, csv_jeju, csv_merged):
    # csv_land : 육지 가중평균 데이터 csv 파일
    # csv_jeju : 제주 가중평균 데이터 csv 파일
    # csv_merged : 합쳐진 데이터를 저장할 새로운 csv 파일

    # 작업 현황 파악을 위한 출력
    print('Merging', csv_land, '&', csv_jeju, 'to', csv_merged)

    # 육지, 제주 가중평균 데이터를 df_smp 데이터프레임에 합치기
    df_land = pd.read_csv(csv_land)
    df_jeju = pd.read_csv(csv_jeju)
    dfs = [df_land, df_jeju]
    df_smp = reduce(lambda left, right: pd.merge(left, right, on=['cdate', 'ctime']), dfs)

    # 합쳐진 데이터프레임 df_smp 의 'cdate' column 을 datetime.date type 으로 변환
    old_cdate_column = df_smp['cdate'].tolist()
    date_column = []
    for i in range(0, len(old_cdate_column)):
        clock = datetime.datetime.strptime(str(old_cdate_column[i]), '%Y-%m-%d').date()
        date_column.append(clock)
    df_smp['cdate'] = date_column

    # 합쳐진 데이터프레임 df_smp 의 'ctime' column 을 datetime.time type 으로 변환
    old_ctime_column = df_smp['ctime'].tolist()
    time_column = []
    for i in range(0, len(old_ctime_column)):
        clock = datetime.datetime.strptime(str(old_ctime_column[i]), '%H:%M:%S').time()
        time_column.append(clock)
    df_smp['ctime'] = time_column

    # index column 1부터 시작 및 이름 'id' 로 설정
    df_smp.index = np.arange(1, len(df_smp) + 1)
    df_smp.index.name = 'id'

    # smp column 이름 'land_smp', 'jeju_smp' 로 변경
    df_smp = df_smp.rename(columns={'smp_x': 'land_smp', 'smp_y': 'jeju_smp'})

    # 새로운 csv 파일에 저장
    df_smp.to_csv(csv_merged, header=True)
    print('Merging completed to file', csv_merged)


# 새로운 데이터 업데이트 후 return the dataframe
# 한국전력거래소_계통한계가격조회 (https://www.data.go.kr/iim/api/selectAPIAcountView.do)
def update():
    # api input parameter 설정
    url = 'https://openapi.kpx.or.kr/openapi/smp1hToday/getSmp1hToday'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

    df_update = pd.DataFrame(columns=['cdate', 'ctime', 'land_smp', 'jeju_smp'])

    for i in range(0, 2):
        if i == 0:
            areaCd = 1
        else:
            areaCd = 9

        queryParams = '?' + urlencode({quote_plus('ServiceKey'): key, quote_plus('areaCd'): areaCd})

        # API 를 통해 데이터 불러와 ElementTree 로 파싱
        response = requests.get(url + queryParams)
        tree = ET.ElementTree(ET.fromstring(response.text))

        # 새로운 데이터 수집
        new_data = []
        for item in tree.findall('.//smp'):
            new_data.append(item.text)

        if i == 0:
            df_update.loc[:, 'land_smp'] = new_data
        else:
            df_update.loc[:, 'jeju_smp'] = new_data

    # cdate, ctime column 설정
    ctime_column = []
    for j in range(0, 24):
        ctime_column.append(datetime.time(hour=(j + 1) % 24, minute=0, second=0))
    df_update.loc[:, 'ctime'] = ctime_column
    df_update.loc[:, 'cdate'] = datetime.datetime.strptime(tree.find('.//tradeDay').text, '%Y%m%d').date()

    return df_update


# csv file to MySQL
def toMySQL():
    data_name = 'hourly_smp'

    with open(r'C:\Users\boojw\OneDrive\Desktop\new_smp\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='{}_eric'.format(data_name), con=engine, if_exists='replace', index=False)

    print('{}.csv is added to MySQL'.format(data_name))


# update MySQL
def updateMySQL():
    table_name = 'SMP.hourly_smp_eric'

    with open(r'C:\Users\boojw\OneDrive\Desktop\new_smp\MySQL_info.txt', 'r') as text_file:
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

    # get the last row of the table
    cursor = cnx.cursor()
    cursor.execute(cursor.execute("SELECT * FROM {} ORDER BY id DESC LIMIT 1".format(table_name)))
    last_row = cursor.fetchall()
    last_id = last_row[0][0]
    print('Last row : ', last_row, '\n')

    # get new data by calling update function
    new_data = update()
    print('New data to be added :\n', new_data)

    # check if the new_data is appropriate
    if new_data.iloc[0].at['cdate'] != last_row[0][1] + relativedelta(days=1):
        print('Incorrect date for new data. Update cancelled.')

    else:
        # insert the new data to the table by taking each row
        for index, row in new_data.iterrows():
            # convert each row of the new dataframe into a list
            insert_data = [last_id + 1 + index] + row.values.tolist()
            print(insert_data)

            # insert into table
            try:
                query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s);'.format(table_name)
                cursor.execute(query_string, insert_data)
                cnx.commit()
                print('New data inserted into MySQL table.')

            except mysql.connector.Error as error:
                print('Failed to insert into MySQL table {}'.format(error))

    # close MySQL connection if it is connected
    if cnx.is_connected():
        cursor.close()
        cnx.close()


# delete rows in MySQL
def deleteMySQL():
    table_name = 'SMP.hourly_smp_eric'

    with open(r'C:\Users\boojw\OneDrive\Desktop\new_smp\MySQL_info.txt', 'r') as text_file:
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

    # get the last row of the table
    cursor = cnx.cursor()
    cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 56640".format(table_name)))
    cnx.commit()

    # close MySQL connection if it is connected
    if cnx.is_connected():
        cursor.close()
        cnx.close()


# main function
def main():
    # 과거 데이터 다운로드 : http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202
    # 과거 육지 smp 데이터 정리
    # organize_past_smp('hourly_land_smp.csv')
    # 과거 제주 smp 데이터 정리
    # organize_past_smp('hourly_jeju_smp.csv')

    # 과거 데이터 (육지 & 제주) 합치기
    # merge_land_jeju('hourly_land_smp.csv', 'hourly_jeju_smp.csv', 'hourly_smp.csv')

    # 새로운 데이터 업데이트
    # update('hourly_smp.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()