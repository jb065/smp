import pandas as pd
from functools import reduce
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import datetime
import numpy as np
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys
import pytz


# format a csv file of past data (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
def format_csv(csv_name, location):
    # csv_name : name of the csv file
    # location : 육지(land) OR 제주(jeju)

    # get dataframe from the csv file
    df_past = pd.read_csv(csv_name)
    print('Formatting', csv_name)

    # change column names from Korean to English
    df_past.columns = ['cdate'] + list(range(1, 25)) + ['max', 'min', location + '_wa']

    # remove columns except 'cdate' and 'location_wa'
    df_past = df_past.drop(list(range(1, 25)) + ['max', 'min'], axis=1)

    # convert 'cdate' column to datetime.date type, and reverse the dataframe
    df_past['cdate'] = df_past['cdate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y/%m/%d').date())
    df_past = df_past.reindex(index=df_past.index[::-1])

    # move data from df_past to df_wa while checking the dates
    df_wa = pd.DataFrame(columns=['cdate', location + '_wa'])
    correct_date = datetime.datetime(2015, 1, 1).date()

    # iterate the row of df_past, transfer each row to df_wa
    for index, row in df_past.iterrows():
        row_data = row.values.tolist()

        # if the date of the row is correct, move data of the row to df_wa
        if row_data[0] == correct_date:
            df_wa.loc[len(df_wa)] = row_data
            correct_date = correct_date + relativedelta(days=1)
            print(row_data[0])

        # if the date of the row has duplicate, don't move the data to df_wa
        elif row_data[0] == correct_date - relativedelta(days=1):
            print('----------Duplicate date:', row_data[0], '----------')

        # if the date of a row is omitted, add a new row with the date and an empty value until the appropriate date
        else:
            while row_data[0] == correct_date + relativedelta(days=1):
                df_wa.loc[len(df_wa)] = [correct_date, None]
                correct_date = correct_date + relativedelta(days=1)
                print('----------Omitted date: {}----------'.format(correct_date))
            # then add the data of the row to df_wa
            df_wa.loc[len(df_wa)] = row_data
            correct_date = correct_date + relativedelta(days=1)
            print(row_data[0])

    print('Formatting', csv_name, 'completed.')
    print('Formatted Dataframe:\n', df_wa)
    df_wa.to_csv(csv_name, index=False, header=True)


# merge csv files
def merge_csv(csv_land, csv_jeju, csv_merged):
    # csv_land : csv file of land_smp_weighted_average
    # csv_jeju : csv file of jeju_smp_weighted_average
    # csv_merged : csv file of both land and jeju smp weighted average

    print('Merging', csv_land, '&', csv_jeju, 'to', csv_merged)

    # df_wa : merged dataframe of land and jeju smp weighted average
    dfs = [pd.read_csv(csv_land), pd.read_csv(csv_jeju)]
    df_wa = reduce(lambda left, right: pd.merge(left, right, on='cdate'), dfs)

    # convert 'cdate' column from str to datetime.date, then add 'id' column
    df_wa['cdate'] = df_wa['cdate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    df_wa.index = np.arange(1, len(df_wa) + 1)
    df_wa.index.name = 'id'

    # save as a new csv file
    df_wa.to_csv(csv_merged, index=True, header=True)
    print('Merging completed to file', csv_merged)


# 새로운 데이터 수집 후 list 형식으로 return
def update():
    # print current status
    print('Getting new data for daily_smp_weighted_average')

    # 크롬 창 뜨지 않게 설정 추가
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # chrome driver 을 통한 크롤링
    driver = webdriver.Chrome(r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)
    
    # 육지 가중평균값 조회
    url = 'https://www.kpx.or.kr/www/contents.do?key=225'
    driver.get(url)

    # 새로운 데이터의 날짜 확인
    land_cdate = driver.find_element_by_xpath("//*[@id=\"day_0\"]").text[:5]
    land_cdate = datetime.datetime.strptime(land_cdate, '%m.%d').date()
    today_date = datetime.datetime.now(pytz.timezone('Asia/Seoul')).date()

    # 새로운 날짜의 데이터를 저장할 리스트 [cdate, land_wa, jeju_wa]
    new_data = [today_date]

    # 수집하려는 데이터가 오늘 데이터가 맞으면, 데이터 정상 수집
    if land_cdate.month == today_date.month and land_cdate.day == today_date.day:
        land_wa = driver.find_element_by_css_selector('#smpLandDateSearchForm > fieldset > div.board_list_03 > table > tbody > tr:nth-child(27) > td:nth-child(8)').text
        new_data.append(float(land_wa))

    # 수집하려는 데이터가 오늘 데이터가 아니면, None 값 부여
    else:
        land_wa = None
        new_data.append(land_wa)

    # 제주 가중평균값 조회
    url = 'https://www.kpx.or.kr/www/contents.do?key=226'
    driver.get(url)

    # 새로운 데이터의 날짜 확인
    jeju_cdate = driver.find_element_by_xpath("//*[@id=\"dayJeju_0\"]").text[:5]
    jeju_cdate = datetime.datetime.strptime(jeju_cdate, '%m.%d').date()

    # 수집하려는 데이터가 오늘 데이터가 맞으면, 데이터 정상 수집
    if jeju_cdate.month == today_date.month and jeju_cdate.day == today_date.day:
        jeju_wa = driver.find_element_by_css_selector('#smpJejuDateSearchForm > fieldset > div.board_list_03 > table > tbody > tr:nth-child(27) > td:nth-child(8)').text
        new_data.append(float(jeju_wa))

    # 수집하려는 데이터가 오늘 데이터가 아니면, None 값 부여
    else:
        jeju_wa = None
        new_data.append(jeju_wa)

    # 크롬 드라이버 종료
    driver.close()

    return new_data


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'daily_smp_weighted_average'
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
                       "CHANGE COLUMN `land_wa` `land_wa` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `jeju_wa` `jeju_wa` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate);".format(table_name)
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


# update to MySQL
def updateMySQL():
    table_name = 'SMP.eric_daily_smp_weighted_average'
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
        print('new_data :', new_data)

        # insert the new data to the table
        query_string = 'INSERT INTO {} (cdate, land_wa, jeju_wa) VALUES (%s, %s, %s) ' \
                       'ON DUPLICATE KEY UPDATE ' \
                       'land_wa = IF(land_wa IS NULL, %s, land_wa), ' \
                       'jeju_wa = IF(jeju_wa IS NULL, %s, jeju_wa);'.format(table_name)
        cursor.execute(query_string, new_data + new_data[1:3])
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
    table_name = 'SMP.eric_daily_smp_weighted_average'
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
        cursor.execute("DELETE FROM {} WHERE id > 2371 ".format(table_name))
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
    # format the downloaded csv files of past data
    # format_csv('daily_land_smp_weighted_average.csv', 'land')
    # format_csv('daily_jeju_smp_weighted_average.csv', 'jeju')
    # merge_csv('daily_land_smp_weighted_average.csv', 'daily_jeju_smp_weighted_average.csv', 'daily_smp_weighted_average.csv')

    # MySQL
    # toMySQL()
    updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()
