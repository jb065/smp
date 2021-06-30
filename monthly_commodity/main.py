import datetime
import numpy as np
import pandas as pd
from selenium.webdriver.chrome.options import Options
import datetime
import numpy as np
from selenium import webdriver
import time
import os
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys
import pytz


# get the past data and format it into an appropriate csv file
def get_past_data():
    print('Getting past data of monthly_commodity')

    # set chrome driver
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(
        r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)

    # enables to download in headless chrome setting
    params = {'behavior': 'allow', 'downloadPath': os.getcwd()}
    driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

    # enter the link and download the xlsx file
    url = 'https://www.worldbank.org/en/research/commodity-markets'
    driver.get(url)
    target = driver.find_element_by_xpath(
        '//*[@id="1"]/div/div/div[1]/div/div/div/div/div[1]/div[1]/div/div/table/tbody/tr[3]/td[1]/a')
    target.click()
    print('Downloading the file...')

    # wait until the file is downloaded
    while not os.path.exists('CMO-Historical-Data-Monthly.xlsx'):
        time.sleep(1)
    print('Download completed')
    driver.close()

    # df : downloaded xlsx file to dataframe with skipped rows
    df = pd.read_excel('CMO-Historical-Data-Monthly.xlsx', sheet_name='Monthly Prices', skiprows=[0, 1, 2, 3])

    # set column names
    cols = df.iloc[1].tolist()
    cols.pop(0)
    cols.insert(0, 'cdate')
    df.columns = cols

    # drop unnecessary rows and columns
    df = df.drop([0, 1], axis=0)
    df = df[['cdate', 'COAL_AUS', 'COAL_SAFRICA', 'CRUDE_PETRO', 'CRUDE_BRENT', 'CRUDE_DUBAI', 'CRUDE_WTI', 'iNATGAS', 'NGAS_EUR', 'NGAS_US', 'NGAS_JP']]

    # set column names to lower cases and reset the index
    df = df.reset_index(drop=True)
    df.columns = [x.lower() for x in df.columns]
    df = df.rename(columns={'inatgas': 'ngas_index'})

    # convert 'cdate' column to datetime.date type
    df['cdate'] = df['cdate'].apply(lambda x: datetime.datetime.strptime(x, '%YM%m').date())

    # delete data before year 2015
    for index, row in df.iterrows():
        row_data = row.values.tolist()
        if row_data[0] < datetime.datetime(2015, 1, 1).date():
            df = df.drop(index, axis=0)

    # create 'id' column
    df = df.reset_index(drop=True)
    df.insert(0, 'id', np.arange(1, len(df) + 1))

    print('monthly_commodity past data save in monthly_commodity.csv')
    df.to_csv('monthly_commodity.csv', index=False, header=True)

    # delete the downloaded xlsx file
    os.remove('CMO-Historical-Data-Monthly.xlsx')


def update():
    # target month of data
    target_month = (datetime.datetime.now(pytz.timezone('Asia/Seoul')).replace(day=1, hour=0, minute=0, microsecond=0) - relativedelta(months=1)).date()
    print('Collecting data for', target_month)

    # 크롬 창 뜨지 않게 설정 추가
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # webdriver 설정
    driver = webdriver.Chrome(
        r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)

    # headless 상태에서 download 가 가능하도록 설정
    params = {'behavior': 'allow', 'downloadPath': os.getcwd()}
    driver.execute_cdp_cmd('Page.setDownloadBehavior', params)

    # 원하는 링크 접속
    url = 'https://www.worldbank.org/en/research/commodity-markets'
    driver.get(url)

    # 업데이트된 xlsx 파일 다운로드
    target = driver.find_element_by_xpath('//*[@id="1"]/div/div/div[1]/div/div/div/div/div[1]/div[1]/div/div/table/tbody/tr[3]/td[1]/a')
    target.click()
    print('Downloading the file...')

    # 파일이 다운로드 될때까지 기다리기
    while not os.path.exists('CMO-Historical-Data-Monthly.xlsx'):
        time.sleep(1)
    print('Download completed')

    # 다운로드 한 xlsx 파일을 데이터프레임으로 읽기
    new_df = pd.read_excel('CMO-Historical-Data-Monthly.xlsx', sheet_name='Monthly Prices', skiprows=[0, 1, 2, 3])

    # column 으로 설정하고 싶은 row 를 리스트로 불러와 column 으로 설정
    cols = new_df.iloc[1].tolist()
    cols.pop(0)
    cols.insert(0, 'cdate')
    new_df.columns = cols

    # row 1 지우기 (불필요한 row)
    new_df = new_df.drop(1, axis=0)

    # 해당 달, 필요한 값만 읽기
    new_df = new_df[['cdate', 'COAL_AUS', 'COAL_SAFRICA', 'CRUDE_PETRO', 'CRUDE_BRENT', 'CRUDE_DUBAI', 'CRUDE_WTI', 'iNATGAS', 'NGAS_EUR', 'NGAS_US', 'NGAS_JP']]

    # 가장 최신 달의 데이터를 리스트 형식으로 저장
    new_data = new_df.iloc[len(new_df.index) - 1].tolist()

    # 'month' 값을 datetime.date 값으로 변환
    new_month = datetime.datetime.strptime(new_data[0], '%YM%m').date()
    new_data[0] = new_month

    # delete the downloaded xlsx file after collecting data
    os.remove('CMO-Historical-Data-Monthly.xlsx')

    # if the month of the new data is inappropriate, return empty data
    if new_month != target_month:
        print('Wrong month for new data. Update cancelled.')
        return [target_month.date(), None, None, None, None, None, None, None, None, None, None]
    else:
        return new_data


# csv file to MySQL
def toMySQL():
    data_name = 'monthly_commodity'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False, chunksize=10000)

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
                       "CHANGE COLUMN `coal_aus` `coal_aus` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `coal_safrica` `coal_safrica` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `crude_petro` `crude_petro` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `crude_brent` `crude_brent` FLOAT NULL, " \
                       "CHANGE COLUMN `crude_dubai` `crude_dubai` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `crude_wti` `crude_wti` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `ngas_index` `ngas_index` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `ngas_eur` `ngas_eur` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `ngas_us` `ngas_us` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `ngas_jp` `ngas_jp` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`);".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate);".format(table_name)
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
    table_name = 'SMP.eric_monthly_commodity'
    print('Updating {}'.format(table_name))

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
        # get new data by calling update function
        cursor = cnx.cursor()
        new_data = update()
        print('New data to be added :', new_data, '\n')

        # insert into table
        query_string = 'INSERT INTO {} (cdate, coal_aus, coal_safrica, crude_petro, crude_brent, crude_dubai, ' \
                       'crude_wti, ngas_index, ngas_eur, ngas_us, ngas_jp) ' \
                       'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
                       'ON DUPLICATE KEY UPDATE ' \
                       'coal_aus = IF(coal_aus IS NULL, %s, coal_aus), ' \
                       'coal_safrica = IF(coal_safrica IS NULL, %s, coal_safrica), ' \
                       'crude_petro = IF(crude_petro IS NULL, %s, crude_petro), ' \
                       'crude_brent = IF(crude_brent IS NULL, %s, crude_brent), ' \
                       'crude_dubai = IF(crude_dubai IS NULL, %s, crude_dubai), ' \
                       'crude_wti = IF(crude_wti IS NULL, %s, crude_wti), ' \
                       'ngas_index = IF(ngas_index IS NULL, %s, ngas_index), ' \
                       'ngas_eur = IF(ngas_eur IS NULL, %s, ngas_eur), ' \
                       'ngas_us = IF(ngas_us IS NULL, %s, ngas_us), ' \
                       'ngas_jp = IF(ngas_jp IS NULL, %s, ngas_jp);'.format(table_name)
        cursor.execute(query_string, new_data + new_data[1:11])
        cnx.commit()
        print('New data inserted into MySQL table.\n')

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
    table_name = 'SMP.eric_monthly_commodity'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

        # delete the target
        cursor = cnx.cursor()
        cursor.execute("DELETE FROM {} WHERE id > 78".format(table_name))
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


# main function
def main():
    # get the past data and format it into an appropriate csv file
    # get_past_data()

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()


# Manual
# version : 2021-06-29
# 1. Run 'get_past_data'. It will automatically download the file from
#    (https://www.worldbank.org/en/research/commodity-markets)
# 2. Formatted csv file, 'monthly_commodity.csv' will be saved in the directory