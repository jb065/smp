import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import datetime
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import numpy as np
import mysql.connector
from mysql.connector import errorcode
from sqlalchemy import create_engine
import sys
import pytz


# format a csv file of past data (http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301)
def format_csv(csv_name):
    # csv_name : name of the csv file

    # get dataframe from the csv file
    df = pd.read_csv(csv_name)
    print('Formatting', csv_name)

    # change column names from Korean to English, and delete 'location' column
    df.columns = ['cdate', 'location', 'nuclear', 'bituminous', 'anthracite', 'oil', 'lng', 'amniotic', 'others', 'total']
    df = df.drop('location', axis=1)

    # convert 'cdate' column to datetime.date type
    df['cdate'] = df['cdate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y/%m').date())

    # delete data before year 2015
    for index, row in df.iterrows():
        row_data = row.values.tolist()
        if row_data[0] < datetime.datetime(2015, 1, 1).date():
            df = df.drop(index, axis=0)

    # reverse the dataframe, reset the index, and add 'id' column
    df = df.reindex(index=df.index[::-1])
    df = df.reset_index(drop=True)
    df.insert(0, 'id', np.arange(1, len(df) + 1))

    # save as a new csv file
    print('Formatting', csv_name, 'completed.')
    print('Formatted Dataframe:\n', df)
    df.to_csv(csv_name, index=False, header=True)


# 새로운 데이터 업데이트
def update():
    # target month of data
    target_month = (datetime.datetime.now(pytz.timezone('Asia/Seoul')).replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(months=1)).date()
    print('Collecting data for', target_month)

    # 크롬 창 뜨지 않게 설정 추가
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # url 에 들어가서 html 을 BeautifulSoup 으로 파싱
    driver = webdriver.Chrome(r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)
    url = 'http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301'
    driver.get(url)

    # 자원별 발전량 데이터가 조회될 때 까지 최대 10초 대기
    # CSS_SELECTOR 중에 해당값이 있을 때 까지 최대 10초 대기
    try:
        element_present = EC.presence_of_element_located((By.CSS_SELECTOR, '#grid1 > div > div > '
                                                                           'div.rMateH5__DataGridBaseContentHolder > '
                                                                           'span:nth-child(9)'))
        WebDriverWait(driver, 10).until(element_present)

    except TimeoutException:
        print('Loading took too much time. Returning empty data.')
        driver.quit()
        return [target_month, None, None, None, None, None, None, None, None]

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # check if the month of the new data is appropriate
    new_month = datetime.datetime.strptime(soup.select_one('#grid1 > div > div > '
                                                           'div.rMateH5__DataGridBaseContentHolder > span:nth-child('
                                                           '9)').text, '%Y/%m').date()

    if new_month != target_month:
        print('Wrong month for new data. Returning empty data.')
        return [target_month, None, None, None, None, None, None, None, None]
    else:
        # collect new data (month, nuclear, bituminous, anthracite, oil, lng, amniotic, others, total)
        new_data = [new_month,
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(66)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(67)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(68)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(69)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(70)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(71)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(72)').text.replace(',', '')),
                    float(soup.select_one('#rMateH5__Content201 > span:nth-child(73)').text.replace(',', ''))]
        return new_data


# csv file to MySQL
def toMySQL():
    data_name = 'monthly_plant'
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
        query_string = "ALTER TABLE {} CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT, " \
                       "CHANGE COLUMN `cdate` `cdate` DATE NOT NULL, " \
                       "CHANGE COLUMN `nuclear` `nuclear` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `bituminous` `bituminous` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `anthracite` `anthracite` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `oil` `oil` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `lng` `lng` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `amniotic` `amniotic` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `others` `others` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `total` `total` FLOAT NULL DEFAULT NULL, " \
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


# update MySQL
def updateMySQL():
    table_name = 'SMP.eric_monthly_plant'
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
        # get new data by calling update function
        cursor = cnx.cursor()
        new_data = update()
        print('New data to be added :', new_data)

        # insert the new data to the table
        query_string = 'INSERT INTO {} (cdate, nuclear, bituminous, anthracite, oil, lng, amniotic, others, ' \
                       'total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
                       'ON DUPLICATE KEY UPDATE ' \
                       'nuclear = IF(nuclear IS NULL, %s, nuclear), ' \
                       'bituminous = IF(bituminous IS NULL, %s, bituminous), ' \
                       'anthracite = IF(anthracite IS NULL, %s, anthracite), ' \
                       'oil = IF(oil IS NULL, %s, oil), ' \
                       'lng = IF(lng IS NULL, %s, lng), ' \
                       'amniotic = IF(amniotic IS NULL, %s, amniotic), ' \
                       'others = IF(others IS NULL, %s, others), ' \
                       'total = IF(total IS NULL, %s, total);'.format(table_name)
        cursor.execute(query_string, new_data + new_data[1:9])
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
    table_name = 'SMP.eric_monthly_plant'
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
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 77".format(table_name)))
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
    # format the downloaded csv file of past data
    # format_csv('monthly_plant.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()
