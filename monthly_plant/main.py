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
        print('Wrong month for new data. Update cancelled.')
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

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    csv_data = pd.read_csv('{}.csv'.format(data_name))
    engine = create_engine('mysql+mysqldb://{}:{}@{}:3306/SMP'.format(id, pw, ip_address), echo=False)
    csv_data.to_sql(name='eric_{}'.format(data_name), con=engine, if_exists='replace', index=False, chunksize=10000)

    print('{}.csv is added to MySQL\n'.format(data_name))

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
    table_name = 'SMP.eric_monthly_plant'

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
    table_name = 'SMP.eric_monthly_plant'
    print('Updating {}'.format(table_name))

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

        # delete the target
        cursor = cnx.cursor()
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 77".format(table_name)))
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
    # format the downloaded csv file of past data
    # format_csv('monthly_plant.csv')

    # MySQL
    # toMySQL()
    updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()


# Manual
# version : 2021-06-29
# 1. Download csv file from the link (to 2 decimal places)
#    (http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301)
# 2. Save the file as 'monthly_plant.csv'
# 3. run 'format_csv' on the csv file

