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
    print('')
    # 크롬 창 뜨지 않게 설정 추가
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # url 에 들어가서 html 을 BeautifulSoup 으로 파싱
    driver = webdriver.Chrome(r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)
    url = 'http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202'
    driver.get(url)

    # 육지 smp 가 조회될 때 까지 최대 3초 대기
    # CSS_SELECTOR 중에 해당값이 있을 때 까지 최대 10초 대기
    try:
        element_present = EC.presence_of_element_located((By.CSS_SELECTOR, '#rMateH5__Content404 > span:nth-child(55)'))
        WebDriverWait(driver, 10).until(element_present)

    except TimeoutException:
        print('Land : Loading took too much time. Returning empty data.')
        driver.quit()
        return

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # 새로운 날짜의 데이터를 저장할 리스트 [cdate, land_wa, jeju_wa]
    new_data = []
    # list of weighted average values [land_wa, jeju_wa] -> will be used when converting the values to float type
    value_list = []

    # 'date' 값 추가
    target = soup.select_one('#grid1 > div > div > div.rMateH5__DataGridBaseContentHolder > span:nth-child(8)')
    new_data.append(datetime.datetime.strptime(target.text, '%Y/%m/%d').date())

    # 육지 가중평균값 추가
    target = soup.select_one('#rMateH5__Content404 > span:nth-child(55)')
    value_list.append(target.text)

    # 제주 smp 조회
    driver.find_element_by_css_selector('#selKind2').click()
    driver.find_element_by_css_selector(
        '#pageGrid > div > div.opBox > div > span.btnArea > button:nth-child(1)').click()

    # 제주 smp 가 조회될 때 까지 최대 10초 대기
    try:
        element_present = EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#rMateH5__Content1208 > span:nth-child(55)'))
        WebDriverWait(driver, 10).until(element_present)

    except TimeoutException:
        print('Jeju : Loading took too much time. Returning empty data')
        driver.quit()
        return

    # 제주 가중평균값 추가
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    target = soup.select_one('#rMateH5__Content1208 > span:nth-child(55)')
    value_list.append(target.text)

    # 크롬 드라이버 종료
    driver.close()

    # convert weighted average values to float and add them to new_data list
    new_data = new_data + [float(i) for i in value_list]

    # 수집된 새로운 데이터 return 하기
    return new_data


# csv file to MySQL
def toMySQL():
    # upload csv file to MySQL
    data_name = 'daily_smp_weighted_average'

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
        query_string = "ALTER TABLE {} " \
                       "CHANGE COLUMN `id` `id` INT NOT NULL AUTO_INCREMENT, " \
                       "CHANGE COLUMN `cdate` `cdate` DATE NOT NULL, " \
                       "CHANGE COLUMN `land_wa` `land_wa` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `jeju_wa` `jeju_wa` FLOAT NULL DEFAULT NULL, " \
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


# update to MySQL
def updateMySQL():
    table_name = 'SMP.eric_daily_smp_weighted_average'
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
        # get new data
        cursor = cnx.cursor()
        new_data = update()
        print('new_data :', new_data, '\n')

        # insert the new data to the table
        query_string = 'INSERT INTO {} (cdate, land_wa, jeju_wa) VALUES (%s, %s, %s) ' \
                       'ON DUPLICATE KEY UPDATE ' \
                       'land_wa = IF(land_wa IS NULL, %s, land_wa), ' \
                       'jeju_wa = IF(jeju_wa IS NULL, %s, jeju_wa);'.format(table_name)
        cursor.execute(query_string, new_data + new_data[1:3])
        cnx.commit()
        print('New data inserted into MySQL table.\n')

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

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

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


# main function
def main():
    # format the downloaded csv files of past data
    # format_csv('daily_land_smp_weighted_average.csv', 'land')
    # format_csv('daily_jeju_smp_weighted_average.csv', 'jeju')
    # merge_csv('daily_land_smp_weighted_average.csv', 'daily_jeju_smp_weighted_average.csv', 'daily_smp_weighted_average.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()


# Manual
# version : 2021-06-29
# 1. Download csv files of land and jeju smp weighted average from the link
#    (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
# 2. Save each of them as 'daily_land_smp_weighted_average.csv' and 'daily_jeju_smp_weighted_average.csv'
# 3. Run 'format_csv' on both files
# 4. Run 'merge_csv' to create a csv file that has the data of both land and jeju
# 5. Three functions can be run all at once
