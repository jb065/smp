# 필요한 모듈 불러오기
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


# 과거 데이터 csv 파일 정리 (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
def organize_past_data(csv_to_organize, location):
    # csv_to_organize : 정리하려는 csv 파일
    # location : 육지(land) OR 제주(jeju)
    df_past = pd.read_csv(csv_to_organize)

    # 작업 현황 파악을 위한 출력
    print('Organizing', csv_to_organize)

    # column 이름을 한국어에서 영어와 숫자로 변경
    cols = ['date']
    for i in range(1, 25):
        cols.append(i)
    cols.append('max')
    cols.append('min')
    cols.append(location + '_wa')
    df_past.columns = cols

    # 데이터프레임에서 날짜와 가중평균을 제외한 모든 column 삭제
    for i in range(1, 25):
        df_past = df_past.drop(i, axis=1)
    df_past = df_past.drop(['max', 'min'], axis=1)

    # 'date' column 을 datetime.date type 으로 변환
    get_column = df_past['date'].tolist()
    date_column = []
    for i in range(0, len(get_column)):
        clock = datetime.datetime.strptime(str(get_column[i]), '%Y/%m/%d').date()
        date_column.append(clock)
    df_past['date'] = date_column

    # df_past 를 df_smp 로 명세서 형식에 맞게 수정
    wa_cols = ['cdate', location + '_wa']
    df_wa = pd.DataFrame(columns=wa_cols)

    # df_past 에서 날짜 하나씩 데이터 불러와 df_smp 에 저장
    # 저장해야하는 날짜
    correct_date = datetime.datetime(2015, 1, 1).date()
    # 하루의 데이터를 가져와, 시간별로 기입
    for i in range(0, len(df_past.index)):
        get_data = df_past.loc[len(df_past.index) - i - 1].tolist() # 하루의 데이터를 저장하는 리스트

        # 다음 데이터의 날짜가 옳다면 데이터 그대로 저장
        if get_data[0] == correct_date:
            df_wa.loc[len(df_wa)] = get_data
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
                to_add = [correct_date, np.NaN]
                df_wa.loc[len(df_wa)] = to_add
                print('Omitted date:', correct_date)
                correct_date = correct_date + relativedelta(days=1)
            # 새로운 데이터 추가
            df_wa.loc[len(df_wa)] = get_data
            # df_smp 에 추가된 날짜 출력
            print(get_data[0])
            correct_date = correct_date + relativedelta(days=1)

    # 완선된 df_wa 출력
    print(df_wa)

    # 다시 csv 파일에 저장
    df_wa.to_csv(csv_to_organize, index=False, header=True)

    # 작업 현황 파악을 위한 출력
    print('Organizing', csv_to_organize, 'completed\n')


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
    df_smp = reduce(lambda left, right: pd.merge(left, right, on='cdate'), dfs)

    # 합쳐진 데이터프레임 df_smp 의 'date' column 을 datetime.date type 으로 변환
    get_column = df_smp['cdate'].tolist()
    date_column = []

    for i in range(0, len(get_column)):
        clock = datetime.datetime.strptime(str(get_column[i]), '%Y-%m-%d').date()
        date_column.append(clock)
    df_smp['cdate'] = date_column

    # index column 1부터 시작 및 이름 'id' 로 설정
    df_smp.index = np.arange(1, len(df_smp) + 1)
    df_smp.index.name = 'id'

    # 새로운 csv 파일에 저장
    df_smp.to_csv(csv_merged, index=True, header=True)
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
    driver.find_element_by_css_selector('#pageGrid > div > div.opBox > div > span.btnArea > button:nth-child(1)').click()

    # 제주 smp 가 조회될 때 까지 최대 10초 대기
    try:
        element_present = EC.presence_of_element_located((By.CSS_SELECTOR, '#rMateH5__Content1208 > span:nth-child(55)'))
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
    # 과거 데이터 다운로드 (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
    # organize_past_data('daily_land_smp_weighted_average.csv', 'land')
    # organize_past_data('daily_jeju_smp_weighted_average.csv', 'jeju')
    # merge_land_jeju('daily_land_smp_weighted_average.csv', 'daily_jeju_smp_weighted_average.csv', 'daily_smp_weighted_average.csv')

    # MySQL
    # toMySQL()
    updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()
