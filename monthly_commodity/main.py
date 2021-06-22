## 필요한 모듈 불러오기
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


def organize_past_data(xlsx_to_organize):
    # xlsx_to_organize : 정리할 xlsx 파일명

    # xlsx 파일을 데이터프레임으로 저장
    # skiprows : 원하지 않는 rows 제외 (상단의 제목 부분)
    df = pd.read_excel(xlsx_to_organize, sheet_name='Monthly Prices', skiprows=[0, 1, 2, 3])

    # column 으로 설정하고 싶은 row 를 리스트로 불러와 column 으로 설정
    cols = df.iloc[1].tolist()
    cols.pop(0)
    cols.insert(0, 'cdate')
    df.columns = cols

    # row 1 지우기 (불필요한 row)
    df = df.drop(1, axis=0)

    # 두바이유, 호주 석탄, 일본 LNG 제외한 모든 columns 삭제
    df = df[['cdate', 'COAL_AUS', 'COAL_SAFRICA', 'CRUDE_PETRO', 'CRUDE_BRENT', 'CRUDE_DUBAI', 'CRUDE_WTI', 'iNATGAS', 'NGAS_EUR', 'NGAS_US', 'NGAS_JP']]

    # index 재설정
    df = df.reset_index(drop=True)
    # column 이름 설정 (소문자로 통일, inatgas 이름 변경)
    df.columns = [x.lower() for x in df.columns]
    df = df.rename(columns={'inatgas': 'ngas_index'})

    # 2015년 이전의 데이터 삭제
    month_column = df['cdate'].tolist()
    month_column.pop(0)
    new_month_column = [np.NaN]  # 새로운 'month' column
    to_delete = []  # 삭제할 2015년 이전의 데이터 인덱스를 담는 리스트

    # 'month' 의 값이 2015년 이전이면, to_delete 에 인덱스를 추가
    for month in month_column:
        month_datetime = datetime.datetime.strptime(month, '%YM%m').date()
        if month_datetime < datetime.datetime(year=2015, month=1, day=1).date():
            to_delete.append(month_column.index(month) + 1)
        else:
            new_month_column.append(month_datetime)

    # to_delete 에 추가된 인덱스 데이터프레임에서 삭제
    df = df.drop(to_delete, axis=0)

    # 'cdate' column 에 새로운 리스트 기입
    df['cdate'] = new_month_column

    # delete 'unit' row
    df = df.drop(0, axis=0)

    # id column 설정 (index 를 리스트로 가져와 첫 element 를 'unit' 으로 설정)
    df = df.reset_index(drop=True)
    df.index = np.arange(1, len(df) + 1)
    id_column = df.index.tolist()
    df.insert(0, 'id', id_column)

    # 완성된 데이터프레임 출력 후 csv 파일에 저장
    print(df)
    df.to_csv('monthly_commodity.csv', index=False, header=True)


def update():
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

    # 해당 달, 두바이유, 호주 석탄, 일본 LNG 값만 읽기
    new_df = new_df[['cdate', 'COAL_AUS', 'COAL_SAFRICA', 'CRUDE_PETRO', 'CRUDE_BRENT', 'CRUDE_DUBAI', 'CRUDE_WTI', 'iNATGAS', 'NGAS_EUR', 'NGAS_US', 'NGAS_JP']]

    # 가장 최신 달의 데이터를 리스트 형식으로 저장
    new_data = new_df.iloc[len(new_df.index) - 1].tolist()

    # 'month' 값을 datetime.date 값으로 변환
    new_month = datetime.datetime.strptime(new_data[0], '%YM%m').date()
    new_data[0] = new_month

    return(new_data)

    """
    # 새로운 데이터가 기존 데이터 다음 달의 데이터가 아닌 경우, 업데이트 취소
    latest_month = datetime.datetime.strptime(df_past.loc[len(df_past.index) - 1].at['cdate'], '%Y-%m-%d').date()
    if new_month != latest_month + relativedelta(months=1):
        print('Wrong month for new data. Update cancelled.')
        os.remove('CMO-Historical-Data-Monthly.xlsx')
        return
    """

    # 다운로드한 xlsx 파일을 지우고 싶을 경우
    os.remove('CMO-Historical-Data-Monthly.xlsx')


# csv file to MySQL
def toMySQL():
    data_name = 'monthly_commodity'

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
    table_name = 'SMP.eric_monthly_commodity'

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
        print('Last row : ', last_row, '\n')

        # get new data by calling update function
        new_data = update()
        print('New data to be added :\n', new_data)

        # check if the new_data is appropriate
        if new_data[0] != last_row[0][1] + relativedelta(months=1):
            print('Update cancelled : Incorrect month for new data')
    
        else:
            # insert into table
            query_string = 'INSERT INTO {} (cdate, coal_aus, coal_safrica, crude_petro, crude_brent, crude_dubai, ' \
                           'crude_wti, ngas_index, ngas_eur, ngas_us, ngas_jp) ' \
                           'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
            cursor.execute(query_string, new_data)
            cnx.commit()
            print('New data inserted into MySQL table.')

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
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id = 78".format(table_name)))
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
    # 과거 데이터 정리
    # xlsx 파일 다운로드 (https://www.worldbank.org/en/research/commodity-markets)
    # organize_past_data('CMO-Historical-Data-Monthly.xlsx')

    # 데이터 업데이트
    # update('cmo_monthly_organized.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()