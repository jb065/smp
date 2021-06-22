# 필요한 모듈 불러오기
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


# 과거 데이터 정리 (http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301)
def organize_past_data(csv_to_organize):
    # csv_to_organize : 정리할 과거 데이터를 갖고 있는 csv 파일
    df = pd.read_csv(csv_to_organize)

    # 작업 현황 파악을 위한 출력
    print('Organizing', csv_to_organize)

    # column 이름 영어로 설정, 'location' column 삭제
    df.columns = ['month', 'location', 'nuclear', 'bituminous', 'anthracite', 'oil', 'lng', 'amniotic', 'others',
                  'total']
    df = df.drop('location', axis=1)

    # 'month' column 의 값을 datetime.date 형식으로 변환
    get_column = df['month'].tolist()  # 'month' column 을 리스트로 저장
    new_column = []
    for i in range(0, len(get_column)):
        new_month = datetime.datetime.strptime(df.loc[i].at['month'], '%Y/%m').date()
        new_column.append(new_month)
    df['month'] = new_column

    # 2015년 이전의 데이터는 삭제
    index = new_column.index(datetime.datetime(2015, 1, 1).date())  # 2015년 1월의 인덱스
    to_delete = []  # 삭제할 row 의 인덱스 저장할 리스트
    for i in range(index + 1, len(new_column)):
        to_delete.append(i)
    df = df.drop(to_delete, axis=0)

    # index reset
    df = df.reset_index(drop=True)
    # 과거 데이터가 상단에 위치하도록 설정 후 index reset 한번 더
    df = df.reindex(index=df.index[::-1])
    df = df.reset_index(drop=True)
    # index column 이름 'id' 로 설정
    df.index = np.arange(1, len(df) + 1)
    df.index.name = 'id'

    # 'month' column -> 'cdate' 으로 이름 변경
    df = df.rename(columns={'month': 'cdate'})

    # 다시 csv 파일에 저장
    df.to_csv('monthly_plant_formatted.csv', index=True, header=True)

    # 작업 현황 파악을 위한 출력
    print('Organizing', csv_to_organize, 'completed')


# 새로운 데이터 업데이트
def update():
    # 크롬 창 뜨지 않게 설정 추가
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # url 에 들어가서 html 을 BeautifulSoup 으로 파싱
    driver = webdriver.Chrome(r'C:/Users/boojw/Downloads/chromedriver_win32/chromedriver.exe', options=chrome_options)
    url = 'http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301'
    driver.get(url)

    # 자원별 발전량 데이터가 조회될 때 까지 최대 3초 대기
    # CSS_SELECTOR 중에 해당값이 있을 때 까지 최대 3초 대기
    try:
        element_present = EC.presence_of_element_located((By.CSS_SELECTOR, '#grid1 > div > div > '
                                                                           'div.rMateH5__DataGridBaseContentHolder > '
                                                                           'span:nth-child(9)'))
        WebDriverWait(driver, 3).until(element_present)

    except TimeoutException:
        print('Loading took too much time')
        driver.quit()
        return

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # 업데이트할 데이터의 날짜가 과거 데이터 다음달이 아닌 경우, 업데이트 취소
    new_month = datetime.datetime.strptime(soup.select_one('#grid1 > div > div > '
                                                           'div.rMateH5__DataGridBaseContentHolder > span:nth-child('
                                                           '9)').text, '%Y/%m')
    """
    latest_month = datetime.datetime.strptime(df_past.loc[0].at['cdate'], '%Y-%m-%d')
    if new_month != latest_month + relativedelta(months=1):
        print('Wrong month for new data. Update cancelled.')
        return
    """

    # collect new data (month, nuclear, bituminous, anthracite, oil, lng, amniotic, others, total)
    new_data = [new_month.date(),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(66)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(67)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(68)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(69)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(70)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(71)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(72)').text.replace(',', '')),
                float(soup.select_one('#rMateH5__Content201 > span:nth-child(73)').text.replace(',', ''))]
    # 데이터가 10,000 과 같은 str 형식 -> ','를 없애고 float type 으로 전환

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
    csv_data.to_sql(name='{}_eric'.format(data_name), con=engine, if_exists='replace', index=False)

    print('{}.csv is added to MySQL'.format(data_name))


# update MySQL
def updateMySQL():
    table_name = 'SMP.monthly_plant_eric'

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
    if new_data[0] != last_row[0][1] + relativedelta(days=1):
        print('Update cancelled : Incorrect date for new data')

    else:
        insert_data = [last_id + 1] + new_data
        print(insert_data)

        # insert into table
        try:
            query_string = 'INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'.format(table_name)
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
    table_name = 'SMP.monthly_plant_eric'

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

    # get the last row of the table
    cursor = cnx.cursor()
    cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 76".format(table_name)))
    cnx.commit()

    # close MySQL connection if it is connected
    if cnx.is_connected():
        cursor.close()
        cnx.close()


# main function
def main():
    # 과거 데이터 다운로드 : http://epsis.kpx.or.kr/epsisnew/selectEkmaGcpBftGrid.do?menuId=050301
    # organize_past_data('monthly_plant.csv')
    # update('monthly_plant_formatted.csv')

    # MySQL
    # toMySQL()
    # updateMySQL()
    # deleteMySQL()


if __name__ == '__main__':
    main()