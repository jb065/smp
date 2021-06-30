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
import sys
import time
import pytz


# format a csv file of past data (http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202)
def format_csv(csv_name, location):
    # csv_name : name of the csv file

    # get dataframe from the csv file
    df_past = pd.read_csv(csv_name)
    print('Formatting', csv_name)

    # change column names from Korean to English
    df_past.columns = ['cdate'] + list(range(1, 25)) + ['max', 'min', 'wa']

    # remove 'max', 'min', 'wa' columns
    df_past = df_past.drop(['max', 'min', 'wa'], axis=1)

    # convert 'cdate' column to datetime.date type, and reverse the dataframe
    df_past['cdate'] = df_past['cdate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y/%m/%d').date())
    df_past = df_past.reindex(index=df_past.index[::-1])

    # move data from df_past to df_wa while checking the dates
    df_smp = pd.DataFrame(columns=['cdate', 'ctime', '{}_smp'.format(location)])
    correct_date = datetime.datetime(2015, 1, 1).date()

    # iterate the row of df_past, transfer each row to df_smp
    for index, row in df_past.iterrows():
        row_data = row.values.tolist()

        # if the date of the row is correct, move data of the row to df_smp
        if row_data[0] == correct_date:
            for j in range(0, 24):
                df_smp.loc[len(df_smp)] = [row_data[0], datetime.time(j, 0, 0), row_data[j + 1]]
            correct_date = correct_date + relativedelta(days=1)
            print(row_data[0])

        # if the date of the row has duplicate, don't move the data to df_smp
        elif row_data[0] == correct_date - relativedelta(days=1):
            print('----------Duplicate date:', row_data[0], '----------')

        # if the date of a row is omitted, add a new row with the date and an empty value until the appropriate date
        else:
            while row_data[0] == correct_date + relativedelta(days=1):
                for k in range(0, 24):
                    df_smp.loc[len(df_smp)] = [correct_date, datetime.time(k, 0, 0), None]
                correct_date = correct_date + relativedelta(days=1)
                print('----------Omitted date: {}----------'.format(correct_date))

            # then add the data of the row to df_wa
            for m in range(0, 24):
                df_smp.loc[len(df_smp)] = [row_data[0], datetime.time(m, 0, 0), row_data[m + 1]]
            correct_date = correct_date + relativedelta(days=1)
            print(row_data[0])

    # save as a new csv file
    print('Formatting', csv_name, 'completed.')
    print('Formatted Dataframe:\n', df_smp)
    df_smp.to_csv(csv_name, index=False, header=True)


# merge csv files
def merge_csv(csv_land, csv_jeju, csv_merged):
    # csv_land : csv file of land_smp
    # csv_jeju : csv file of jeju_smp
    # csv_merged : csv file of both land and jeju smp

    print('Merging', csv_land, '&', csv_jeju, 'to', csv_merged)

    # df_smp : merged dataframe of land and jeju smp
    dfs = [pd.read_csv(csv_land), pd.read_csv(csv_jeju)]
    df_smp = reduce(lambda left, right: pd.merge(left, right, on=['cdate', 'ctime']), dfs)

    # 'cdate' : str -> datetime.date | 'ctime' : str -> datetime.time | add 'id' column
    df_smp['cdate'] = df_smp['cdate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
    df_smp['ctime'] = df_smp['ctime'].apply(lambda x: datetime.datetime.strptime(x, '%H:%M:%S').time())
    df_smp = df_smp.reset_index(drop=True)
    df_smp.insert(0, 'id', np.arange(1, len(df_smp) + 1))

    # save as a new csv file
    df_smp.to_csv(csv_merged, index=False, header=True)
    print('Merging completed to file', csv_merged)


# returns the template of dataframe
def get_template(target_date):
    df = pd.DataFrame(columns=['cdate', 'ctime', 'land_smp', 'jeju_smp'])
    ctime_column = []
    for i in range(0, 24):
        ctime_column.append(datetime.time((i + 1) % 24, 0, 0))

    df['cdate'] = [target_date] * 24
    df['ctime'] = ctime_column
    df['land_smp'] = None
    df['jeju_smp'] = None

    return df


# 새로운 데이터 업데이트 후 return the dataframe
# 한국전력거래소_계통한계가격조회 (https://www.data.go.kr/iim/api/selectAPIAcountView.do)
def update():
    # api input parameter 설정
    url = 'https://openapi.kpx.or.kr/openapi/smp1hToday/getSmp1hToday'
    key = 'mhuJYMs8aVw+yxSF4sKzam/E0FlKQ0smUP7wZzcOp25OxpdG9L1lwA4JJuZu8Tlz6Dtzqk++vWDC5p0h56mtVA=='

    df_update = pd.DataFrame(columns=['cdate', 'ctime', 'land_smp', 'jeju_smp'])
    target_date = datetime.datetime.now(pytz.timezone('Asia/Seoul')).date()
    df_template = get_template(target_date)
    retry_error_code = ['01', '02', '03', '04', '05']

    # try collecting data from API for 5 times
    for j in range(0, 5):
        print('Trial {} : Getting hourly_smp of'.format(j+1), target_date, '\n')
        api_error = False

        for i in range(0, 2):
            if i == 0:
                areaCd = 1
            else:
                areaCd = 9

            queryParams = '?' + urlencode({quote_plus('ServiceKey'): key, quote_plus('areaCd'): areaCd})

            # API 를 통해 데이터 불러와 ElementTree 로 파싱
            response = requests.get(url + queryParams)
            tree = ET.ElementTree(ET.fromstring(response.text))
            result_code = tree.find('.//resultCode').text

            # result_code(00) : If data is appropriately collected
            if result_code == '00':
                # check if the date of the new data is appropriate
                base_date = datetime.datetime.strptime(tree.find('.//tradeDay').text, '%Y%m%d').date()

                if base_date != target_date:
                    print('Trial {} : Wrong month for new data. Returning empty data.'.format(j+1))
                    return df_template
                else:
                    # 새로운 데이터 수집
                    new_data = []
                    for item in tree.findall('.//smp'):
                        new_data.append(item.text)

                    if i == 0:
                        df_update.loc[:, 'land_smp'] = new_data
                    else:
                        df_update.loc[:, 'jeju_smp'] = new_data

            # error worth retry : retry after 2 min
            elif result_code in retry_error_code:
                api_error = True
                break

            # error not worth retry : return empty dataframe
            else:
                print('Trial {} : Critical API Error {}. Returning empty data.\n'.format(j + 1, result_code))
                return df_template

        # if there is an error in API, retry after 2 minutes
        if api_error:
            print('Trial {} : Failed. API Error Code {}. Automatically retry in 2 min'.format(j + 1, result_code), '\n')
            time.sleep(120)
        else:
            break

    # cdate, ctime column 설정
    ctime_column = []
    for j in range(0, 24):
        ctime_column.append(datetime.time(hour=(j + 1) % 24, minute=0, second=0))
    df_update.loc[:, 'ctime'] = ctime_column
    df_update.loc[:, 'cdate'] = datetime.datetime.strptime(tree.find('.//tradeDay').text, '%Y%m%d').date()

    # insert the collected data to the template dataframe and return it
    df_result = df_template.combine_first(df_update)
    df_result = df_result.where(pd.notnull(df_result), None)
    return df_result


# csv file to MySQL
def toMySQL():
    data_name = 'hourly_smp'

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
                       "CHANGE COLUMN `ctime` `ctime` TIME NOT NULL, " \
                       "CHANGE COLUMN `land_smp` `land_smp` FLOAT NULL DEFAULT NULL, " \
                       "CHANGE COLUMN `jeju_smp` `jeju_smp` FLOAT NULL DEFAULT NULL, " \
                       "ADD PRIMARY KEY (`id`); ;".format(table_name)
        cursor.execute(query_string)
        cnx.commit()
        print('Data type and features are set\n')

        # set an unique key
        query_string = "ALTER TABLE {} ADD UNIQUE KEY uidx (cdate, ctime);".format(table_name)
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
    table_name = 'SMP.eric_hourly_smp'
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

    # insert the new data to the table by taking each row
    try:
        # get new data
        cursor = cnx.cursor()
        new_data = update()
        print('New data to be added :\n', new_data, '\n')

        for index, row in new_data.iterrows():
            try:
                # insert into table
                row_data = row.values.tolist()
                query_string = 'INSERT INTO {} (cdate, ctime, land_smp, jeju_smp) ' \
                               'VALUES (%s, %s, %s, %s) ' \
                               'ON DUPLICATE KEY UPDATE ' \
                               'land_smp = IF(land_smp IS NULL, %s, land_smp), ' \
                               'jeju_smp = IF(jeju_smp IS NULL, %s, jeju_smp);'.format(table_name)
                cursor.execute(query_string, row_data + row_data[2:4])
                cnx.commit()
                print('New data inserted into MySQL table.')

            except mysql.connector.Error as error:
                print('Failed to insert into MySQL table. {}\n'.format(error))

            except:
                print("Unexpected error:", sys.exc_info(), '\n')

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
    table_name = 'SMP.eric_hourly_smp'

    with open(r'C:\Users\boojw\OneDrive\Desktop\MySQL_info.txt', 'r') as text_file:
        ip_address = text_file.readline().strip()
        id = text_file.readline().strip()
        pw = text_file.readline().strip()

    # connect to MySQL
    try:
        cnx = mysql.connector.connect(user=id, password=pw, host=ip_address, database='SMP')

        # delete the target
        cursor = cnx.cursor()
        cursor.execute(cursor.execute("DELETE FROM {} WHERE id > 56664".format(table_name)))
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
    # format the downloaded csv files of past data
    # format_csv('hourly_land_smp.csv', 'land')
    # format_csv('hourly_jeju_smp.csv', 'jeju')
    # merge_csv('hourly_land_smp.csv', 'hourly_jeju_smp.csv', 'hourly_smp.csv')

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
# 2. Save each of them as 'hourly_land_smp.csv' and 'hourly_jeju_smp.csv'
# 3. Run 'format_csv' on both files
# 4. Run 'merge_csv' to create a csv file that has the data of both land and jeju
# 5. Three functions can be run all at once
