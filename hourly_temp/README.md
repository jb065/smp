# hourly_temp
## 설명
시간별 각 도시의 기온 데이터를 수집하는 프로그램

## 함수 설명
- <b>get_past_data(end_date)
	- API 를 통해 각 도시의 과거 데이터를 수집하는 함수
	- 2015년 01월 01일부터 설정한 end_date 까지의 데이터 수집
    - 세종시의 경우 2019년 05월 31일 11시 이전의 데이터가 없기 때문에 그 이후부터 end_date 까지의 데이터 수집
    - 도시별 데이터를 'hourly_temp_seoul.csv' 형식의 파일명으로 저장
	- parameters
        - end_date: 데이터를 수집하고자 하는 마지막 날짜
    - return 값: 없음

- <b>fix_sejong()
	- 수집된 세종시의 데이터를 수정하는 함수
    - 세종시의 경우 2019년 05월 31일 11시 이전의 데이터가 없음
    - 2015-01-01 00:00:00 부터 2019-05-31 10:00:00 의 데이터에 np.NaN 값 부여
    - 도시별 데이터를 'hourly_temp_seoul.csv' 형식의 파일명으로 저장
	- parameters, return 값: 없음

- <b>fix_time()
	- 도시별 수집된 과거 데이터의 csv 파일에서 잘못된 시간을 수정하는 함수
    - 중복된 시간대의 데이터는 삭제하고, 누락된 시간에 대하여는 np.Nan 값 부여
    - 디렉토리에 수집된 csv 파일들이 있어야 함
	- parameters, return 값: 없음

- <b>filter_wrong_time(df)
	- input 으로 부여된 데이터프레임을 검토하여 적절하지 않은 시간값을 갖는 행의 index 값을 리스트 형식으로 종합하여 return 하는 함수
    - fix_time() 함수를 실행할 때 사용
	- parameters
        - df: 검토하고자 하는 데이터프레임
    - return 값
        - wrong_time: (list) 잘못된 시간대를 갖는 행의 인덱스들을 담은 리스트

- <b>merge()
	- 도시별 csv 파일을 하나의 csv 파일 (hourly_temp.csv) 로 합치는 함수
    - csv 파일들이 main.py 가 있는 디렉토리에 존재해야 함
	- parameters, return 값: 없음

- <b>get_template(base_date)
	- 특정 날짜 (base_date) 에 API 를 호출했을 때 기대하는 시간별 기온 데이터의 template 을 데이터프레임 형식으로 만들어 return 하는 함수
	- API 호출을 통한 데이터 수집 실패 시 None (Null) 값을 입력하기 위한 목적
	- parameter
		- base_date: 데이터를 수집하고자 하는 대상 날짜
	- return 값
		- df: (pandas.core.frame.DataFrame) 시간별 SMP 데이터의 템플릿 데이터프레임

- <b>update()
	- API 를 호출하여 각 도시 시간별 기온 데이터를 데이터프레임 형식으로 수집하는 함수
	- 어제 날짜 (서울 timezone) 를 target_date 으로 설정
	- API error code 중 재시도 시 성공 가능성이 있는 코드를 리스트로 저장
	- 재시도 가치가 있는 error code 발생 시 2분 간격으로 최대 5번 재시도
	- 5번의 재시도 실패 또는 critical error code 발생 시 template 데이터프레임 return
	- parameters: 없음
	- return 값
		- df: (pandas.core.frame.DataFrame) 모든 도시의 시간별 기온 데이터를 담은 데이터프레임

- <b>toMySQL()
	- csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : cdate + ctime + city
	- parameters, return 값: 없음

- <b>updateMySQL()
	- update() 함수를 실행시켜 새로운 데이터를 수집하고 MySQL 테이블에 입력
	- MySQL 테이블에 대하여 새로운 데이터의 날짜와 시간에 대한 데이터 확인
		- Null 값일 경우 수집한 데이터 입력
		- 데이터가 이미 존재할 경우, 데이터 입력 취소
	- parameters, return 값: 없음
	
- <b>deleteMySQL()
	- MySQL 테이블에서 데이터를 삭제할 때 쓸 수 있는 함수

- <b>getMySQLInfo()
    - 저장된 MySQL_info.txt 파일에서 MySQL connection 을 생성하는데 필요한 정보 수집
    - host_name, port, db_name, id, pw 값 수집하여 dictionary 형태로 return

## 사용방법
### 과거 데이터 수집 및 가공
1. get_past_data() 함수를 실행시켜 각 도시의 시간별 기온 데이터를 csv 파일로 저장
<br>```get_past_data('20210630')```
2. 함수를 실행하면 각 도시별 데이터가 csv 파일로 저장됨 (e.g. 'hourly_temp_seoul.csv')
3. 세종시 데이터는 2019-05-31 11:00:00 부터 수집되기 때문에 fix_sejong() 함수 실행하여 그 이전 데이터에 대하여 np.NaN 갑 부여
<br>```fix_sejong()```
4. 누락 또는 중복된 데이터 처리를 위해 fix_time() 함수 실행
<br>```fix_tim()```
5. merge() 함수를 통해 도시별 데이터를 하나의 csv 파일로 종합
<br>```merge()```

### 과거 데이터 MySQL table 에 입력
1. toMySQL() 함수 실행<br>`toMySQL()`
2. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행