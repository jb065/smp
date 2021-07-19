# forecast_village
## 설명
날씨 동네예보 데이터를 수집하는 프로그램

## 함수 설명
- <b>create_csv()
	- get_village() 함수를 실행하여 수집한 데이터프레임을 csv 파일로 저장하는 함수
	- MySQL 테이블에 최초 기입할 때 사용할 csv 파일을 만드는 목적
	- csv 파일을 'forecast_village.csv' 명으로 저장
	- parameters, return 값 없음
	
- <b>get_template(base_time)
	- 특정시간 (base_time) 에 API 를 호출했을 때 기대하는 동네예보 데이터의 template 을 데이터프레임 형식으로 만드는 함수
	- API 를 호출하여 데이터를 수집할 때, 예보값이 없는 시간 또는 날짜가 있기 때문에, 그러한 시간에 대하여는 None (Null) 값을 부여하기 위한 목적
	- API 호출을 통한 데이터 수집 실패 시 None (Null) 값을 입력하기 위한 목적
	- parameter
		- base_time: 동네예보를 발표한 시간
	- return 값
		- df: (pandas.core.frame.DataFrame) 예보값 발표시간에 대한 템플릿 데이터프레임

- <b>get_village()
	- API 를 호출하여 동네예보 데이터를 데이터프레임 형식으로 수집하는 함수
	- 현재시간 (서울 timezone) 과 제일 가까운 과거 동네예보 발표시간을 base_time  로 설정
	- API error code 중 재시도 시 성공 가능성이 있는 코드를 리스트로 저장
	- 재시도 가치가 있는 error code 발생 시 2분 간격으로 최대 5번 재시도
	- 5번의 재시도 실패 또는 critical error code 발생 시 template 데이터프레임 return
	- parameters: 없음
	- return 값
		- 데이터프레임: (pandas.DataFrame) 동네예보 데이터를 갖는 데이터프레임
	
- <b>toMySQL()
	- csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : base_date + base_time + target_date + target_time + city
	- parameters, return 값: 없음
		
- <b>updateMySQL()
	- get_village() 함수를 실행시켜 새로운 데이터를 수집하고 MySQL 테이블에 입력
	- 호출하는 시점에서 가장 최근의 동네예보 수집
	- MySQL 테이블에 대하여 새로운 데이터의 날짜에 대한 데이터 확인
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
1. 동네예보는 과거데이터 수집이 불가
2. 자동화 준비 완료 후, 첫 동네예보 데이터를 수집하여 csv 파일로 저장하고, MySQL 에 업로드해야 함
3. create_csv() 함수 실행 (디렉토리에 'forecast_village.csv' 파일 생성)


### 과거 데이터 MySQL table 에 입력
1. toMySQL() 함수 실행<br>`toMySQL()`
2. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행
2. 수집 시점 및 주기
	- 8번 / 하루
	- 02:10, 05:10, 08:10 ... 20:10, 23:10
	- 동네예보는 하루 8번 발표되고, 발표 10분 이후부터 API 로 제공