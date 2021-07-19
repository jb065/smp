# monthly_commodity
## 설명
월별 자원 가격 데이터를 수집하는 프로그램
<br>수집 데이터 : 석유 시장가, 석탄 시장가, LNG 시장가 등

## 사용방법
### 과거 데이터 수집 및 가공
1. get_past_data() 함수를 실행시켜 월별 자원 가격 데이터를 csv 파일로 저장
<br>```get_past_data()```
2. 함수를 실행하면 웹사이트에서 csv 파일을 다운받아 형식에 맞게 수정하여 'monthly_commodity.csv' 로 저장됨

### 과거 데이터 MySQL table 에 입력
1. toMySQL() 함수 실행<br>`toMySQL()`
2. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행
2. 수집 시점 및 주기
	- 1번 / 한달
	- 매달 20일에 이전 달의 데이터 수집
	<br>e.g. 7월 20일에 6월 데이터 수집

## 함수 설명
- <b>get_past_data()
	- 웹사이트에서 월별 자원 가격 데이터를 갖는 csv 파일을 다운로드하여 형식에 맞게 수정하는 함수
    - https://www.worldbank.org/en/research/commodity-markets
    - 수정된 csv 파일을 'monthly_commodity.csv' 파일로 저장
	- parameters, return 값: 없음

- <b>update()
	- 웹사이트에서 월별 자원 가격 데이터를 갖는 csv 파일을 다운로드하여 새로운 데이터를 리스트 형식으로 추출하는 함수
	- https://www.worldbank.org/en/research/commodity-markets
	- 웹사이트에 업로드된 csv 파일이 새로운 데이터를 갖고 있지 않으면 데이터에 None 값 부여
	- parameters: 없음
	- return 값
		- new_data: (list) 해당 월의 자원별 가격을 갖는 리스트

- <b>toMySQL()
	- csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : cdate
	- parameters, return 값: 없음

- <b>updateMySQL()
	- update() 함수를 실행시켜 새로운 데이터를 수집하고 MySQL 테이블에 입력
	- MySQL 테이블에 대하여 새로운 데이터의 cdate 에 대한 데이터 확인
		- Null 값일 경우 수집한 데이터 입력
		- 데이터가 이미 존재할 경우, 데이터 입력 취소
	- parameters, return 값: 없음
	
- <b>deleteMySQL()
	- MySQL 테이블에서 데이터를 삭제할 때 쓸 수 있는 함수

- <b>getMySQLInfo()
    - 저장된 MySQL_info.txt 파일에서 MySQL connection 을 생성하는데 필요한 정보 수집
    - host_name, port, db_name, id, pw 값 수집하여 dictionary 형태로 return