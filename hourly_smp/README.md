# hourly_smp
## 설명
시간별 SMP 데이터를 수집하는 프로그램

## 함수 설명
- <b>format_csv(csv_name, location)
	- 다운로드한 과거 데이터 csv 파일을 형식에 맞게 formatting 하고 다시 csv 파일에 저장하는 함수
	- formatting 과정에서 누락되거나 중복되는 데이터는 추가 또는 삭제
	- parameters
        - csv_name: formatting 하고자 하는 csv 파일명
        - location: 육지인지 제주인지 나타태는 값 ('land' 또는 'jeju')
    - return 값: 없음

- <b>merge_csv(csv_land, csv_jeju, csv_merged)
	- 형식에 맞게 수정된 육지와 제주 csv 파일들을 하나이 csv 파일로 합치는 함수
	- parameters
        - csv_land: 육지 데이터를 담는 csv 파일명
        - csv_jeju: 제주 데이터를 담는 csv 파일명
        - csv_merged: 육지와 제주 데이터를 담는 새로운 종합 csv 파일명
    - return 값: 없음

- <b>get_template(target_date)
	- 특정 날짜 (target_date) 에 API 를 호출했을 때 기대하는 시간별 SMP 데이터의 template 을 데이터프레임 형식으로 만들어 return 하는 함수
	- API 호출을 통한 데이터 수집 실패 시 None (Null) 값을 입력하기 위한 목적
	- parameter
		- target_date: 데이터를 수집하고자 하는 대상 날짜
	- return 값
		- df: (pandas.core.frame.DataFrame) 시간별 SMP 데이터의 템플릿 데이터프레임

- <b>update()
	- API 를 호출하여 시간별 SMP 데이터를 리스트 형식으로 수집하는 함수
	- 현재 날짜 (서울 timezone) 를 target_date 으로 설정
	- API error code 중 재시도 시 성공 가능성이 있는 코드를 리스트로 저장
	- 재시도 가치가 있는 error code 발생 시 2분 간격으로 최대 5번 재시도
	- 5번의 재시도 실패 또는 critical error code 발생 시 template 데이터프레임 return
	- parameters: 없음
	- return 값
		- df: (pandas.core.frame.DataFrame) 시간별 SMP 데이터를 담은 데이터프레임

- <b>toMySQL()
	- csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : cdate + ctime
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
1. 아래 링크에서 각각 육지와 제주에 대한 과거 데이터의 csv 파일 다운로드
<br>http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202
2. 각 파일명을 'hourly_land_smp.csv', 'hourly_jeju_smp.csv' 로 저장
3. 수집한 csv 파일들을 main.py 파일이 있는 디렉토리로 이동
4. 두 파일에 대하여 format_csv() 함수 실행하여 형식에 맞게 수정
<br>```format_csv('hourly_land_smp.csv', 'land')```
<br>```format_csv('hourly_jeju_smp.csv', 'jeju')```
5. merge_csv() 함수 실행하여 육지와 제주 데이터를 종합하는 csv 파일 생성
<br>```merge_csv('hourly_land_smp.csv', 'hourly_jeju_smp.csv', 'hourly_smp.csv')```
7. 과거 데이터를 담는 종합 csv 파일 ('hourly_smp.csv') 생성 완료


### 과거 데이터 MySQL table 에 입력
1. toMySQL() 함수 실행<br>`toMySQL()`
2. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행
2. 수집 시점 및 주기
	- 1번 / 하루
	- 정오에 프로그램 실행하여 오늘 데이터 수집