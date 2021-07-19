# hourly_powersupply
## 설명
시간별 전력수급현황 데이터를 수집하는 프로그램
<br>수집 데이터 : 공급능력, 현재수요, 최대예측수요, 공급예비력, 공급예비율, 운영예비력, 운영예비율

## 함수 설명
- <b>merge_csv()
	- 다운로드한 과거 데이터의 csv 파일을 형식에 맞게 formatting 하면서 하나의 csv 파일로 종합하는 함수
	- parameters
        - csv_merged: 종합된 csv 파일을 저장할 이름
    - return 값: 없음

- <b>filter_wrong_time(df)
	- 데이터프레임을 검토하면서 누락되거나 중복된 시간대를 추출
    - 데이터프레임의 행 하나씩 검토하면서 이전의 행과 1시간 차이가 나는지 확인
	- 과거 데이터의 csv 파일에 누락되거나 중복된 데이터를 확인하는 목적
	- parameters
        - df: 검토하고자 하는 데이터 프레임
    - return 값
        - wrong_time: (list) 잘못된 시간대의 데이터프레임에서의 인덱스를 모은 리스트

- <b>fix_wrong_time(csv_name)
	- 과거 데이터의 csv 파일을 불러와 잘못된 시간대를 수정하는 함수
	- csv 파일을 데이터프레임으로 변환하고, filter_wrong_time() 함수를 실행
    - return 된 시간대에 대하여 누락된 데이터에 NaN값 부여
    - 수정된 데이터프레임을 다시 csv 파일에 저장
	- parameters
        - csv_name: 수정하고자 하는 csv 파일명
    - return 값: 없음

- <b>update()
	- API 를 호출하여 전력수급현황 데이터를 리스트 형식으로 수집하는 함수
	- 현재시간 (서울 timezone) 과 제일 가까운 과거 정각 시각을 target_time 으로 설정
	- API error code 중 재시도 시 성공 가능성이 있는 코드를 리스트로 저장
	- 재시도 가치가 있는 error code 발생 시 30초 간격으로 최대 5번 재시도
	- 5번의 재시도 실패 또는 critical error code 발생 시 template 데이터프레임 return
	- parameters: 없음
	- return 값
		- 데이터프레임: (list) 전력수급현황 데이터를 갖는 리스트
	
- <b>toMySQL()
	- csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : cdate + ctime
	- parameters, return 값: 없음
		
- <b>updateMySQL()
	- update() 함수를 실행시켜 새로운 데이터를 수집하고 MySQL 테이블에 입력
	- MySQL 테이블에 대하여 새로운 데이터의 시간에 대한 데이터 확인
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
1. 아래 링크에서 과거 데이터의 csv 파일을 3개월 단위로 나누어 다운로드
<br>https://openapi.kpx.or.kr/sukub.do
2. 각 파일명을 '20150101-20150331' 형태로 저장
3. 수집한 csv 파일들을 main.py 파일이 있는 디렉토리로 이동
4. main.py 파일이 있는 디렉토리에 수집한 csv 파일 이외 다른 csv 파일이 없어야 함
5. merge() 함수 실행
6. merge() 함수를 통해 생성된 종합 csv 파일에 대하여 fix_wront_time() 함수 실행
7. 과거 데이터를 담는 종합 csv 파일 생성 완료


### 과거 데이터 MySQL table 에 입력
1. toMySQL() 함수 실행<br>`toMySQL()`
2. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행