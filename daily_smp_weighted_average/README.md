# daily_smp_weighted_average
## 설명
일별 SMP 가중평균값을 수집하는 프로그램

## 사용방법
### 과거 데이터 수집 및 가공
1. EPSIS 웹사이트에서 과거 데이터 csv 파일 다운로드<br>http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202
2. 육지, 제주 파일 나누어 다운로드하고 main.py 파일이 있는 디렉토리에 저장
3. 각 파일명을<br> 'daily_land_smp_weighted_average.csv', <br>'daily_jeju_smp_weighted_average.csv' <br>으로 수정
4. 각 파일에 대하여 'format_csv()' 함수 실행<br>`format_csv('daily_land_smp_weighted_average.csv', 'land')`<br>`format_csv('daily_jeju_smp_weighted_average.csv', 'jeju')`
5. 두 csv 파일에 대하여 'merge_csv()' 함수 실행<br>`merge_csv('daily_land_smp_weighted_average.csv', 'daily_jeju_smp_weighted_average.csv', 'daily_smp_weighted_average.csv')`

### 과거 데이터 MySQL table 에 입력
1. 형식에 맞게 종합된 과거 데이터 csv 파일을 'daily_smp_weighted_average.csv' 이름으로 저장
2. toMySQL() 함수 실행<br>`toMySQL()`
3. 파일명이 함수 내에서 설정되기 때문에 parameter 값 입력 불필요

### 새로운 데이터 수집
1. updateMySQL() 함수 실행
2. 수집 시점 및 주기
	- 1번 / 하루
	- 정오에 프로그램 실행하여 오늘 데이터 수집

## 함수 설명
- <b>format_csv(csv_name, location)
	- 다운로드한 과거 데이터 csv 파일을 형식에 맞게 수정하는 함수
	- 중복된 데이터는 삭제, 누락된 날짜의 데이터는 None 값 부여
	- 새로운 csv 파일을 생성하지 않고, 함수에 입력한 csv 파일을 수정
	- parameters
		- csv_name: (str) 형식을 바꾸고자 하는 csv 파일명
		- location: (str) csv 파일의 대상 ('land' 또는 'jeju')
	- return 값: 없음
	
- <b>merge_csv(csv_land, csv_jeju, csv_merged)
	- 육지(land) 와 제주(jeju) 의 csv 파일을 병합하여 새로운 종합 csv 파일을 생성하는 함수
	- parameters
		- csv_land: (str) 육지 데이터 csv 파일
 		- csv_jeju: (str) 제주 데이터 csv 파일
	- return 값: 없음
	
- <b>toMySQL()
	- 종합된 과거 데이터 csv 파일을 새로운 MySQL table 에 입력하는 함수
	- 테이블에 데이터 입력 후, 각 column 별 설정값 부여 (Type, Not Null, Auto Increment 등)
	- UNIQUE KEY (uidx) 설정 : cdate
	- parameters, return 값: 없음
	
- <b>update()
	- 웹 크롤링을 통해 새로운 데이터를 수집하는 함수
	- http://epsis.kpx.or.kr/epsisnew/selectEkmaSmpShdGrid.do?menuId=050202
	- 가장 최근 날짜의 육지, 제주 가중평균값을 수집
	- 크롤링 실패 케이스
		- 초기 웹페이지 접속 실패: return [오늘 날짜 하루 전 날짜, None, None]
		- 제주 가중평균값 조회 실패: return [수집 데이터, 수집 데이터, None]
	- parameters: 없음
	- return 값: (list) 날짜, 육지 가중평균, 제주 가중평균을 담은 리스트
		- 날짜: datetime.date
		- 육지, 제주 가중평균: float
		- e.g. [datetime.date(2021, 7, 1), 87.2, 109.34]
		
- <b>updateMySQL()
	- update() 함수를 실행시켜 새로운 데이터를 수집하고 MySQL 테이블에 입력
	- MySQL 테이블에 대하여 새로운 데이터의 날짜에 대한 데이터 확인
		- Null 값일 경우 수집한 데이터 입력
		- 데이터가 이미 존재할 경우, 데이터 입력 취소
	- parameters, return 값: 없음
	
- <b>deleteMySQL()
	- MySQL 테이블에서 데이터를 삭제할 때 쓸 수 있는 함수