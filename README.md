# smp_data
## 설명
SMP (계통한계가격) 예측을 위한 데이터 수집하는 프로그램

## 디렉토리 설명
| 디렉토리                        | 설명                   | 데이터 수집 방법 |
|:------------------------------:|:---------------------:|:---------------:|
| daily_smp_weigthed_average     | 일별 SMP 가중평균       | API            |
| forecast_mid                   | 날씨 중기예보 데이터     | API            |
| forecast_ultra                 | 날씨 초단기예보 데이터   | API            |
| forecast_village               | 날씨 동네예보 데이터     | API            |
| hourly_powersupply             | 시간별 전력수급현황      | 웹 크롤링       |
| hourly_smp                     | 시간별 SMP 가격         | API            |
| hourly_temp                    | 각 지역 시간별 기온      | AP            |
| monthly_commodity              | 월별 자원 가격          | 파일 다운로드   |
| monthly_plant                  | 월별 자원별 참여설비용량 | 웹 크롤링       |