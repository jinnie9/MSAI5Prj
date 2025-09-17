서비스 지연 및 부하 발생 원인 추적 
*Tmax Sysmaster 기반으로 수집 된 데이터 중 매 시간마다 지연된 Top100 서비스를 주기적으로 수집
지연/부하 유발되는 서비스 분석 (사용자, RPA, 서비스) + GPT를 이용한 쿼리 튜닝

Azure Web App URL
* https://app-jin-query-01.azurewebsites.net/

<img width="710" height="239" alt="diagram2" src="https://github.com/user-attachments/assets/cd62a3c9-896d-4cd5-b33f-25d140fa2ca8" />

AS-IS

<img width="874" height="395" alt="image" src="https://github.com/user-attachments/assets/adc90669-1eec-4741-aa39-b8db9f343261" />


TO-BE
1) 화면 조회주기 변경 및 DataSource 설정
   - 화면 조회주기 변경
    
    <img width="733" height="94" alt="image" src="https://github.com/user-attachments/assets/153f0478-7f38-4dca-89eb-e7116a90f342" /> 
  - BlobStorage : http호출(AzureFunction)을 통한 JSON 응답 수신 하여 BlobStorage에 저장
  - 로컬 JSON : Python을 통한 Random으로 생성된 JSON 객체로 처리  

    <img width="203" height="83" alt="image" src="https://github.com/user-attachments/assets/9e017fc9-2415-4900-b118-b5e441e9c475" />
    <img width="390" height="153" alt="image" src="https://github.com/user-attachments/assets/8325e619-cb71-46bc-8929-b6e9ac293136" />

2) ElapseTime 그래프 및 리스트 조회
   - 조회조건 : 시작일 / 종료일 / cfg_path
   - 시간대별 평균 응답시간 그래프
   - Top Elapse Time 리스트
     
   <img width="609" height="827" alt="image" src="https://github.com/user-attachments/assets/943a343e-f2db-474d-af2b-9145a0622b1f" />

3) 쿼리 튜닝
   - Top Elapse Time 리스트에서 선택한 쿼리에 대한 튜닝 진행 (gpt4.1-mini)

     <img width="605" height="296" alt="image" src="https://github.com/user-attachments/assets/c46982ae-80cc-40ed-9f65-d695691bdce3" />



   
