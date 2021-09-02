# s3-ETL
## Steps

### E:由boto3來下載xetra存在s3的資料
![image](https://github.com/zaqxsw800402/s3-pipeline/blob/develop/picture/0511-0.png?raw=true)
### T:藉由pandas整理資料
將資料濃縮成每日比較表
### L:將資料存回自己的s3裡
![image](https://github.com/zaqxsw800402/s3-pipeline/blob/develop/picture/0511.png?raw=true)
### 測試
將程式碼用class來表示<BR>
增加unit test