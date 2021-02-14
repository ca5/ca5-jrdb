# ca5-jrdb

テスト実行メモ:   
```
functions-framework --target test --signature-type event
curl -d '{"data":{"account":"アカウントID", "password":"パスワード"}}' -X POST  "Content-Type: application/json" http://localhost:8080
```