# ca5-jrdb


## バッチ設定
### バッチのタイミング
http://www.jrdb.com/member/data/

- 毎週木曜に翌土日の速報(BAC,BAB, KAB,KAA,KTA)
- 毎週木曜に前週土日の確定(SED,SEC,SKB)
    → ここで前週全日チェックしておく
- 毎週金曜に翌土の速報(PACI)
- 毎週土に翌日の速報(PACI)
- 毎週土に当日の確定(SED,SEC,HJC,TYB)
    → スキップ
- 毎週日に当日の確定(SED,SEC,HJC,TYB)
    → スキップ

## メモ
テスト実行メモ:

```
functions-framework --target test --signature-type event
curl -d '{"data":{"account":"アカウントID", "password":"パスワード"}}' -X POST  "Content-Type: application/json" http://localhost:8080
```


