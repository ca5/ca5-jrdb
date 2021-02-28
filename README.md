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
## deploy
```
gcloud functions deploy main --runtime python38 --trigger-topic トピック名 --project プロジェクト名 --timeout タイムアウト値
```

## メモ
テスト実行メモ:

```
functions-framework --target test --signature-type event

PAYLOAD='{"account":"アカウントID", "password":"パスワード", "start_date": "2005-05-07", "end_date": "2005-05-09"}' && \
curl -d '{"data": {"@type": "pubsub", "attributes":, "data":"'$(echo $PAYLOAD | base64 -w0)'"}}' -X POST -H "Content-Type: application/json" http://localhost:8080
```


