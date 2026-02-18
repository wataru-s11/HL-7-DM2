# central_monitor_datamatrix

HL7受信で得たベッド別vitalsを、PHIを含まないDataMatrixペイロードに変換し、監視GUI右下へ表示・院外PC側でデコードしてJSONL保存する最小実装です。

## ディレクトリ

```
central_monitor_datamatrix/
  src/
    hl7_sender.py
    hl7_receiver.py
    hl7_parser.py
    generator.py
    monitor.py
    dm_payload.py
    dm_codec.py
    dm_render.py
    dm_decoder.py
    capture_and_decode.py
  dataset/
  requirements.txt
```

## ペイロード仕様（PHIなし）

`dm_payload.make_payload` は以下キーを出力します。

- `v`: schema_version (int)
- `ts`: payload生成時刻 (ISO8601)
- `seq`: monitor更新ごとの連番
- `beds`: ベッドごとの最小vitals情報（`value`, `unit`, `flag`, `ts`）
- `crc32`: CRC32（payload本体から計算）

> `monitor_cache` に `patient` 情報があっても payload には含めません。

## DataMatrixライブラリ

第一候補は `pylibdmtx`（libdmtx依存）です。

- Linux例（Debian/Ubuntu）:
  - `sudo apt-get install libdmtx0b libdmtx-dev`
- Windows例:
  - `pylibdmtx` + libdmtx DLL をPATHに配置

もし `pylibdmtx` が環境で利用できない場合も、**DataMatrixを維持**するために `libdmtx` をシステム導入する方針を推奨します（QRへの置換はしません）。

## 実行手順（最小）

1. 依存インストール

```bash
cd central_monitor_datamatrix
pip install -r requirements.txt
```

2. receiver起動（HL7受信して monitor_cache.json を更新）

```bash
python src/hl7_receiver.py --host 0.0.0.0 --port 2575 --cache monitor_cache.json
```

3. generator起動（テストHL7送信）

```bash
python src/generator.py --host 127.0.0.1 --port 2575 --interval 1.0
```

4. monitor起動（GUI表示 + 右下DataMatrixオーバーレイ）

```bash
python src/monitor.py --cache monitor_cache.json --interval-ms 1000
```

5. monitor画面のスクリーンショットPNGを作成して decode

```bash
python src/capture_and_decode.py --image /path/to/screenshot.png --output-root dataset
```

またはフォルダ最新N枚:

```bash
python src/capture_and_decode.py --image-dir /path/to/screenshots --latest-n 10 --output-root dataset
```

6. 出力確認

- `dataset/YYYYMMDD/dm_results.jsonl` にレコード追記
- CRC32が一致したpayloadのみ保存（不一致は警告ログ）

## 注意

- PHI（患者ID、氏名、生年月日）はDataMatrix payloadへ含めません。
- 失敗時（decode不能、CRC不一致）はjsonl保存せずログ出力します。
