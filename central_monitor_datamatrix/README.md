# central_monitor_datamatrix

HL7受信で得たベッド別vitalsを、PHIを含まないDataMatrixペイロードに変換し、監視GUI右下へ表示・スクショ画像からデコードしてJSONL保存する最小実装です。

## 追加/主要ファイル

- `src/dm_payload.py`: 6ベッド×20パラメータ固定レイアウトのバイナリpacket生成/復元
- `src/dm_codec.py`: packetをzlib圧縮しCRC32付きblobへwrap/unwrap
- `src/dm_render.py`: 既存の `zint-bindings` ベース描画実装（`monitor.py` 側で利用）
- `src/make_datamatrix_png.py`: `tool/zint.exe` を subprocess 実行し、`--binary` でPNG生成
- `src/dm_decoder.py`: DataMatrixからバイナリblob抽出（`result.bytes`優先）
- `src/capture_and_decode.py`: PNG/フォルダ入力→デコード→CRC検証→JSONL追記
- `src/monitor.py`: 右下DataMatrix常時表示を組み込み

## 固定レイアウト仕様（version=1）

- 対象ベッド: `BED01`〜`BED06`
- 各ベッド: `PARAMS_20` の20項目を固定順序で保持
- packet構造:
  - header: `magic(4)=CMDM`, `version(1)=1`, `beds_count(1)=6`, `params_count(1)=20`, `reserved(1)=0`, `timestamp_ms(int64)`
  - body: 各ベッドごとに `bed_present(uint8)` + 各パラメータ `present(uint8)` + `value(int32)`
- 浮動小数点が必要な項目は `SCALE_MAP` で量子化（例: `TEMP` は10倍）

## zint.exe 設置

`make_datamatrix_png.py` は `central_monitor_datamatrix/tool/zint.exe` を直接呼び出します。  
**zint 2.16.0 の実行ファイルを `tool/zint.exe` に配置してください。**

## 生成/復号コマンド

```bash
python src/make_datamatrix_png.py --cache monitor_cache.json --out dataset/dm.png
python src/decode_datamatrix_png.py --image dataset/dm.png
```

`make_datamatrix_png.py` は blobサイズ(bytes) をINFO表示し、`decode_datamatrix_png.py` は blob/packetサイズとCRC OKをINFO表示します。

## 動作確認手順（最小）

1) generator起動

```bash
python src/generator.py --host 127.0.0.1 --port 2575 --interval 1.0
```

2) receiver起動

```bash
python src/hl7_receiver.py --host 0.0.0.0 --port 2575 --cache monitor_cache.json
```

3) monitor起動（右下DataMatrix表示）

```bash
python src/monitor.py --cache monitor_cache.json --interval-ms 1000
```

4) monitor画面をPNGスクショ保存

5) スクショからデコードしてJSONL追記

```bash
python src/capture_and_decode.py --input /path/to/screenshot.png --out dataset/dm_results.jsonl
```

6) 出力確認

- `dataset/dm_results.jsonl` に1行JSONで追記
- CRC一致時のみ保存（`crc_ok: true`）
