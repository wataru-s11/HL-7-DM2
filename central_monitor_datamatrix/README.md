# central_monitor_datamatrix

HL7受信で得たベッド別vitalsを、PHIを含まないDataMatrixペイロードに変換し、監視GUI右下へ表示・スクショ画像からデコードしてJSONL保存する最小実装です。

## 追加/主要ファイル

- `src/dm_payload.py`: PHIなしpayload生成、`schema_version`、`SeqCounter`
- `src/dm_codec.py`: CRC32付与/検証、圧縮エンコード/デコード
- `src/dm_render.py`: `pylibdmtx` でDataMatrix生成
- `src/dm_decoder.py`: ROI画像からDataMatrixデコード
- `src/capture_and_decode.py`: PNG/フォルダ入力→デコード→CRC検証→JSONL追記
- `src/monitor.py`: 右下DataMatrix常時表示を組み込み

## ペイロード仕様（PHIなし）

`make_payload(monitor_cache, seq)` は以下キーを返します。

- `v`: schema version
- `ts`: ISO8601時刻
- `seq`: 更新連番
- `beds`: `{bed_id: {"vitals": {...}}}`
- vitals項目は数値化可能な`value`のみ採用し、`unit/flag/status`は存在時のみ採用

`encode_payload()` 時に `crc32`（8桁大文字HEX）が付与され、圧縮バイナリ化されます。

## DataMatrix依存 (`pylibdmtx`)

```bash
pip install -r requirements.txt
```

- Linux (Debian/Ubuntu) 例:
  - `sudo apt-get install libdmtx0b libdmtx-dev`
- Windows 例:
  - `pip install pylibdmtx`
  - `libdmtx` DLL (`dmtx.dll`等) をPATHが通る場所へ配置

### Windowsでの典型的なDLLエラー

- `ImportError: Unable to find dmtx shared library` が出る場合、`libdmtx` DLL未配置が原因です。
- Python本体と同じbit数(64bit/32bit)のDLLを使用してください。
- PowerShell再起動後、`python -c "from pylibdmtx.pylibdmtx import encode, decode; print('ok')"` で確認できます。

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

## `capture_and_decode.py` CLI

- `--input <png path or folder>`: 単一画像またはフォルダ
- `--out <jsonl path>`: 出力先
- `--roi "x,y,w,h"` (任意): 明示ROI
- `--last N`: フォルダ入力時の最新N枚（デフォルト10）

`--roi`未指定時は**右下25%×25%**を自動ROIとして使用します。

## 表示座標のデフォルト

- DataMatrix表示位置: `monitor.py` の右下固定
- 余白: 右20px / 下20px
- サイズ: 280x280 px
