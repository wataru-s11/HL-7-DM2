# central_monitor_datamatrix

HL7受信で得たベッド別vitalsを、PHIを含まないDataMatrixペイロードに変換し、監視GUI右下へ表示・スクショ画像からデコードしてJSONL保存する最小実装です。

## 追加/主要ファイル

- `src/dm_payload.py`: 6ベッド×20パラメータ固定レイアウトのバイナリpacket生成/復元
- `src/dm_codec.py`: packetをzlib圧縮しCRC32付きblobへwrap/unwrap
- `src/dm_datamatrix.py`: DataMatrix生成/復号の共通処理（zint実行・decode処理）
- `src/dm_render.py`: 既存の `zint-bindings` ベース描画実装（`monitor.py` 側で利用）
- `src/make_datamatrix_png.py`: `tool/zint.exe` を subprocess 実行し、`--binary` でPNG生成
- `src/dm_display_app.py`: cache更新監視→DataMatrix再生成→小窓表示（送信側）
- `src/dm_capture_decode_app.py`: ROIキャプチャ→DataMatrix decode→JSONL追記（受信側）
- `src/decode_datamatrix_png.py`: PNGからDataMatrix復号（共通関数利用）
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

`make_datamatrix_png.py` / `dm_display_app.py` は `central_monitor_datamatrix/tool/zint.exe` を直接呼び出します。  
**zint 2.16.0 の実行ファイルを `tool/zint.exe` に配置してください。**

## 生成/復号コマンド

```bash
python src/make_datamatrix_png.py --cache monitor_cache.json --out dataset/dm.png
python src/decode_datamatrix_png.py --image dataset/dm.png
```

## Sender/Receiver 実行手順（推奨）

### 1) HL7データ生成（任意）

```bash
python src/generator.py --host 127.0.0.1 --port 2575 --interval 1.0
```

### 2) HL7 receiver起動（cache更新）

```bash
python src/hl7_receiver.py --host 0.0.0.0 --port 2575 --cache monitor_cache.json
```

### 3) 送信側アプリ: DataMatrix小窓表示

cacheファイルの更新mtimeを監視し、更新時に `dataset/dm_latest.png` を再生成して表示更新します。

```bash
python src/dm_display_app.py --cache monitor_cache.json --out dataset/dm_latest.png --interval-sec 1 --monitor-index 1 --margin-right-px 40
```

- デフォルトサイズは `420x420` 固定
- 既定では `--monitor-index 1` の右上に固定表示
- `--margin-right-px` / `--margin-top-px` で表示位置を微調整

### 4) 受信側アプリ: 10秒ごとキャプチャ→decode→JSONL

```bash
python src/dm_capture_decode_app.py --interval-sec 10 --left 1400 --top 20 --width 420 --height 420 --out-jsonl dataset/decoded_results.jsonl
```

- 保存画像: `dataset/captures/YYYYMMDD_HHMMSS.png`
- JSONL: 1行1レコード（`timestamp_ms`, `decoded_at_ms`, `source_image`, `decode_ok`, `crc_ok`, `beds`）
- decode失敗時は `decode_ok:false` と `error` を記録（プロセスは継続）

## ROI決めのコツ

- まず `dm_display_app.py` の座標を固定し、その同じ座標を `dm_capture_decode_app.py` のROIに設定する。
- 余白を含めすぎると認識率が下がるため、DataMatrixがほぼ中央になるようにROIを合わせる。
- Windowsの表示スケーリング（125%/150%）を使う場合、見た目座標と実ピクセル座標がズレるため、ペイント等で実測して調整する。
- decodeが不安定な場合は `width/height` を少し広げるか、表示ウィンドウ位置を固定して再調整する。

## 動作確認手順（最小）

1) `dm_display_app.py` を起動してDataMatrix小窓が表示されることを確認
2) `dm_capture_decode_app.py` を起動して10秒ごとに `dataset/captures/*.png` が増えることを確認
3) `dataset/decoded_results.jsonl` が追記され、成功時は `decode_ok:true` / `crc_ok:true` になることを確認
