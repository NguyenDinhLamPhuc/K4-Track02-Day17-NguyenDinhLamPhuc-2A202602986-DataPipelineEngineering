# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Đình Lâm Phúc
**Repo:** https://github.com/NguyenDinhLamPhuc/K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering
**Commit bài nộp:**
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Copilot
**Nguồn tham khảo khác (nếu có):**

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 dòng cho 12 ticket; `T-91` hiện các trạng thái cũ thay vì 1 hàng mới nhất; `T-97` không thành tombstone mà vẫn còn dữ liệu nhạy cảm. | `LOOKBACK_DAYS = 0` nhưng p99 lateness đo từ Bronze là 3 ngày; `u05` trên 08-12 mất 3 event do bị xếp sai ngày; checksum `gold_feature_daily` không khớp với full recompute. | `T-97` vẫn còn ở Silver, snapshot training và RAG index; `gold_doc_chunks` còn chunk của ticket đã xoá; các fail về `gold_training_set` và `gold_doc_chunks` đều lặp lại. |
| **Nguyên nhân gốc** | Không merge theo khoá `ticket_id` và LSN mới nhất; dữ liệu CDC được append thay vì overwrite đúng thực thể; delete `op='d'` chưa được biến thành tombstone và vẫn giữ PII. | Gold xây dựng window theo `day` hiện tại mà không cộng lookback theo độ trễ thực tế; dữ liệu đến muộn 3 ngày bị lọt ra ngoài partition đúng. | Delete trong Debezium là `after = null`, `op = 'd'`; nếu không chuyển thành tombstone, downstream vẫn đọc bản ghi cũ và tiếp tục đưa ticket đã xoá vào snapshot/RAG. |
| **Cách sửa** (file, vài dòng) | Dùng MERGE theo `ticket_id`, cập nhật theo `_lsn` mới nhất; khi `op='d'` thì đặt `is_deleted = true`. | Tăng `LOOKBACK_DAYS` lên  `ceil(p99)` = 3; trong gold, rebuild window `[day - LOOKBACK_DAYS, day]` bằng `event_time` và overwrite-partition theo range. | Xử lý tombstone ở Silver trước, rồi filter `WHERE NOT is_deleted` ở training/RAG; snapshot dựng lại từ Bronze as-of day, không sửa snapshot cũ. |
| **Khái niệm trên slide** | Silver — Có khoá, một hàng = một thực thể, newer LSN wins, delete phải lan. | Late data — lookback = P99 lateness và event time, không phải ingest time. | CDC log-based — “tombstone”, “snapshot bất biến + quyền xoá dữ liệu”. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — gold combined checksum:
  `39e115c510ecdf526800eac227158a4f`
- `gold_feature_daily`: `8630e04a61d1`
- `gold_training_set`: `9370ca77af23`
- `gold_doc_chunks`: `cb9ebd12fdcc`
- dbt build: `PASS=19 WARN=0 ERROR=0`
- parity: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, microbatch theo ngày cho `gold_feature_daily`: vì Silver là state table theo `ticket_id`, nên phải “newer LSN wins”; Gold là daily aggregate theo `event_date`, nên microbatch theo ngày với lookback 3 ngày sẽ xây lại các partition có khả năng nhận late data.
- Tombstone thay vì xoá hẳn hàng trong Silver: vì CDC delete đến dưới dạng `op='d'` với `after=null`; giữ một hàng tombstone với `is_deleted=true` cho phép audit, re-run idempotent và tránh mất lịch sử dữ liệu sự kiện.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: vì training set là immutable snapshot, đúng với yêu cầu điểm thời (point-in-time), nên mỗi `vYYYY-MM-DD` phải độc lập và không bị tác động bởi chạy lại hay dữ liệu mới.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: vì dữ liệu không lớn, workload là ETL/warehouse local, cần tốc độ dev và dễ debug, nên DuckDB + dbt đủ cho demo và parity mà không cần overhead vận hành Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
    Snapshot bất biến phục vụ khả năng tái lập và audit, nhưng quyền được xoá dữ liệu có ưu tiên cao hơn đối với dữ liệu cá nhân. Tôi giữ snapshot lịch sử như một phiên bản logic bất biến, đồng thời chạy quy trình deletion request trên toàn bộ bản sao vật lý. Nếu pháp lý yêu cầu xoá tuyệt đối, snapshot cũ phải được rewrite hoặc huỷ; khi đó ghi lại deletion manifest và metadata audit thay vì giữ văn bản gốc. Các snapshot tạo sau thời điểm xoá sẽ không chứa T-97.

2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
  Đặt PII gate ở Silver sau bước parse CDC và trước mọi Gold, training hoặc RAG; đồng thời kiểm tra lại ở các sink downstream như snapshot và document chunks. Ngoài regex email/số điện thoại, bổ sung bộ nhận diện tên người, địa chỉ, mã khách hàng và một bước kiểm duyệt theo allowlist/denylist hoặc NER. Đo bằng tập dữ liệu PII đã gắn nhãn, theo dõi precision, recall và tỷ lệ rò rỉ; release chỉ được thông qua khi tỷ lệ PII chưa che ở các bảng downstream bằng không hoặc nằm dưới ngưỡng chính sách.
## 5. Output (dán nguyên văn)

```text
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe -m pytest
..................................                                                [100%]
34 passed in 2.06s
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc 39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc 39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc 39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc 39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe main.py --land-only
  2026-08-10  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-11  tickets:already-landed(3)  events:already-landed(5)  transcripts:already-landed(2)
  2026-08-12  tickets:already-landed(5)  events:already-landed(6)  transcripts:already-landed(1)
  2026-08-13  tickets:already-landed(3)  events:already-landed(7)  transcripts:already-landed(1)
  2026-08-14  tickets:already-landed(4)  events:already-landed(4)  transcripts:already-landed(1)
  2026-08-15  tickets:already-landed(4)  events:already-landed(8)  transcripts:already-landed(1)
  2026-08-16  tickets:already-landed(4)  events:already-landed(7)  transcripts:already-landed(2)
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> $env:DO_NOT_TRACK = '1'
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> Push-Location dbt_project
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering\dbt_project> try {
>>     ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
>> } finally {
>>     Pop-Location
>> }
06:17:14  Running with dbt=1.12.5
06:17:15  Registered adapter: duckdb=1.11.0
06:17:15  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
06:17:15  
06:17:15  Concurrency: 1 threads (target='dev')
06:17:15  
06:17:15  1 of 19 START sql view model main.stg_events ................................... [RUN]
06:17:15  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.05s]
06:17:15  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
06:17:15  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
06:17:15  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
06:17:15  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
06:17:15  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
06:17:15  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.09s]
06:17:15  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
06:17:15  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.11s]
06:17:15  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
06:17:15  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
06:17:15  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
06:17:15  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
06:17:15  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
06:17:15  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.01s]
06:17:15  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
06:17:15  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.02s]
06:17:15  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high. [RUN]
06:17:15  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
06:17:15  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
06:17:16  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
06:17:16  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
06:17:16  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
06:17:16  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
06:17:16  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
06:17:16  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
06:17:16  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
06:17:16  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
06:17:16  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.01s]
06:17:16  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
06:17:16  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.03s]
06:17:16  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
06:17:16  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
06:17:16  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.16s]
06:17:16  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
06:17:16  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
06:17:16  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
06:17:16  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
06:17:16  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
06:17:16  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
06:17:16  
06:17:16  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.84 seconds (0.84s).
06:17:16  
06:17:16  Completed successfully
06:17:16  
06:17:16  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree


```
- B1:
```text
(.venv) PS D:\labs\K4-Track02-Day17-NguyenDinhLamPhuc-2A202602986-DataPipelineEngineering> .\.venv\Scripts\python.exe -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```
- B2 brainstorm: `bonus/DESIGN.md`

