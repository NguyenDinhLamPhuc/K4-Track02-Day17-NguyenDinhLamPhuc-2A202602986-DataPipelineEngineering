# Bonus B2 — Brainstorm pipeline cho chatbot CSKH tiếng Việt

## Bài toán và giả định

Thiết kế này mở rộng bài lab thành hệ thống hỗ trợ nhân viên chăm sóc khách hàng của một sản phẩm SaaS tại Việt Nam. Chatbot tìm hướng dẫn, gợi ý câu trả lời có nguồn và gán nhãn ticket; nhân viên duyệt trước khi gửi khách hàng. Trace hội thoại và phản hồi của nhân viên tạo dữ liệu đánh giá để cải thiện hệ thống. Mục tiêu là giảm thời gian tìm thông tin mà vẫn kiểm soát câu trả lời sai, dữ liệu cá nhân và tri thức đã hết hiệu lực.

Đây là phương án đề xuất, chưa phải hệ thống production đã triển khai. Các giả định để ra quyết định gồm: 2.000 ticket/ngày, 20.000 sự kiện/ngày, 5.000 tài liệu hướng dẫn, nhóm vận hành hai người; tài liệu mới cần xuất hiện trong tìm kiếm trong 30 phút, báo cáo và tập đánh giá có thể cập nhật hằng ngày. Mục tiêu thử nghiệm là phản hồi P95 dưới 5 giây và ngân sách hạ tầng cộng suy luận tối đa 5 triệu đồng/tháng. Các giá trị này cần được xác nhận bằng đo tải và trao đổi với chủ sản phẩm, không phải số liệu của bộ seed.

Nguồn gồm CDC ticket từ cơ sở dữ liệu, sự kiện click/feedback dạng JSON, transcript hội thoại và tài liệu HTML/PDF tiếng Việt. Khó khăn là bản ghi giao trùng, đến muộn, ticket bị xoá, PDF scan lỗi dấu, tài liệu thay phiên bản và feedback không đồng nghĩa với câu trả lời đúng. Không được đưa toàn bộ transcript chưa xử lý trực tiếp vào prompt hoặc dữ liệu huấn luyện.

## Sơ đồ kiến trúc đề xuất

```text
CDC ticket    JSON event/trace    Transcript    HTML/PDF
     \              |                |           /
      +-------------+----------------+----------+
                    v
       Bronze: dữ liệu gốc + metadata ingest
       manifest batch, quyền truy cập hạn chế
                    |
       parse / schema / dedup / xử lý PII
              |                     |
              v                     v
     Quarantine + cảnh báo     Silver chuẩn hoá
     sửa parser rồi replay     ticket hiện tại + lịch sử
                               event + document version
                                      |
                  +-------------------+------------------+
                  v                   v                  v
        Feature theo event date   Chunk + embedding   Trace đã xử lý
        Snapshot point-in-time    cache theo hash     duyệt + chia tập
                  |                   |                  |
                  v                   v                  v
         Gold feature/label     Index theo tenant    Eval / SFT version
                                      |                  |
                                      v                  v
                             Retrieval -> LLM -> Đánh giá offline
                                      |                  |
                             Nhân viên duyệt <--- cổng phát hành
                                      |
                                 Trace/feedback

Luồng xoá: registry ticket/document -> chặn truy xuất ngay
           -> dọn chunk, index, cache và tập dữ liệu dẫn xuất
```

Bronze là nguồn replay trong thời gian lưu giữ được cho phép; Silver thể hiện trạng thái và lịch sử chuẩn hoá; Gold phục vụ từng nhu cầu cụ thể. Mỗi đầu ra mang batch, phiên bản nguồn và phiên bản transform để truy nguyên lỗi. Sơ đồ là kiến trúc mục tiêu, không hàm ý các thành phần ngoài lab đã tồn tại.

## 1. Cần batch hay streaming để đủ tươi?

**Câu hỏi mở:** Nhân viên cần nội dung mới ngay lập tức hay chỉ cần trong ca làm việc? Có thao tác nào không thể chờ cửa sổ batch?

**Quyết định:** Dùng micro-batch 15 phút cho tài liệu và trạng thái ticket; chạy tổng hợp feature và chuẩn bị tập đánh giá hằng đêm. Yêu cầu xoá hoặc thu hồi quyền đi qua registry riêng để tầng phục vụ chặn ngay, không đợi index được xây lại. Khi registry không kiểm tra được quyền, hệ thống không trả đoạn nội dung đó.

**Đánh đổi:** Streaming toàn bộ giảm độ trễ nhưng tăng công vận hành offset, state và recovery. Batch dễ kiểm tra và replay hơn, đổi lại có khoảng trễ hiển thị. Với giả định độ tươi 30 phút, micro-batch để lại thời gian cho parse và lập chỉ mục. Nếu đo được nhu cầu dưới một phút hoặc hàng đợi thường xuyên quá 30 phút, mới xem xét streaming cho nhánh có nhu cầu đó. Không chọn streaming chỉ vì đã có CDC.

## 2. Làm sao dữ liệu bẩn không đi vào model?

**Câu hỏi mở:** Chặn cả batch khi schema đổi hay cho phần hợp lệ tiếp tục? PII trong tiếng Việt được xử lý ở đâu?

**Quyết định:** Kiểm hợp đồng theo bản ghi: khoá bắt buộc, kiểu timestamp, mã thao tác CDC, enum nhãn và phiên bản schema. Dòng lỗi vào quarantine kèm lý do, vị trí nguồn và phiên bản parser. Batch có manifest số dòng nhận, hợp lệ và lỗi để tránh mất dữ liệu âm thầm. Trường mới không ảnh hưởng hợp đồng được giữ ở Bronze; thay đổi kiểu hoặc thiếu khoá bị cách ly.

Email và số điện thoại được che trước Gold, embedding và LLM; tên, địa chỉ và mã khách hàng cần thêm bước nhận diện cùng kiểm tra mẫu thủ công. Regex của lab chỉ là nền tảng thử nghiệm. Văn bản được chuẩn hoá Unicode nhưng giữ dấu tiếng Việt; PDF có chất lượng OCR thấp không tự động xuất bản vào kho tìm kiếm. Tách dữ liệu theo tenant và truyền quyền truy cập xuống từng chunk.

**Đánh đổi:** Kiểm chặt làm giảm độ phủ lúc đầu, nhưng tránh model học từ dữ liệu sai. Cho các dòng tốt tiếp tục giúp duy trì dịch vụ, song báo cáo phải hiển thị tỷ lệ thiếu. Đề xuất cảnh báo cho người trực pipeline khi quarantine vượt 2% một batch hoặc gấp ba mức nền; đây là ngưỡng thử nghiệm cần hiệu chỉnh. Data owner quyết định sửa dữ liệu hay parser, sau đó replay từ Bronze.

## 3. Replay, dữ liệu muộn và xoá có làm sai trạng thái không?

**Câu hỏi mở:** Nếu batch cũ chạy sau batch mới, hệ thống có làm ticket đã đóng mở lại hoặc hồi sinh ticket đã xoá không?

**Quyết định:** Dedup trong batch theo khoá và thứ tự CDC; ghi Silver bằng MERGE theo ticket, chỉ nhận phiên bản mới hơn. Với nguồn CDC duy nhất của lab, dùng LSN; nếu có nhiều nguồn, phải bổ sung định danh nguồn và quy tắc thứ tự phù hợp, không so LSN giữa các nguồn độc lập. CDC delete lấy khoá từ before/key, giữ tombstone cùng LSN nhưng bỏ PII. Kafka tombstone không được diễn giải thành một thay đổi nghiệp vụ mới.

Feature nhóm theo event time. Trên bộ seed, P99 lateness là 3 ngày nên lookback bằng ceil(P99) = 3; lần chạy 15/08 tính lại cả 12–15/08. Production phải đo lại phân phối này. P99 không bao phủ mọi ngoại lệ: sự kiện ngoài cửa sổ sẽ đánh dấu partition cần backfill, thay vì bị bỏ qua hoặc chuyển sang ngày ingest.

**Đánh đổi:** Giữ metadata tombstone và lịch sử tốn lưu trữ nhưng chống hồi sinh và giúp kiểm toán. Backfill ghi vào bảng tạm, so checksum/contract rồi công bố trong transaction; chỉ một writer được ghi cùng partition. Side-effect gửi tin nhắn nằm ngoài transform, cần khoá idempotency riêng. Việc xoá phải lan tới index, cache LLM và bản sao dataset; metadata chặn replay được giữ theo chính sách tối thiểu. Bronze cũng cần thời hạn lưu và cơ chế xoá, không thể coi “append-only” là giữ PII vĩnh viễn.

## 4. Dùng RAG hay knowledge graph?

**Câu hỏi mở:** Người dùng cần tìm một hướng dẫn cụ thể hay suy luận qua nhiều quan hệ giữa tài khoản, hợp đồng và sản phẩm?

**Quyết định:** Bắt đầu với retrieval kết hợp từ khoá và vector. Từ khoá hỗ trợ mã lỗi, tên gói và thuật ngữ chính xác; vector hỗ trợ cách diễn đạt tiếng Việt khác nhau. Chunk theo tiêu đề và cấu trúc nội dung, gắn document_id, version, tenant, quyền truy cập và nguồn trích dẫn. Chỉ index tài liệu đã duyệt; ticket khách hàng không tự trở thành tri thức dùng chung.

**Đánh đổi:** Vector đơn thuần đơn giản nhưng dễ bỏ sót mã định danh; kết hợp hai kiểu truy xuất thêm bước xếp hạng và đo chất lượng. KG có ích cho truy vấn nhiều bước nhưng cần ontology, trích xuất quan hệ và sửa quan hệ sai. Chưa chọn KG vì nhu cầu ban đầu chủ yếu là tìm hướng dẫn. Chỉ mở nhánh KG nếu tập eval cho thấy một nhóm câu hỏi multi-hop quan trọng liên tục thất bại. Khi chưa đủ bằng chứng từ nguồn, chatbot chuyển cho nhân viên thay vì tự suy diễn.

## 5. Flywheel cải thiện model hay khuếch đại lỗi?

**Câu hỏi mở:** Một câu trả lời được bấm thích có đủ điều kiện làm mẫu SFT không? Làm sao bảo đảm train không nhìn thấy tương lai?

**Quyết định:** Trace lưu phiên bản prompt/model, tài liệu đã truy xuất, thời điểm và phản hồi đã xử lý PII. Chia train/eval theo nhóm hội thoại và thời gian; phát hiện near-duplicate trước khi xuất bản để cùng một vấn đề không lọt sang cả hai tập. Giữ một tập eval cố định có đáp án do người duyệt, không đưa nó vào dữ liệu fine-tune. Feedback chỉ là tín hiệu lấy mẫu; mẫu SFT cần đáp án được sửa và xác nhận.

Feature lúc train và serve dùng cùng định nghĩa. Snapshot tại thời điểm T chỉ dùng dữ liệu đã đến hệ thống lúc T, kể cả khi event_time của dữ liệu giao muộn nhỏ hơn T. Nhãn kết quả tương lai có thể dùng làm target theo cửa sổ đã định nghĩa, nhưng không được đưa vào input. Tài liệu retrieval khi tái hiện một trace cũng cần đúng phiên bản và quyền tại thời điểm đó.

**Đánh đổi:** Duyệt thủ công giảm tốc độ tạo tập huấn luyện nhưng hạn chế tự học lại câu trả lời sai. Ưu tiên lấy mẫu theo nhóm lỗi, ngôn ngữ và độ khó thay vì chỉ chọn phản hồi tích cực. Việc tách theo thời gian làm tập đánh giá khó hơn nhưng gần tình huống phát hành thực tế.

## 6. Điều gì vỡ ở quy mô 10× và ai trả chi phí?

**Câu hỏi mở:** Khi số ticket tăng, nút thắt là database, embedding, LLM hay người duyệt?

**Quyết định:** Cache đầu ra LLM theo hash(input đã xử lý), model và prompt version; kiểm schema trước Gold và cache cả phản hồi sai để replay không gọi lặp. Cache embedding thêm phiên bản model và cấu hình chunk. Ước tính token trên phần cache miss trước khi chạy; job vượt hạn mức cần giảm phạm vi hoặc được chủ sản phẩm duyệt ngân sách.

Chủ sản phẩm chịu ngân sách, người vận hành theo dõi token, cache hit, thời gian OCR, độ trễ batch và hàng đợi duyệt. Chi phí tháng được tính bằng tổng token vào/ra nhân đơn giá thực tế, cộng embedding, compute và lưu trữ; chưa chốt nhà cung cấp hoặc giả vờ có báo giá. Giả thuyết cần đo là suy luận, OCR và công duyệt chiếm phần lớn chi phí.

**Đánh đổi:** Cache giảm chi phí nhưng cần xoá theo lineage và vô hiệu hoá đúng phiên bản. Ở 10×, thử gộp file nhỏ, giới hạn concurrency và chỉ xử lý tài liệu đổi hash trước khi thay nền tảng. Ở 100×, benchmark việc phân vùng dữ liệu, tách worker OCR/embedding và kho phục vụ đa người dùng; DuckDB của lab không mặc nhiên là lựa chọn cho tải production. Ngưỡng chuyển kiến trúc là vi phạm SLA lặp lại sau tối ưu, không chỉ là số lượng công nghệ đang dùng.

## Phương án bị loại và điều kiện xem xét lại

Loại phương án gọi LLM cho toàn bộ ticket ở mỗi lần chạy rồi đưa mọi câu trả lời được thích vào SFT. Nó đơn giản để demo nhưng replay phát sinh chi phí, nhãn có thể thay đổi không truy nguyên được và feedback nhiễu tạo vòng lặp học sai. Cache có phiên bản, schema gate và duyệt dữ liệu giải quyết trực tiếp ba vấn đề này. Việc gọi lại toàn bộ chỉ hợp lý khi chủ động đổi model/prompt và đã ước tính chi phí.

## Cách kiểm chứng và phạm vi bàn giao

Thiết kế kế thừa các tình huống kiểm chứng của lab: chạy lại batch cũ không đổi trạng thái mới; T-97 vẫn bị xoá; sự kiện u05 nhận ngày 15/08 vẫn tính cho ngày 12/08; feature incremental khớp full recompute. Với LLM, kiểm cache hit không gọi model, đổi prompt tạo phiên bản mới và đầu ra sai không vào Gold.

Trước production, cần thêm kịch bản mất kết nối giữa ghi và công bố, sự kiện ngoài lookback, tài liệu bị thu hồi quyền, truy xuất chéo tenant, chất lượng OCR và tài liệu cố chứa chỉ dẫn điều khiển chatbot. Đo retrieval trên tập câu hỏi tiếng Việt do nhân viên duyệt, tỷ lệ trả lời có nguồn và thời gian phản hồi trước khi mở rộng người dùng.

