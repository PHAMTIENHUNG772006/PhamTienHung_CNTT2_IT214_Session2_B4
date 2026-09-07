1. Phân tích rủi ro Coupling ở tầng dữ liệu trong LibraXĐoạn code trong BorrowingService đang vi phạm nghiêm trọng nguyên tắc Database-per-service (mỗi vi dịch vụ sở hữu và toàn quyền quản lý kho dữ liệu của riêng nó). Mô hình hiện tại thực chất chỉ là Distributed Monolith (kiến trúc phân tán vỏ microservice nhưng lõi cơ sở dữ liệu dùng chung).SQLSELECT b.title, m.name FROM borrowings br
JOIN books b ON br.book_id = b.id
JOIN members m ON br.member_id = m.id
WHERE br.id = ?
Các vi phạm và rủi ro cụ thể:Xâm phạm ranh giới sở hữu dữ liệu (Data Domain Boundary Violation):Bảng books thuộc nghiệp vụ quản lý của book-service.Bảng members thuộc nghiệp vụ quản lý của member-service.borrowing-service đọc trực tiếp từ 2 bảng này, bỏ qua toàn bộ tầng Business Logic/Validation của book-service và member-service.Phá vỡ tính độc lập triển khai (Tight Schema Coupling):Nếu nhóm phát triển book-service đổi tên cột books.title thành books.book_name hoặc tách bảng để hỗ trợ đa ngôn ngữ, borrowing-service sẽ lập tức gặp lỗi runtime (BadSqlGrammarException) dù không có thay đổi mã nguồn nào ở phía mượn trả.Mọi hoạt động Migration Schema (Flyway/Liquibase) đều buộc phải đồng bộ và triển khai đồng thời giữa các team.Cạnh tranh tài nguyên và nghẽn kết nối (Resource Contention & Lock Contention):Vì cả 4 service cùng trỏ vào librax_db, một câu query nặng hoặc transaction treo ở borrowing-service có thể chiếm dụng connection pool, khóa bảng (table lock/row lock), làm tê liệt book-service và member-service.Ràng buộc công nghệ lưu trữ (Polyglot Persistence):Dùng chung một MySQL Database ngăn cản các service lựa chọn công nghệ tối ưu riêng (ví dụ: book-service dùng Elasticsearch để tìm kiếm toàn văn, notification-service dùng MongoDB lưu lịch sử thông báo JSON phi cấu trúc).2. Thiết kế tầng dữ liệu theo mô hình Database-per-serviceTách cơ sở dữ liệu librax_db thành 4 cơ sở dữ liệu vật lý (hoặc schema tách biệt hoàn toàn với thông tin xác thực riêng):ServiceTên DatabaseCác bảng sở hữuQuyền hạn truy cậpbook-servicebooks_dbbooks, authors, categoriesĐộc quyền đọc/ghi bởi book-servicemember-servicemembers_dbmembers, memberships, cardsĐộc quyền đọc/ghi bởi member-serviceborrowing-serviceborrowings_dbborrowings, borrowing_itemsĐộc quyền đọc/ghi bởi borrowing-servicenotification-servicenotifications_dbnotifications, templates, logsĐộc quyền đọc/ghi bởi notification-serviceSơ đồ luồng lưu trữ và liên kết dữ liệu:[book-service]        [member-service]       [borrowing-service]       [notification-service]
      │                      │                        │                         │
      ▼                      ▼                        ▼                         ▼
 ┌──────────┐          ┌────────────┐          ┌──────────────┐          ┌──────────────────┐
 │ books_db │          │ members_db │          │borrowings_db │          │ notifications_db │
 └──────────┘          └────────────┘          └──────────────┘          └──────────────────┘
       ▲                      ▲                       │
       │                      │                       │
       └──── [REST / gRPC] ───┴─────── (Giao tiếp) ───┘
Quy tắc tham chiếu: Trong bảng borrowings (borrowings_db), các cột book_id và member_id chỉ lưu dưới dạng Identifier logic (khóa ngoại mềm). Không thiết lập Foreign Key Constraint vật lý xuyên database.3. Tái thiết kế phương thức getBorrowingDetailSử dụng pattern API Composition: borrowing-service truy vấn dữ liệu gốc từ borrowings_db, sau đó gọi song song qua Client REST API của book-service và member-service rồi tổng hợp kết quả trả về.1. DTO phản hồi chi tiếtJavapackage com.librax.borrowing.dto;

public class BorrowingDetailDto {
    private Long borrowingId;
    private String bookTitle;
    private String memberName;

    public BorrowingDetailDto(Long borrowingId, String bookTitle, String memberName) {
        this.borrowingId = borrowingId;
        this.bookTitle = bookTitle;
        this.memberName = memberName;
    }

    public Long getBorrowingId() { return borrowingId; }
    public String getBookTitle() { return bookTitle; }
    public String getMemberName() { return memberName; }

    @Override
    public String toString() {
        return String.format("BorrowingDetail[id=%d, book='%s', member='%s']", borrowingId, bookTitle, memberName);
    }
}
2. Entity/Record nội bộ của borrowing-serviceJavapackage com.librax.borrowing.entity;

public record BorrowingRecord(Long id, Long bookId, Long memberId) {}
3. Service Implementation (Gọi API song song qua CompletableFuture)Javapackage com.librax.borrowing.service;

import com.librax.borrowing.dto.BorrowingDetailDto;
import com.librax.borrowing.entity.BorrowingRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.CompletableFuture;

@Service
public class BorrowingService {

    private static final Logger log = LoggerFactory.getLogger(BorrowingService.class);

    private final JdbcTemplate jdbcTemplate;
    private final RestTemplate restTemplate; // Bean RestTemplate đã được đánh dấu @LoadBalanced

    public BorrowingService(JdbcTemplate jdbcTemplate, RestTemplate restTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.restTemplate = restTemplate;
    }

    public BorrowingDetailDto getBorrowingDetail(Long borrowingId) {
        // Bước 1: Chỉ query bảng nội bộ 'borrowings' thuộc borrowings_db
        String sql = "SELECT id, book_id, member_id FROM borrowings WHERE id = ?";
        BorrowingRecord record;
        try {
            record = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> new BorrowingRecord(
                    rs.getLong("id"),
                    rs.getLong("book_id"),
                    rs.getLong("member_id")
            ), borrowingId);
        } catch (EmptyResultDataAccessException ex) {
            log.warn("Borrowing record not found with id: {}", borrowingId);
            return null;
        }

        // Bước 2: Gọi đồng thời sang book-service và member-service qua API để tối ưu thời gian phản hồi
        CompletableFuture<String> bookTitleFuture = CompletableFuture.supplyAsync(() -> fetchBookTitle(record.bookId()));
        CompletableFuture<String> memberNameFuture = CompletableFuture.supplyAsync(() -> fetchMemberName(record.memberId()));

        // Chờ cả 2 lời gọi hoàn thành
        CompletableFuture.allOf(bookTitleFuture, memberNameFuture).join();

        String bookTitle = bookTitleFuture.join();
        String memberName = memberNameFuture.join();

        // Bước 3: Tổng hợp (Compose) dữ liệu trả về cho client
        return new BorrowingDetailDto(borrowingId, bookTitle, memberName);
    }

    private String fetchBookTitle(Long bookId) {
        try {
            String url = "http://BOOK-SERVICE/api/books/" + bookId + "/title";
            return restTemplate.getForObject(url, String.class);
        } catch (Exception ex) {
            log.error("Failed to fetch book title for bookId {}: {}", bookId, ex.getMessage());
            return "Unknown Book Title (Fallback)";
        }
    }

    private String fetchMemberName(Long memberId) {
        try {
            String url = "http://MEMBER-SERVICE/api/members/" + memberId + "/name";
            return restTemplate.getForObject(url, String.class);
        } catch (Exception ex) {
            log.error("Failed to fetch member name for memberId {}: {}", memberId, ex.getMessage());
            return "Unknown Member Name (Fallback)";
        }
    }
}
4. Các bài toán phát sinh sau khi tách Database và Hướng giải quyếtBài toán 1: Độ trễ tăng cao và Nguy cơ Distributed N+1 (Distributed Queries Latency)Vấn đề: Thay vì 1 câu lệnh JOIN nội bộ trong database tốn vài mili-giây, hệ thống phải thực hiện nhiều lời gọi mạng (I/O network calls) qua HTTP/REST. Khi cần hiển thị danh sách 50 đơn mượn sách, nếu gọi tuần tự sẽ phát sinh $50 \times 2 = 100$ request bổ sung (bài toán Distributed N+1), gây tắc nghẽn đường truyền.Hướng xử lý:Batching Endpoint: Cung cấp API nhận danh sách ID (ví dụ: POST /api/books/batch-titles truyền lên mảng IDs để lấy danh sách tên một lần duy nhất).CQRS & Read Models: Với các màn hình thống kê hoặc tra cứu thường xuyên, áp dụng mô hình CQRS (Command Query Responsibility Segregation). Khi book-service đổi tên sách, nó bắn event sang Message Broker (Kafka/RabbitMQ); borrowing-service lắng nghe event và lưu sẵn bản sao dữ liệu đọc (title, name) vào Read Model của mình để query tại chỗ mà không cần gọi API.Bài toán 2: Mất tính toàn vẹn giao dịch ACID (Distributed Transactions)Vấn đề: Khi một độc giả thực hiện mượn sách, hệ thống phải:Trừ số lượng tồn kho trong books_db (book-service).Kiểm tra hạn mức thẻ trong members_db (member-service).Tạo phiếu mượn trong borrowings_db (borrowing-service).Do nằm trên 3 database tách biệt, hệ thống không thể mở một Transaction @Transactional duy nhất. Nếu bước 3 thất bại sau khi bước 1 đã trừ sách, dữ liệu sẽ rơi vào trạng thái bất nhất.Hướng xử lý:Saga Pattern (Orchestration hoặc Choreography): Chia nghiệp vụ thành chuỗi các local transactions độc lập trên từng service. Nếu một bước thất bại, hệ thống phát sinh chuỗi các giao dịch bù trừ (Compensating Transactions) để hoàn nguyên trạng thái (ví dụ: gọi lệnh cộng lại số lượng sách vào kho).Eventual Consistency (Tính nhất quán cuối cùng): Chấp nhận dữ liệu có thể tạm thời chưa đồng bộ trong vài giây, nhưng đảm bảo sẽ đạt trạng thái nhất quán thông qua cơ chế Outbox Pattern và Message Broker.
