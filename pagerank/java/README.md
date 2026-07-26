# PageRank trên Hadoop MapReduce — bản Java

Cài đặt Java MapReduce API cho thuật toán PageRank, chạy **thật** trên Hadoop
(không mô phỏng) — theo đúng đặc tả trong `Giai_thich_thuat_toan_Java_Hadoop.md`
và `PageRank_Hadoop_Project_Setup_Guide.txt`.

## Cấu trúc

```
java/
├── src/main/java/pagerank/hadoop/
│   ├── PageRankConstants.java     - hằng số dùng chung (damping, tolerance, tên counter...)
│   ├── PreprocessMapper.java      - PreprocessJob: đọc file cạnh -> phát (src,"E"+dst) và (dst,"N")
│   ├── PreprocessReducer.java     - gom adjacency list, gán rank khởi tạo = 1/N
│   ├── PreprocessJob.java         - job chạy 1 lần, chuẩn bị dữ liệu vòng lặp đầu
│   ├── PageRankMapper.java        - mỗi vòng lặp: giữ cấu trúc đồ thị + chia rank cho neighbor
│   ├── PageRankReducer.java       - cộng dồn contribution, tính rank mới, xử lý dangling node
│   ├── PageRankIterationJob.java  - 1 vòng lặp MapReduce (Input->Map->Shuffle&Sort->Reduce->Output)
│   └── PageRankDriver.java        - main: điều phối N vòng lặp, đọc Hadoop Counter, in kết quả
├── build.sh                       - biên dịch + đóng gói pagerank-hadoop.jar
└── run.sh                         - upload dataset lên HDFS, chạy driver, tải kết quả về, ghi benchmark
```

## Thuật toán (tóm tắt — xem đầy đủ trong `Giai_thich_thuat_toan_Java_Hadoop.md`)

Định dạng bản ghi mỗi vòng lặp: `node<TAB>rank<TAB>adjacencyList` (phân cách bởi dấu phẩy,
rỗng nếu node dangling).

- **PreprocessJob** (chạy 1 lần): file cạnh thô `src<TAB>dst` → bản ghi PageRank ban đầu
  (`rank = 1/N` cho mọi node, kể cả node chỉ được trỏ đến mà không trỏ đi đâu).
- **PageRankIterationJob** (chạy lặp lại N lần, driver Java gọi lại): mỗi vòng là 1 job
  MapReduce độc lập — Mapper chia rank hiện tại cho out-degree và phát contribution tới
  từng neighbor; Reducer cộng dồn contribution, áp dụng công thức
  `newRank = (1-d)/N + d × (sumContrib + danglingSum/N)`.
- **Dangling node & hội tụ dùng Hadoop Counter**: vì mỗi Reducer chỉ thấy 1 node, không có
  cách nào biết tổng toàn cục (tổng rank rò rỉ từ dangling node, tổng độ lệch hội tụ) nếu
  không dùng Hadoop Counters. Bản này dùng 2 counter: `NEXT_DANGLING_MASS_SCALED` (dangling
  mass cho vòng kế tiếp, cộng ở Reducer dựa trên rank **vừa tính xong**, không phải rank cũ —
  xem bug đã phát hiện và sửa ở mục 4 của tài liệu lý thuyết) và `DIFF_SCALED` (tổng
  `|newRank-oldRank|`, dùng để dừng vòng lặp khi hội tụ). Counter chỉ nhận số nguyên `long`
  nên mọi giá trị double được nhân với `SCALE = 10^12` trước khi cộng vào counter.

## Tham số mặc định (theo `PageRank_Hadoop_Project_Setup_Guide.txt`)

| Tham số | Giá trị mặc định |
|---|---|
| Damping factor | 0.85 |
| Initial rank | 1/N |
| Max iteration | 20 |
| Convergence threshold | 1e-6 |
| Số Reducer | 1 |
| HDFS replication | 1 (đã cấu hình sẵn trong `hdfs-site.xml`) |

Có thể ghi đè khi chạy: `./run.sh <input> <label> [damping] [tolerance] [maxIter]`.

## Build

```bash
cd pagerank/java
./build.sh
```

Sinh ra file `pagerank-hadoop.jar`. Yêu cầu lệnh `hadoop` có trong PATH (đã có sẵn nếu
làm theo hướng dẫn setup Hadoop trước đó).

## Chạy

```bash
# Test nhanh với đồ thị toy 4 node (có 1 dangling node D) để kiểm tra logic đúng
./run.sh ../dataset/toy.tsv toy

# Chạy với dataset thật Wiki-Vote (SNAP, ~7.115 node, ~103.689 cạnh)
./run.sh ../dataset/Wiki-Vote.txt wikivote
```

Mỗi lần chạy `run.sh` sẽ:
1. Dọn output cũ (HDFS + local) của đúng dataset đó.
2. Upload file cạnh lên HDFS (`/pagerank/java/input/`).
3–4. Chạy `PageRankDriver` (tự lặp cho tới khi hội tụ hoặc hết `maxIter`), in thời gian
   thực thi + dòng `CSV_RESULT,hadoop,<dataset>,<N>,<iterations>,<seconds>`.
5. Tải **toàn bộ** output từng vòng lặp về `../output/java_<dataset>/` (giữ lại mọi vòng,
   theo đúng yêu cầu "Keep every iteration output" trong tài liệu setup).
6. Ghi log đầy đủ vào `../log/`, và append dòng kết quả vào `../benchmark/results.csv`.

## Kiểm chứng kết quả

- Driver tự in **tổng PageRank toàn đồ thị** ở cuối — phải xấp xỉ **1.0** (sai số nhỏ do
  làm tròn số thực); nếu lệch nhiều nghĩa là xử lý dangling node bị sai.
- Với `toy.tsv`, nên đối chiếu chéo với bản Python/Java thuần (`pagerank.py` /
  `PageRank.java` trong đề tài) chạy cùng đồ thị, cùng damping/tolerance — PageRank từng
  node phải khớp (chỉ lệch rất nhỏ do làm tròn), đúng phương pháp kiểm chứng đã dùng trong
  báo cáo (Mục 3.4 của `PageRank_BaoCao.docx`, mô phỏng Pig bằng Python/pandas).
- Với `Wiki-Vote.txt`: đối chiếu top-N node có PageRank cao nhất với các bản Python/Pig
  chạy cùng dataset (nếu có).

## Ghi chú kỹ thuật

- File input SNAP (như `Wiki-Vote.txt`) có 4 dòng comment bắt đầu bằng `#` ở đầu file —
  `PreprocessMapper` và bước đếm N trong `PageRankDriver` tự động bỏ qua các dòng này.
- Số Reducer cố định = 1 (`PageRankConstants.NUM_REDUCERS`) theo yêu cầu trong tài liệu
  setup, để đảm bảo output mỗi vòng lặp luôn là 1 file duy nhất, dễ theo dõi/debug.
- `PageRankDriver` implement `Tool` (dùng `ToolRunner`) nên vẫn nhận được các tham số
  chuẩn của Hadoop như `-D <key>=<value>` nếu cần tinh chỉnh thêm.
