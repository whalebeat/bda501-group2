# PageRank trên Hadoop MapReduce — bản Java

Cài đặt Java MapReduce cho PageRank, chạy thật trên Hadoop.

## Cấu trúc

```
java/
├── src/main/java/pagerank/hadoop/
│   ├── PageRankConstants.java     - hằng số dùng chung
│   ├── PreprocessMapper.java      - phát (src,"E"+dst) và (dst,"N")
│   ├── PreprocessReducer.java     - gom adjacency list, gán rank khởi tạo = 1/N
│   ├── PreprocessJob.java         - job chạy 1 lần, chuẩn bị dữ liệu vòng lặp đầu
│   ├── PageRankMapper.java        - chia rank cho neighbor mỗi vòng lặp
│   ├── PageRankReducer.java       - cộng dồn contribution, tính rank mới
│   ├── PageRankIterationJob.java  - 1 vòng lặp MapReduce
│   └── PageRankDriver.java        - main: điều phối N vòng lặp
├── build.sh                       - biên dịch + đóng gói pagerank-hadoop.jar
└── run.sh                         - upload dataset, chạy driver, tải kết quả, ghi benchmark
```

## Tham số mặc định

| Tham số | Giá trị mặc định |
|---|---|
| Damping factor | 0.85 |
| Initial rank | 1/N |
| Max iteration | 20 |
| Convergence threshold | 1e-6 |
| Số Reducer | 1 |
| HDFS replication | 1 |

Có thể ghi đè: `./run.sh <input> <label> [damping] [tolerance] [maxIter]`.

## Build

```bash
cd pagerank/java
./build.sh
```

Yêu cầu lệnh `hadoop` có trong PATH.

## Chạy

```bash
# Toy graph 4 node (test nhanh)
./run.sh ../dataset/toy.tsv toy

# Dataset thật Wiki-Vote
./run.sh ../dataset/Wiki-Vote.txt wikivote
```

`run.sh` sẽ: dọn output cũ → upload lên HDFS → chạy `PageRankDriver` → tải output mọi vòng
lặp về `../output/java_<dataset>/` → ghi log vào `../log/` và benchmark vào
`../benchmark/results.csv`.

## Kiểm chứng kết quả

- Tổng PageRank toàn đồ thị phải xấp xỉ **1.0**.
- Đối chiếu kết quả từng node với bản Python/Java thuần hoặc Pig chạy cùng dataset.

## Ghi chú kỹ thuật

- File SNAP (như `Wiki-Vote.txt`) có dòng comment `#` đầu file — tự động bị bỏ qua.
- Số Reducer cố định = 1 để mỗi vòng lặp ra đúng 1 file output.
- `PageRankDriver` implement `Tool`, nhận tham số chuẩn Hadoop `-D <key>=<value>`.
