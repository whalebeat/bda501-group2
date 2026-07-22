# PageRank bang Java tren Hadoop MapReduce that

Day la ban cai dat PageRank chay **MapReduce that** (khong mo phong):
Mapper -> Shuffle&Sort (Hadoop tu lam) -> Reducer, lap lai nhieu vong qua
mot driver Java, dung dung API `org.apache.hadoop.mapreduce`. Khac voi
`Code/java/PageRank.java` (ban Java thuan, don may, khong dung Hadoop).

## Cau truc

| File | Vai tro |
|---|---|
| `PreprocessMapper.java` / `PreprocessReducer.java` | Chay 1 lan: chuyen file canh `src\tdst` thanh ban ghi `node \t rank \t adjList` voi rank khoi tao `1/N` |
| `PageRankMapper.java` / `PageRankReducer.java` | Mot vong lap PageRank (goi lai nhieu lan boi Driver): Mapper phat dong gop + bao toan cau truc, Reducer cong don + ap dung cong thuc |
| `PageRankDriver.java` | `main()`: dieu phoi Preprocess + vong lap Iteration den khi hoi tu, in Top 10 + dong CSV_RESULT |

Chi tiet thuat toan (vai tro Mapper/Reducer, xu ly dangling node bang
Counter, kiem tra hoi tu bang Counter) duoc giai thich day du trong
`BaoCao/Giai_thich_thuat_toan_Java_Hadoop.md` - dung de viet bao cao.

## 1. Build (chay o may dev, khong can Hadoop cai san)

```bash
cd Code/java-hadoop
mvn package
```

Ra file `target/pagerank-hadoop-1.0.jar`.

Neu cluster Hadoop tren VM dung phien ban khac `3.3.6`, sua
`<hadoop.version>` trong `pom.xml` cho khop (`hadoop version` tren VM de
kiem tra) roi build lai. `maven.compiler.target=8` da chon de tuong thich
voi hau het cac ban Hadoop 2.x/3.x, ke ca khi VM dung JDK cu hon may dev.

## 2. Mang sang VM va chuan bi du lieu

```bash
# Copy jar + du lieu len VM (scp, hoac shared folder cua may ao)
scp target/pagerank-hadoop-1.0.jar user@vm:~/
scp ../data/small.tsv ../data/medium.tsv ../data/large.tsv user@vm:~/data/

# Tren VM: dua du lieu len HDFS
hdfs dfs -mkdir -p /pagerank/input
hdfs dfs -put ~/data/small.tsv  /pagerank/input/small.tsv
hdfs dfs -put ~/data/medium.tsv /pagerank/input/medium.tsv
hdfs dfs -put ~/data/large.tsv  /pagerank/input/large.tsv
```

## 3. Chay

```bash
hadoop jar pagerank-hadoop-1.0.jar pagerank.PageRankDriver \
    /pagerank/input/small.tsv /pagerank/out_small 0.85 1e-8 100
```

Tham so: `<input_edges> <output_base> [damping=0.85] [tol=1e-8] [maxIter=100]`.

Driver se:
1. Xoa `output_base` cu neu da ton tai (de chay lai khong bi loi "output
   directory already exists" cua Hadoop).
2. Quet nhanh file input de dem so node N.
3. Chay `PreprocessJob` -> ghi ket qua vao `output_base/iter_0`.
4. Lap `PageRankIterationJob` -> ghi ket qua vao `output_base/iter_1`,
   `output_base/iter_2`, ... den khi hoi tu hoac het `maxIter`.
5. In Top 10 node theo PageRank + dong `CSV_RESULT,hadoop,...` ra stdout.

Xem tien trinh Map/Reduce that (so task, % hoan thanh, so luong Counter)
tren YARN ResourceManager UI (`http://<vm>:8088`) hoac log console khi
`hadoop jar` chay.

## 4. Kiem tra ket qua dung

Chay voi `small.tsv` (11 node) phai cho **36 vong lap** va thu tu Top-10
giong het `Code/java/PageRank.java` va `Code/python/pagerank.py` (da co
san trong bao cao, Muc 5.1 - cung 36/27/24 vong lap cho small/medium/large).
Neu so vong lap hoac Top-10 lech nhieu, kiem tra lai gia tri `damping`/`tol`
truyen vao co giong voi hai ban kia khong (mac dinh deu la `0.85`/`1e-8`).

## 5. Doc/xoa ket qua

```bash
hdfs dfs -cat /pagerank/out_small/iter_*/part-r-00000 | head
hdfs dfs -rm -r /pagerank/out_small   # don du lieu trung gian neu can
```

## Gioi han moi truong khi sinh code nay

Code duoc bien dich thu bang `mvn package` o may khong co Hadoop cai san
(chi de bat loi cu phap/API), **chua duoc chay that tren mot Hadoop
cluster/YARN** vi moi truong do khong co Hadoop. Viec chay that tren du
lieu small/medium/large va doi chieu so lieu se do ban thuc hien tren VM
theo huong dan o tren.
