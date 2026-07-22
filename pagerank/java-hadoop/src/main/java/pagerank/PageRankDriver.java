package pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Driver PageRank tren Hadoop MapReduce that.
 *
 * Cach chay (sau khi "hdfs dfs -put" file canh len HDFS va build jar
 * bang "mvn package"):
 *
 *   hadoop jar pagerank-hadoop-1.0.jar pagerank.PageRankDriver \
 *       <input_edges_hdfs_path> <output_base_hdfs_path> \
 *       [damping=0.85] [tol=1e-8] [maxIter=100]
 *
 * Dinh dang file dau vao: moi dong "src\tdst" (canh huong tu src -> dst),
 * giong het dinh dang dung cho Code/python/pagerank.py va
 * Code/java/PageRank.java.
 *
 * Quy trinh:
 *   1) Quet nhanh file dau vao de dem so node phan biet N (khong phai
 *      buoc tinh toan chinh, chi la buoc chuan bi metadata).
 *   2) Chay PreprocessJob (Mapper/Reducer that) de sinh ban ghi
 *      "node \t rank=1/N \t adjList" tu danh sach canh.
 *   3) Lap lai PageRankIterationJob (Mapper -> Shuffle&Sort -> Reducer
 *      that, dung Hadoop) cho den khi hoi tu (tong do lech < tol) hoac
 *      dat so vong lap toi da. Dangling-node mass va do lech hoi tu duoc
 *      cong don toan cuc bang Hadoop Counters giua cac task doc lap.
 *   4) In Top 10 node theo PageRank va dong CSV_RESULT cung dinh dang voi
 *      pagerank.py / PageRank.java de gop vao bang so sanh hieu nang.
 */
public class PageRankDriver {

    public static final long SCALE = 1_000_000_000_000L;

    public enum PRCounter {
        INITIAL_DANGLING_SCALED,
        NEXT_DANGLING_MASS_SCALED,
        DIFF_SCALED
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Su dung: hadoop jar pagerank-hadoop-1.0.jar pagerank.PageRankDriver "
                    + "<input_edges> <output_base> [damping=0.85] [tol=1e-8] [maxIter=100]");
            System.exit(1);
        }

        String inputPathStr = args[0];
        String outputBaseStr = args[1];
        double damping = args.length > 2 ? Double.parseDouble(args[2]) : 0.85;
        double tol = args.length > 3 ? Double.parseDouble(args[3]) : 1e-8;
        int maxIter = args.length > 4 ? Integer.parseInt(args[4]) : 100;

        long t0 = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Path inputPath = new Path(inputPathStr);
        Path outputBase = new Path(outputBaseStr);
        FileSystem fs = FileSystem.get(conf);

        // Chay lai se xoa ket qua cua lan chay truoc (Hadoop khong cho ghi
        // de thu muc output da ton tai).
        if (fs.exists(outputBase)) {
            fs.delete(outputBase, true);
        }

        int n = countDistinctNodes(fs, inputPath);
        if (n == 0) {
            throw new IllegalStateException("Khong tim thay node nao trong file dau vao: " + inputPathStr);
        }
        System.out.println("So node (N): " + n);

        Path prevOutput = new Path(outputBase, "iter_0");
        double danglingSum = runPreprocess(conf, inputPath, prevOutput, n);
        System.out.printf("Dangling sum khoi tao: %.10f%n", danglingSum);

        int itersUsed = 0;
        double lastDiff = Double.MAX_VALUE;
        for (int iter = 1; iter <= maxIter; iter++) {
            Path curOutput = new Path(outputBase, "iter_" + iter);
            IterationResult result = runIteration(conf, prevOutput, curOutput, n, damping, danglingSum);

            itersUsed = iter;
            lastDiff = result.diff;
            danglingSum = result.danglingSumNext;
            prevOutput = curOutput;

            System.out.printf("Vong lap %d: diff=%.10f%n", iter, result.diff);
            if (result.diff < tol) {
                break;
            }
        }

        long t1 = System.currentTimeMillis();

        List<Map.Entry<String, Double>> ranked = readFinalRanks(fs, prevOutput);
        ranked.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        System.out.println("\nTop 10 node theo PageRank:");
        for (int i = 0; i < Math.min(10, ranked.size()); i++) {
            Map.Entry<String, Double> e = ranked.get(i);
            System.out.printf("  %s\t%.6f%n", e.getKey(), e.getValue());
        }

        System.out.printf("%nSo iteration den khi hoi tu: %d (diff cuoi = %.10f)%n", itersUsed, lastDiff);
        System.out.printf("Tong thoi gian (khong tinh doc du lieu cuc bo): %.4fs%n", (t1 - t0) / 1000.0);

        System.out.printf("CSV_RESULT,hadoop,%s,%d,%d,%.6f%n",
                inputPathStr, n, itersUsed, (t1 - t0) / 1000.0);
    }

    /** Ket qua mot vong lap PageRankIterationJob can cho driver de quyet dinh buoc tiep theo. */
    private static class IterationResult {
        final double diff;
        final double danglingSumNext;

        IterationResult(double diff, double danglingSumNext) {
            this.diff = diff;
            this.danglingSumNext = danglingSumNext;
        }
    }

    private static double runPreprocess(Configuration baseConf, Path input, Path output, int n)
            throws Exception {
        Configuration conf = new Configuration(baseConf);
        conf.setInt("pagerank.totalNodes", n);

        Job job = Job.getInstance(conf, "PageRank Preprocess");
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(PreprocessReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, input);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, output);

        if (!job.waitForCompletion(true)) {
            throw new IOException("PreprocessJob that bai");
        }

        long danglingScaled = job.getCounters().findCounter(PRCounter.INITIAL_DANGLING_SCALED).getValue();
        return danglingScaled / (double) SCALE;
    }

    private static IterationResult runIteration(Configuration baseConf, Path input, Path output,
            int n, double damping, double danglingSum) throws Exception {
        Configuration conf = new Configuration(baseConf);
        conf.setInt("pagerank.totalNodes", n);
        conf.set("pagerank.damping", String.valueOf(damping));
        conf.set("pagerank.danglingSum", String.valueOf(danglingSum));

        Job job = Job.getInstance(conf, "PageRank Iteration");
        job.setJarByClass(PageRankDriver.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, input);
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, output);

        if (!job.waitForCompletion(true)) {
            throw new IOException("PageRankIterationJob that bai");
        }

        long diffScaled = job.getCounters().findCounter(PRCounter.DIFF_SCALED).getValue();
        long danglingScaled = job.getCounters().findCounter(PRCounter.NEXT_DANGLING_MASS_SCALED).getValue();
        return new IterationResult(diffScaled / (double) SCALE, danglingScaled / (double) SCALE);
    }

    /** Quet file/thu muc dau vao mot lan de dem so node phan biet (khong phai buoc tinh toan chinh). */
    private static int countDistinctNodes(FileSystem fs, Path input) throws IOException {
        Set<String> nodes = new HashSet<>();
        for (Path file : listDataFiles(fs, input)) {
            try (FSDataInputStream in = fs.open(file);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split("\t");
                    if (parts.length < 2) continue;
                    nodes.add(parts[0].trim());
                    nodes.add(parts[1].trim());
                }
            }
        }
        return nodes.size();
    }

    /** Doc ket qua vong lap cuoi cung ("node \t rank \t adjList" moi dong) tu cac file part-r-*. */
    private static List<Map.Entry<String, Double>> readFinalRanks(FileSystem fs, Path outputDir)
            throws IOException {
        List<Map.Entry<String, Double>> result = new ArrayList<>();
        for (Path file : listDataFiles(fs, outputDir)) {
            try (FSDataInputStream in = fs.open(file);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    String[] parts = line.split("\t", -1);
                    if (parts.length < 2) continue;
                    result.add(new java.util.AbstractMap.SimpleEntry<>(
                            parts[0], Double.parseDouble(parts[1])));
                }
            }
        }
        return result;
    }

    private static List<Path> listDataFiles(FileSystem fs, Path path) throws IOException {
        List<Path> files = new ArrayList<>();
        FileStatus status = fs.getFileStatus(path);
        if (status.isDirectory()) {
            for (FileStatus child : fs.listStatus(path)) {
                String name = child.getPath().getName();
                if (!child.isDirectory() && !name.startsWith("_")) {
                    files.add(child.getPath());
                }
            }
        } else {
            files.add(path);
        }
        return files;
    }
}
