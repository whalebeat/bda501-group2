package pagerank.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Driver dieu phoi toan bo pipeline PageRank tren Hadoop that
 * (xem Giai_thich_thuat_toan_Java_Hadoop.md muc 5):
 *
 *   1) Chay PreprocessJob mot lan, lay danglingSum khoi tao.
 *   2) Lap: chay PageRankIterationJob voi danglingSum hien tai, doc
 *      DIFF_SCALED va NEXT_DANGLING_MASS_SCALED tu Counter sau khi job
 *      hoan tat -&gt; neu diff &lt; tol thi dung, nguoc lai dung
 *      NEXT_DANGLING_MASS_SCALED lam danglingSum cho vong ke tiep, va
 *      output vong nay lam input vong sau. Moi vong lap giu nguyen thu
 *      muc output tren HDFS (khong xoa) theo yeu cau
 *      "Keep every iteration output" trong PageRank_Hadoop_Project_Setup_Guide.txt.
 *   3) Doc truc tiep output cuoi cung (chi N dong), sap xep in Top 10 theo
 *      PageRank, in dong CSV_RESULT cung dinh dang voi pagerank.py /
 *      PageRank.java de gop vao bang so sanh hieu nang cua bao cao (Muc 5.1).
 *
 * Cach chay:
 *   hadoop jar pagerank-hadoop.jar pagerank.hadoop.PageRankDriver \
 *       &lt;input_edges_hdfs_path&gt; &lt;output_base_hdfs_dir&gt; \
 *       [damping=0.85] [tolerance=1e-6] [maxIter=20] [datasetLabel=dataset]
 */
public class PageRankDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PageRankDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PageRankDriver <input_edges> <output_base_dir> "
                    + "[damping=0.85] [tolerance=1e-6] [maxIter=20] [datasetLabel=dataset]");
            return 1;
        }

        long tStart = System.currentTimeMillis();

        Path inputEdges = new Path(args[0]);
        Path outputBase = new Path(args[1]);
        double damping = args.length > 2 ? Double.parseDouble(args[2]) : PageRankConstants.DEFAULT_DAMPING;
        double tolerance = args.length > 3 ? Double.parseDouble(args[3]) : PageRankConstants.DEFAULT_TOLERANCE;
        int maxIter = args.length > 4 ? Integer.parseInt(args[4]) : PageRankConstants.DEFAULT_MAX_ITERATIONS;
        String datasetLabel = args.length > 5 ? args[5] : inputEdges.getName();

        Configuration baseConf = getConf();
        FileSystem fs = FileSystem.get(baseConf);

        // Xoa thu muc output cu (neu co) truoc khi chay, tranh loi
        // "output directory already exists" cua Hadoop.
        if (fs.exists(outputBase)) {
            fs.delete(outputBase, true);
        }

        System.out.println("[PageRankDriver] Dem so node (N) bang mot luot quet nhe file dau vao...");
        int numNodes = countDistinctNodes(fs, inputEdges);
        System.out.println("[PageRankDriver] N = " + numNodes + " node");
        System.out.println("[PageRankDriver] damping=" + damping + " tolerance=" + tolerance + " maxIter=" + maxIter);

        // ---- Buoc 1: PreprocessJob (chay 1 lan) ----
        Configuration preConf = new Configuration(baseConf);
        preConf.setInt(PageRankConstants.CONF_NUM_NODES, numNodes);

        Path iter0 = new Path(outputBase, "iter0");
        Job preJob = PreprocessJob.build(preConf, inputEdges, iter0);
        System.out.println("[PageRankDriver] Chay PreprocessJob...");
        if (!preJob.waitForCompletion(true)) {
            System.err.println("PreprocessJob that bai.");
            return 1;
        }

        Counters preCounters = preJob.getCounters();
        long initialDanglingScaled = preCounters
                .findCounter(PageRankConstants.COUNTER_GROUP,
                        PageRankConstants.COUNTER_INITIAL_DANGLING_MASS_SCALED)
                .getValue();
        double danglingSum = initialDanglingScaled / (double) PageRankConstants.SCALE;
        System.out.println("[PageRankDriver] danglingSum khoi tao = " + danglingSum);

        // ---- Buoc 2: Lap PageRankIterationJob ----
        Path prevOutput = iter0;
        int actualIterations = 0;
        double lastDiff = Double.MAX_VALUE;

        for (int i = 1; i <= maxIter; i++) {
            Configuration iterConf = new Configuration(baseConf);
            iterConf.setInt(PageRankConstants.CONF_NUM_NODES, numNodes);
            iterConf.setDouble(PageRankConstants.CONF_DAMPING, damping);
            iterConf.set(PageRankConstants.CONF_DANGLING_SUM, String.valueOf(danglingSum));

            Path currOutput = new Path(outputBase, "iter" + i);
            Job iterJob = PageRankIterationJob.build(iterConf, prevOutput, currOutput);

            System.out.println("[PageRankDriver] Vong lap " + i + " ...");
            if (!iterJob.waitForCompletion(true)) {
                System.err.println("PageRankIterationJob that bai o vong " + i);
                return 1;
            }

            Counters iterCounters = iterJob.getCounters();
            long diffScaled = iterCounters
                    .findCounter(PageRankConstants.COUNTER_GROUP, PageRankConstants.COUNTER_DIFF_SCALED)
                    .getValue();
            long nextDanglingScaled = iterCounters
                    .findCounter(PageRankConstants.COUNTER_GROUP,
                            PageRankConstants.COUNTER_NEXT_DANGLING_MASS_SCALED)
                    .getValue();

            lastDiff = diffScaled / (double) PageRankConstants.SCALE;
            danglingSum = nextDanglingScaled / (double) PageRankConstants.SCALE;
            actualIterations = i;

            System.out.println("[PageRankDriver]   diff = " + lastDiff + ", danglingSum(next) = " + danglingSum);

            prevOutput = currOutput;

            if (lastDiff < tolerance) {
                System.out.println("[PageRankDriver] Da hoi tu sau " + i + " vong lap (diff = " + lastDiff
                        + " < tol = " + tolerance + ")");
                break;
            }
        }

        long tEnd = System.currentTimeMillis();
        double elapsedSeconds = (tEnd - tStart) / 1000.0;

        // ---- Buoc 3: Doc output cuoi cung, in Top 10 ----
        List<NodeRank> results = readFinalOutput(fs, prevOutput);
        results.sort(Comparator.comparingDouble((NodeRank r) -> r.rank).reversed());

        System.out.println();
        System.out.println("===== KET QUA PAGERANK (Top 10) =====");
        System.out.printf("%-4s %-20s %s%n", "#", "Node", "PageRank");
        for (int i = 0; i < Math.min(10, results.size()); i++) {
            NodeRank r = results.get(i);
            System.out.printf("%-4d %-20s %.10f%n", i + 1, r.node, r.rank);
        }

        double totalRank = 0.0;
        for (NodeRank r : results) {
            totalRank += r.rank;
        }
        System.out.println();
        System.out.println("Tong PageRank toan do thi (kiem tra bao toan ~ 1.0): " + totalRank);

        System.out.println();
        System.out.println("CSV_RESULT,hadoop," + datasetLabel + "," + numNodes + "," + actualIterations + ","
                + elapsedSeconds);
        System.out.println();
        System.out.println("[PageRankDriver] Hoan tat trong " + elapsedSeconds + " giay, " + actualIterations
                + " vong lap. Ket qua cuoi cung: " + prevOutput);

        return 0;
    }

    /** Dem so node phan biet (ca cot src va dst) bang mot luot quet nhe file canh. */
    private int countDistinctNodes(FileSystem fs, Path inputEdges) throws IOException {
        Set<String> nodes = new HashSet<>();
        List<Path> files = new ArrayList<>();

        FileStatus status = fs.getFileStatus(inputEdges);
        if (status.isDirectory()) {
            for (FileStatus fst : fs.listStatus(inputEdges)) {
                if (fst.isFile() && !fst.getPath().getName().startsWith("_")) {
                    files.add(fst.getPath());
                }
            }
        } else {
            files.add(inputEdges);
        }

        for (Path p : files) {
            try (FSDataInputStream in = fs.open(p);
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    String[] parts = line.split("\\s+");
                    if (parts.length < 2) {
                        continue;
                    }
                    nodes.add(parts[0].trim());
                    nodes.add(parts[1].trim());
                }
            }
        }
        return nodes.size();
    }

    /** Doc toan bo cac file part-r-* trong thu muc output cuoi cung. */
    private List<NodeRank> readFinalOutput(FileSystem fs, Path outputDir) throws IOException {
        List<NodeRank> results = new ArrayList<>();
        for (FileStatus fst : fs.listStatus(outputDir)) {
            String name = fst.getPath().getName();
            if (!fst.isFile() || name.startsWith("_")) {
                continue;
            }
            try (FSDataInputStream in = fs.open(fst.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    if (parts.length < 2) {
                        continue;
                    }
                    String node = parts[0];
                    double rank = Double.parseDouble(parts[1]);
                    results.add(new NodeRank(node, rank));
                }
            }
        }
        return results;
    }

    private static final class NodeRank {
        final String node;
        final double rank;

        NodeRank(String node, double rank) {
            this.node = node;
            this.rank = rank;
        }
    }
}
