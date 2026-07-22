package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Gom tat ca gia tri "E..." cua mot node thanh adjacency list, ghi ban
 * ghi khoi tao: node \t (1.0/N) \t adjList (adjList rong neu la
 * dangling node - khong co canh ra).
 *
 * N (tong so node) duoc Driver tinh truoc (quet nhanh file input) va
 * truyen vao qua Configuration key "pagerank.totalNodes".
 */
public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {

    private int totalNodes;

    @Override
    protected void setup(Context context) {
        totalNodes = context.getConfiguration().getInt("pagerank.totalNodes", 1);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> adj = new ArrayList<>();
        for (Text v : values) {
            String s = v.toString();
            if (s.startsWith("E")) {
                adj.add(s.substring(1));
            }
            // "N" marker: chi de dam bao key nay ton tai, khong can xu ly gi them
        }

        double initialRank = 1.0 / totalNodes;

        if (adj.isEmpty()) {
            long scaled = Math.round(initialRank * PageRankDriver.SCALE);
            context.getCounter(PageRankDriver.PRCounter.INITIAL_DANGLING_SCALED).increment(scaled);
        }

        String adjList = String.join(",", adj);
        context.write(key, new Text(initialRank + "\t" + adjList));
    }
}
