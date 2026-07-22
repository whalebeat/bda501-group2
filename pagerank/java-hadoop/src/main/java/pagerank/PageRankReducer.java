package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Nhan tat ca gia tri cua mot node: dung mot gia tri "S..." (rank cu +
 * adjacency list) va nhieu gia tri "C..." (dong gop PageRank tu cac node
 * lang gieng tro toi). Cong don dong gop, ap dung cong thuc PageRank
 * (co cong ca phan dangling-mass cua vong lap truoc), ghi ban ghi moi va
 * cap nhat Counter do lech hoi tu.
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private double damping;
    private int totalNodes;
    private double danglingSumPrev;

    @Override
    protected void setup(Context context) {
        org.apache.hadoop.conf.Configuration conf = context.getConfiguration();
        damping = Double.parseDouble(conf.get("pagerank.damping", "0.85"));
        totalNodes = conf.getInt("pagerank.totalNodes", 1);
        danglingSumPrev = Double.parseDouble(conf.get("pagerank.danglingSum", "0.0"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double oldRank = 0.0;
        String adjList = "";
        double sumContrib = 0.0;

        for (Text v : values) {
            String s = v.toString();
            if (s.charAt(0) == 'S') {
                int sep = s.indexOf('|');
                oldRank = Double.parseDouble(s.substring(1, sep));
                adjList = s.substring(sep + 1);
            } else if (s.charAt(0) == 'C') {
                sumContrib += Double.parseDouble(s.substring(1));
            }
        }

        double newRank = (1.0 - damping) / totalNodes
                + damping * (sumContrib + danglingSumPrev / totalNodes);

        long diffScaled = Math.round(Math.abs(newRank - oldRank) * PageRankDriver.SCALE);
        context.getCounter(PageRankDriver.PRCounter.DIFF_SCALED).increment(diffScaled);

        // Neu node nay la dangling, cong don newRank (rank cua VONG HIEN TAI,
        // khong phai rank cu) de Driver dung lam danglingSum cho vong lap ke tiep.
        if (adjList.isEmpty()) {
            long scaled = Math.round(newRank * PageRankDriver.SCALE);
            context.getCounter(PageRankDriver.PRCounter.NEXT_DANGLING_MASS_SCALED).increment(scaled);
        }

        context.write(key, new Text(newRank + "\t" + adjList));
    }
}
