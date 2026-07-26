package pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PageRankIterationJob - Reducer
 *
 * Nhan tat ca gia tri cua 1 node (ca "S..." lan cac "C..."):
 *   - Cong don cac gia tri "C..." thanh sumContrib.
 *   - Lay lai adjList va oldRank tu gia tri "S...".
 *   - Ap dung cong thuc PageRank:
 *       newRank = (1-d)/N + d * (sumContrib + danglingSumPrev / N)
 *   - Ghi ra ban ghi moi: node &lt;TAB&gt; newRank &lt;TAB&gt; adjList.
 *   - Cong |newRank - oldRank| vao counter DIFF_SCALED (dung de kiem tra hoi tu).
 *
 * [SUA 2026-07-25 - xem Giai_thich_thuat_toan_Java_Hadoop.md muc 4]
 * Neu node nay la dangling (adjList rong), cong newRank (rank VUA TINH XONG
 * o vong nay, KHONG PHAI oldRank) vao counter NEXT_DANGLING_MASS_SCALED - day
 * chinh la danglingSum dung cho vong lap KE TIEP. Viec nay phai lam o Reducer
 * (khong phai Mapper) vi tai thoi diem Mapper chay, newRank cua vong hien tai
 * chua duoc tinh ra. Ban goc tinh gia tri nay o Mapper dua tren rank cua vong
 * TRUOC nen danglingSum luon bi "cham" mot vong so voi gia tri dung, lam tong
 * PageRank toan do thi khong con bao toan = 1 tu vong lap thu 2 tro di.
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    private double damping;
    private int numNodes;
    private double danglingSumPrev;
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) {
        Configuration conf = context.getConfiguration();
        this.damping = conf.getDouble(PageRankConstants.CONF_DAMPING, PageRankConstants.DEFAULT_DAMPING);
        this.numNodes = conf.getInt(PageRankConstants.CONF_NUM_NODES, 1);
        this.danglingSumPrev = Double.parseDouble(conf.get(PageRankConstants.CONF_DANGLING_SUM, "0.0"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double sumContrib = 0.0;
        double oldRank = 0.0;
        String adjList = "";
        boolean hasStructure = false;

        for (Text v : values) {
            String s = v.toString();
            char tag = s.charAt(0);
            String body = s.substring(1);
            if (tag == 'S') {
                int sep = body.indexOf('|');
                oldRank = Double.parseDouble(body.substring(0, sep));
                adjList = body.substring(sep + 1);
                hasStructure = true;
            } else if (tag == 'C') {
                sumContrib += Double.parseDouble(body);
            }
        }

        if (!hasStructure) {
            // Khong nen xay ra: moi node luon co dung 1 gia tri "S..." vi
            // PreprocessJob dam bao moi node co 1 ban ghi. Bo qua neu du lieu loi.
            return;
        }

        double newRank = (1.0 - damping) / numNodes
                + damping * (sumContrib + danglingSumPrev / numNodes);

        outValue.set(newRank + "\t" + adjList);
        context.write(key, outValue);

        double diff = Math.abs(newRank - oldRank);
        long diffScaled = Math.round(diff * PageRankConstants.SCALE);
        context.getCounter(PageRankConstants.COUNTER_GROUP, PageRankConstants.COUNTER_DIFF_SCALED)
                .increment(diffScaled);

        if (adjList.isEmpty()) {
            long danglingScaled = Math.round(newRank * PageRankConstants.SCALE);
            context.getCounter(PageRankConstants.COUNTER_GROUP,
                    PageRankConstants.COUNTER_NEXT_DANGLING_MASS_SCALED).increment(danglingScaled);
        }
    }
}
