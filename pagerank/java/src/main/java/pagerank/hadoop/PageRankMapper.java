package pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PageRankIterationJob - Mapper
 *
 * Input: moi dong la 1 ban ghi node cua vong lap truoc,
 *        dinh dang "node&lt;TAB&gt;rank&lt;TAB&gt;adjList"
 *
 * Phat ra 2 loai gia tri cho moi node (dung 2 viec ma tai lieu ly thuyet mo ta):
 *   1) Giu nguyen cau truc do thi: (node, "S" + rank + "|" + adjList)
 *   2) Chia rank hien tai cho out-degree va phat dong gop cho tung neighbor:
 *      (neighbor, "C" + (rank / outDegree))
 *
 * Neu node la dangling (adjList rong), phan rank cua no khong the phat cho
 * neighbor nao nen Mapper khong lam gi them voi no - phan xu ly dangling mass
 * duoc chuyen sang Reducer (xem PageRankReducer va
 * Giai_thich_thuat_toan_Java_Hadoop.md muc 4 ve bug da phat hien va sua:
 * "dangling sum bi lech 1 vong lap").
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.trim().isEmpty()) {
            return;
        }
        String[] parts = line.split("\t", -1);
        if (parts.length < 2) {
            return; // dong khong hop le, bo qua
        }
        String node = parts[0];
        double rank = Double.parseDouble(parts[1]);
        String adjList = parts.length >= 3 ? parts[2] : "";

        // 1) Giu nguyen cau truc do thi
        outKey.set(node);
        outValue.set("S" + rank + "|" + adjList);
        context.write(outKey, outValue);

        // 2) Phat dong gop cho tung neighbor (neu khong phai dangling)
        if (!adjList.isEmpty()) {
            String[] neighbors = adjList.split(",");
            double contrib = rank / neighbors.length;
            String contribStr = "C" + contrib;
            for (String n : neighbors) {
                if (n.isEmpty()) {
                    continue;
                }
                outKey.set(n);
                outValue.set(contribStr);
                context.write(outKey, outValue);
            }
        }
        // dangling node: khong phat gia tri "C" nao - dung nhu tai lieu ly thuyet mo ta
    }
}
