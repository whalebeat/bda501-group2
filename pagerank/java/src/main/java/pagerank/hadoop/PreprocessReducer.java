package pagerank.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PreprocessJob - Reducer
 *
 * Gom cac gia tri "E..." thanh adjacency list cua node. Neu mot node khong
 * co gia tri "E..." nao thi day la dangling node ngay tu dau (out-degree = 0).
 *
 * Ghi ra: node <TAB> (1/N) <TAB> adjList (phan cach boi dau phay, rong neu
 * dangling) - dung dinh dang duoc mo ta trong Giai_thich_thuat_toan_Java_Hadoop.md
 * muc 2.
 *
 * Phan mo rong so voi tai lieu ly thuyet goc: cong don counter
 * INITIAL_DANGLING_MASS_SCALED de Driver lay duoc danglingSum khoi tao dung
 * ngay tu vong lap dau tien (tai lieu ly thuyet chi noi chung chung "phat
 * contribution", khong noi ro buoc khoi tao nay tren Hadoop that).
 */
public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {

    private double initialRank;
    private final Text outValue = new Text();

    @Override
    protected void setup(Context context) {
        int numNodes = context.getConfiguration().getInt(PageRankConstants.CONF_NUM_NODES, 1);
        this.initialRank = 1.0 / numNodes;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> adjacency = new ArrayList<>();
        for (Text v : values) {
            String s = v.toString();
            if (s.startsWith("E")) {
                adjacency.add(s.substring(1));
            }
            // "N" marker: khong can xu ly gi them, chi can dam bao Reducer
            // duoc goi cho key nay (tuc la node ton tai trong do thi).
        }

        String adjList = String.join(",", adjacency);
        outValue.set(initialRank + "\t" + adjList);
        context.write(key, outValue);

        if (adjacency.isEmpty()) {
            long scaled = Math.round(initialRank * PageRankConstants.SCALE);
            context.getCounter(PageRankConstants.COUNTER_GROUP,
                    PageRankConstants.COUNTER_INITIAL_DANGLING_MASS_SCALED).increment(scaled);
        }
    }
}
