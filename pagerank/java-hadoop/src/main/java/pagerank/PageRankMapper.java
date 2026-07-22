package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Doc mot ban ghi "node \t rank \t adjList" (adjList la danh sach node
 * dich cach nhau boi dau phay, rong neu la dangling node) va:
 *   - Giu nguyen cau truc do thi: phat (node, "S"+rank+"|"+adjList) de
 *     Reducer vua biet lai adjacency list, vua biet rank cu (de tinh do
 *     lech hoi tu).
 *   - Chia deu rank hien tai cho tung lien ket ra: phat
 *     (neighbor, "C"+contribution) toi moi node duoc lien ket.
 *
 * Dangling mass cho vong lap ke tiep duoc PageRankReducer tinh (dua tren
 * rank MOI vua tinh xong), khong phai o day - vi Mapper chi thay duoc rank
 * CU (dau vao cua vong hien tai), dung o day se bi lech mot vong lap.
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (line.trim().isEmpty()) return;

        String[] parts = line.split("\t", -1);
        if (parts.length < 2) return;

        String node = parts[0];
        double rank = Double.parseDouble(parts[1]);
        String adjList = parts.length > 2 ? parts[2] : "";

        // Bao toan cau truc do thi + rank hien tai (de Reducer tinh do lech)
        context.write(new Text(node), new Text("S" + rank + "|" + adjList));

        if (adjList.isEmpty()) {
            return;
        }

        String[] neighbors = adjList.split(",");
        double contribution = rank / neighbors.length;
        Text contribText = new Text("C" + contribution);
        for (String neighbor : neighbors) {
            if (neighbor.isEmpty()) continue;
            context.write(new Text(neighbor), contribText);
        }
    }
}
