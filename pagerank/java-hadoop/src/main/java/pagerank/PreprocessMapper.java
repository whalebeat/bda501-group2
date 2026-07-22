package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Doc file canh dang "src\tdst" (moi dong mot canh huong src -> dst).
 * Voi moi canh, phat ra:
 *   (src, "E"+dst)  - de Reducer gom thanh adjacency list cua src
 *   (dst, "N")      - marker de dam bao dst cung duoc tao ban ghi node,
 *                      ke ca khi dst khong bao gio xuat hien o cot nguon
 *                      (vi du: node la "sink", khong co canh ra).
 */
public class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Text NODE_MARKER = new Text("N");

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split("\t");
        if (parts.length < 2) return;
        String src = parts[0].trim();
        String dst = parts[1].trim();
        if (src.isEmpty() || dst.isEmpty()) return;

        context.write(new Text(src), new Text("E" + dst));
        context.write(new Text(dst), NODE_MARKER);
    }
}
