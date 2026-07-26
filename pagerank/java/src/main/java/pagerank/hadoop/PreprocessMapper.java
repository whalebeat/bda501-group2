package pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * PreprocessJob - Mapper
 *
 * Input: moi dong la mot canh cua do thi, dinh dang "src<TAB>dst"
 * (dong bat dau bang '#' - vi du header cua bo du lieu SNAP nhu Wiki-Vote -
 * duoc bo qua).
 *
 * Output:
 *   (src, "E" + dst)  -> de Reducer gom thanh adjacency list cua src
 *   (dst, "N")        -> marker dam bao dst cung duoc tao ban ghi ngay ca
 *                        khi no khong bao gio xuat hien o cot nguon (node
 *                        "sink" - node dangling / chi duoc tro den).
 */
public class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outKey = new Text();
    private final Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.startsWith("#")) {
            return;
        }
        String[] parts = line.split("\\s+");
        if (parts.length < 2) {
            return; // dong khong dung dinh dang "src<TAB>dst", bo qua
        }
        String src = parts[0].trim();
        String dst = parts[1].trim();
        if (src.isEmpty() || dst.isEmpty()) {
            return;
        }

        outKey.set(src);
        outValue.set("E" + dst);
        context.write(outKey, outValue);

        outKey.set(dst);
        outValue.set("N");
        context.write(outKey, outValue);
    }
}
