package pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Job chay 1 lan duy nhat: bien file canh tho (src&lt;TAB&gt;dst) thanh
 * ban ghi PageRank ban dau (rank = 1/N cho moi node).
 */
public final class PreprocessJob {

    private PreprocessJob() {
    }

    public static Job build(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "PageRank - Preprocess");
        job.setJarByClass(PreprocessJob.class);

        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(PreprocessReducer.class);
        job.setNumReduceTasks(PageRankConstants.NUM_REDUCERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
