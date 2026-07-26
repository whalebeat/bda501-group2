package pagerank.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Mot vong lap PageRank: Input -&gt; Mapper -&gt; Shuffle&amp;Sort -&gt; Reducer -&gt; Output.
 * Driver goi lai job nay nhieu lan, output vong truoc lam input vong sau.
 */
public final class PageRankIterationJob {

    private PageRankIterationJob() {
    }

    public static Job build(Configuration conf, Path input, Path output) throws IOException {
        Job job = Job.getInstance(conf, "PageRank - Iteration");
        job.setJarByClass(PageRankIterationJob.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
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
