package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // mapreduce本地运行
        conf.set("mapreduce.app-submission.cross-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        // load data from hdfs
        Path inPath = new Path("/data/wordcount/input");
        FileInputFormat.addInputPath(job, inPath);
        // wirte result to hdfs
        Path outPath = new Path("/data/wordcount/output");
        // if output path exist, delete
        if (outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }
}
