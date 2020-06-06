package mapreduce.deduplication;

import mapreduce.wordcount.MyMapper;
import mapreduce.wordcount.MyReducer;
import mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用mapreduce去重
 */
public class Deduplication {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // mapreduce本地运行
        conf.set("mapreduce.app-submission.cross-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("Deduplication");

        // load data from hdfs
        Path inPath = new Path("/data/deduplication/input/");
        FileInputFormat.addInputPath(job, inPath);
        // wirte result to hdfs
        Path outPath = new Path("/data/deduplication/output/");
        // if output path exist, delete
        if (outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(DedupMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(DedupReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

    private static class DedupMapper extends Mapper<Object,Text,Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value,new Text(""));
        }
    }

    private static class DedupReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }
}
