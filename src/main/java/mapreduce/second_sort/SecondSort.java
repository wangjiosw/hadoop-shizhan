package mapreduce.second_sort;

import mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 输入：
 * A 3
 * B 5
 * C 1
 * B 6
 * A 4
 * C 5
 * 需求：按第一字段升序排序，若第一字段相同，则按第二字段降序排序
 */
public class SecondSort {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // mapreduce本地运行
        conf.set("mapreduce.app-submission.cross-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        Path inPath = new Path("./data/sort/input");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("./data/sort/output");
        // if output path exist, delete
        if (outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(SortMapper.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);

        job.setOutputKeyClass(MyKeyPair.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(SortReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

    private static class SortMapper extends Mapper<LongWritable,Text,MyKeyPair,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            String first = split[0];
            int second = Integer.parseInt(split[1]);
            MyKeyPair outKey = new MyKeyPair();
            outKey.setFirst(first);
            outKey.setSecond(second);
            IntWritable outValue = new IntWritable(second);
            context.write(outKey,outValue);
        }
    }

    private static class SortReducer extends Reducer<MyKeyPair,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(MyKeyPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Text outKey = new Text();
            for ( IntWritable val: values) {
                outKey.set(key.getFirst());
                context.write(outKey,val);
            }
        }
    }
}
