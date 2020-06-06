package mapreduce.average_score;

import mapreduce.wordcount.MyMapper;
import mapreduce.wordcount.MyReducer;
import mapreduce.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class AverageScore {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // mapreduce本地运行
        conf.set("mapreduce.app-submission.cross-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        Path inPath = new Path("./data/average/input");
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path("./data/average/output");
        // if output path exist, delete
        if (outPath.getFileSystem(conf).exists(outPath))
            outPath.getFileSystem(conf).delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(ScoreMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(ScoreReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }

    private static class ScoreMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        Text name = new Text();
        IntWritable score = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] s = value.toString().split(" ");
            name.set(s[0]);
            score.set(Integer.parseInt(s[1]));
            context.write(name,score);
        }
    }

    private static class ScoreReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
        FloatWritable averageScore = new FloatWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            averageScore.set((float) (sum*1.0/count));
            context.write(key, averageScore);
        }
    }
}
