package hbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;

/**
 * 使用mapreduce将hbase中student表的一部分数据移动到student_new表
 * student表：rowkey,info:name,info:age,info:address
 * student_new表：rowkey,info:name,info:age
 */
public class StudentMRRunner {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node002:2181,node003:2181,node004:2181");

        // mapreduce本地运行
        conf.set("mapreduce.app-submission.cross-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);
        job.setJarByClass(StudentMRRunner.class);
        job.setJobName("StudentMRRunner");

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        TableMapReduceUtil.initTableMapperJob("student",
                scan,
                ReadStudentMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);
        TableMapReduceUtil.initTableReducerJob("student_new",
                WriteStudentReducer.class,job);

        job.setNumReduceTasks(1);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);

    }
}
