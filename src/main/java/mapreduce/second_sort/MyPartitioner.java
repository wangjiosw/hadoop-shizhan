package mapreduce.second_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区类
 * 依据第一个字段分区
 */
public class MyPartitioner extends Partitioner<MyKeyPair, IntWritable> {
    public int getPartition(MyKeyPair myKeyPair, IntWritable intWritable, int i) {
        return (myKeyPair.getFirst().hashCode() & Integer.MAX_VALUE) % i;
    }
}
