package mapreduce.second_sort;

import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {
    public MyGroupComparator() {
        super(MyKeyPair.class,true);
    }

    public int compare(MyKeyPair a, MyKeyPair b) {
        // 生成key-valueList时按第一个字段来
        return a.getFirst().compareTo(b.getFirst());
    }
}
