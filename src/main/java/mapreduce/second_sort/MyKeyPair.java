package mapreduce.second_sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义组合key类
 */
public class MyKeyPair implements WritableComparable<MyKeyPair> {
    private String first;
    private int second;

    // 比较器
    public int compareTo(MyKeyPair o) {
        // 默认升序排列
        int res = this.first.compareTo(o.first);
        if (res!=0){
            return res;
        }else {
            // 若第一字段相同，则按第二字段降序排序
            return -Integer.valueOf(this.second).compareTo(Integer.valueOf(o.second));
        }
    }

    // 序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.first);
        dataOutput.writeInt(this.second);
    }

    // 反序列化
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }

    public String getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
