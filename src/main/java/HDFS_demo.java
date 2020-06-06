import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * HDFS Java API操作
 */
public class HDFS_demo {

    Configuration conf = null;
    FileSystem fs = null;

    @Before
    public void conn() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    /**
     * 查询hdfs文件内容并输出
     */
    @Test
    public void catFile() throws IOException {
        FSDataInputStream in = fs.open(new Path("hdfs:/wordcount/input/word.txt"));
        IOUtils.copyBytes(in,System.out,4096,false);
        IOUtils.closeStream(in);
    }


    /**
     * 创建目录
     */
    @Test
    public void createDir() throws IOException {
        boolean isok = fs.mkdirs(new Path("hdfs:/mydir"));
        if (isok){
            System.out.println("创建目录成功！");
        } else {
            System.out.println("创建目录失败！");
        }
    }

    /**
     * 创建文件
     */
    @Test
    public void createFile() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("hdfs:/mydir/newfile.txt"));
        fsDataOutputStream.write("我是文件内容\n".getBytes());
        fsDataOutputStream.close();
        System.out.println("文件创建成功！");
    }

    /**
     * 删除文件
     */
    @Test
    public void deleteFile() throws IOException {
        boolean isok = fs.deleteOnExit(new Path("hdfs:/mydir/newfile.txt"));
        if (isok){
            System.out.println("删除成功！");
        } else {
            System.out.println("删除失败！");
        }
    }
    /**
     * 删除目录
     */
    @Test
    public void deleteDir() throws IOException {
        boolean isok = fs.delete(new Path("hdfs:/mydir"),true);
        if (isok){
            System.out.println("删除成功！");
        } else {
            System.out.println("删除失败！");
        }
    }

    /**
     * 遍历文件和目录
     */
    @Test
    public void listStatus() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("hdfs:/"));
        if (fileStatuses.length > 0){
            for (FileStatus f: fileStatuses) {
                showDir(f);
            }
        }

    }


    private void showDir(FileStatus f) throws IOException {
        Path path = f.getPath();
        System.out.println(path);
        if (f.isDirectory()){
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses.length > 0){
                for (FileStatus file: fileStatuses) {
                    showDir(file);
                }
            }

        }
    }

    @After
    public void close() throws IOException {
        fs.close();
    }



}
