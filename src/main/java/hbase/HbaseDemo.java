package hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseDemo {
    private Admin admin = null;
    Connection connection = null;
    Table table = null;

    String tbName = "t1";
    String rowKey = "201426802020";

    @Before
    public void connect() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node002:2181,node003:2181,node004:2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        table = connection.getTable(TableName.valueOf(tbName));
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        TableName tableName = TableName.valueOf(tbName);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor("f1");
        desc.addFamily(family);
        admin.createTable(desc);
        System.out.println("create table success!!");
    }

    /**
     * 添加数据
     * @throws IOException
     */
    @Test
    public void putData() throws IOException {
        Put put = new Put(rowKey.getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),"wanger".getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(),"20".getBytes());
        put.addColumn("f1".getBytes(),"location".getBytes(),"hangzhou".getBytes());

        table.put(put);
        System.out.println("put data success!!");

    }

    /**
     * 查询数据
     * @throws IOException
     */
    @Test
    public void getData() throws IOException {
        Table table = connection.getTable(TableName.valueOf("t1"));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        for (Cell cell: result.rawCells()) {
            String family = new String(CellUtil.cloneFamily(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            System.out.println("family:"+family+"\tqualifier:"+qualifier+"\tvalue:"+value);
        }
    }

    /**
     * scan
     * @throws IOException
     */
    @Test
    public void scanTable() throws IOException {
        Scan scan = new Scan();
        // 设置过滤器
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(rowKey.getBytes()));
        // FamilyFilter...
        // QualifierFilter...
        // SingleColumnValueFilter...
        // 挺多过滤器可以设置的...
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            for (Cell cell: res.rawCells()) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println("family:"+family+"\tqualifier:"+qualifier+"\tvalue:"+value);
            }
        }
    }

    /**
     * 删除数据
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
        System.out.println("delete data success!!");
    }




    @After
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
        if (table != null){
            table.close();
        }
    }

}
