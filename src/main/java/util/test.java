package util;

public class test {
    public static void main(String[] args) {
        String property = PropertiesUtil.getProperty("hbase.zookeeper.quorum");
        System.out.println(property);
    }
}
