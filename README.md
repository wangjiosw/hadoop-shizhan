# 虚拟机集群配置情况
| |NN1|NN2|DN|ZK|ZKFC|JNN|RM|NM|
|----|----|----|----|----|----|----|----|---|
|node001|*| | | |*|*| |	|	
|node002| |*|*|*|*|*| |*|
|node003| | |*|*| |*|*|*|					
|node004| | |*|*| |	|*|*|

# Hadoop
## 1. HDFS Java API操作
- 查询hdfs文件内容并输出
- 创建目录
- 创建文件
- 删除文件
- 删除目录
- 遍历文件和目录

源码：[HDFS.HDFS_demo.java](src/main/java/hdfs/HDFS_demo.java)

# Mapreduce
- [WordCount](src/main/java/mapreduce/wordcount)
- [去重](src/main/java/mapreduce/deduplication/Deduplication.java)
- [求平均分](src/main/java/mapreduce/average_score/AverageScore.java)
- [二次排序](src/main/java/mapreduce/second_sort)

# HBase
- [HbaseDemo](src/main/java/hbase/HbaseDemo.java)
- [Hbase MapReduce数据转移](src/main/java/hbase/example)






