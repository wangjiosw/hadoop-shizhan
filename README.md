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
## 1. wordcount
- 从hdfs的/data/wordcount/input目录读取数据 
- mapreduce本地运行wordcount程序 
- 结果输出到从hdfs的/data/wordcount/output目录

源码：[wordcount](src/main/java/wordcount/)
