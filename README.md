## 什么是BOS HDFS工具

BOS HDFS 工具是百度智能云基于 Hadoop 框架推出的专门用于解决大数据场景下BOS中数据的读写和使用问题的便捷工具。

大数据场景下的数据分析已经成为企业关注的核心业务。Hadoop 在分布式数据处理方面具有出色的能力，凭借其可靠、高效、可伸缩、并发处理的特点，已发展为当今最为主流的大数据开源框架之一。Hadoop 实现了一个分布式文件系统（Hadoop Distributed File System），简称为 HDFS。HDFS 具有高容错性的特点，并通过高吞吐量来支持应用程序的数据访问，适合超大数据集的业务场景。HDFS 为海量数据提供了可靠的存储性能，已成为 Hadoop 生态中最重要的一部分。数据量的与日俱增使原生的 Hadoop 面临一些新的问题，HDFS自建及运维成本非常高，同时本地 HDFS 上海量数据如何存放也是企业面临的巨大挑战。因此，在企业数据上云的趋势下，越来越多的企业选择将数据存储在云端，即存储在对象存储服务当中。但由于对象存储上层数据接口的限制，对象存储中数据和自建 HDFS 之间的访问和读写操作一直是大数据场景下的一个瓶颈，BOS HDFS 很好地解决了这个问题。

BOS HDFS 工具全面兼容 Hadoop 2.7+/3.1+ 系列，支持 HDFS 数据在 BOS 中的海量存储，并在上层数据运算中使用 HDFS 标准接口来对数据进行访问和读写，有效解决自建 HDFS 数据的高运维成本和低可扩展性问题。您可以通过调用该工具充分享受到 BOS 带来的超低价格、超高性能、高可靠和高吞吐的强大优势，满足企业在大数据场景中对数据的读写和使用需求。

## BOS HDFS工具的优势

- 框架兼容：全面兼容 Hadoop 2.7+/3.1+
- 无感调用：实现对 BOS 中数据的无感调用
- 数据存储性价比高：融合对象存储服务 BOS 的超低价格、超高性能、高可靠性、高可用性和高吞吐优势

您只需下载相应的SDK包，修改部分配置即可使用BOS HDFS工具。


## 下载

- 下载 [BOS FS JAR](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.3-community.jar.zip)，将解压后的jar包复制到 `$hadoop_dir/share/hadoop/common`中。

## 使用前准备

- 在Hadoop配置路径中修改 `log4j.properties`，调整BOS SDK的日志配置：`log4j.logger.com.baidubce.http=WARN`
- 在`$hadoop_dir/etc/core-site.xml`文件中添加或者修改BOS HDFS相关配置。

```xml
<property>
  <name>fs.bos.access.key</name>
  <value>{Your AK}</value>
</property>

<property>
  <name>fs.bos.secret.access.key</name>
  <value>{Your SK}</value>
</property>

<property>
  <name>fs.bos.endpoint</name>
  <value>http://bj.bcebos.com</value>
</property>

<property>
  <name>fs.bos.impl</name>
  <value>org.apache.hadoop.fs.bos.BaiduBosFileSystem</value>
</property>

<property>
  <name>fs.bos.bucket.hierarchy</name>
  <value>false</value>
</property>

```

可配置的属性：

|  名称   | 描述  |
|  ----  | ----  |
| fs.bos.access.key  | 必须，BOS的访问accessKey。 |
| fs.bos.secret.access.key  | 必须，BOS的访问secretKey。 |
| fs.bos.endpoint | 必须，BOS存储桶所在的endpoint。 |
| fs.bos.session.token.key| 可选，临时访问STS方式，如果配置，fs.bos.access.key和fs.bos.secret.access.key也应该是临时访问密钥。 |
| fs.bos.block.size |可选，默认128MB， 模拟BlockSize，在Distcp等需要Block信息的场景需要使用 |
| fs.bos.max.connections | 可选，客户端支持的最大连接数，默认1000。 |
| fs.bos.multipart.uploads.block.size | 可选，客户端使用分块上传时候的分块大小，默认12582912，单位：byte。 |
| fs.bos.multipart.uploads.concurrent.size | 可选，客户端使用分块上传时候并发数，默认10。 |
| fs.bos.object.dir.showtime| 可选，list时候显示目录的修改时间，会额外增加一些交互次数，默认false。 |
| fs.bos.tail.cache.enable | 可选，缓存文件尾部部分数据，可以提升parquet/orc等文件的解析性能，默认true。 |
| fs.bos.crc32c.checksum.enable |可选，开启crc32c checksum校验，开启后上传的文件才会计算CRC2C。 |
| fs.bos.bucket.hierarchy | 可选，bucket是否是层级namespace的，一般是false，正确设置后节省一次校验开销。 |

## 开始使用

使用BOS HDFS访问BOS服务时路径需要以`bos://`开始。如：

```bash
$ hdfs dfs -ls bos://{bucket}/
$ hdfs dfs -put ${local_file} bos://{bucket}/a/b/c
```

或者，在`$hadoop_dir/etc/core-site.xml`下配置默认文件目录

```xml
<property>
  <name>fs.defaultFS</name>
  <value>bos://{bucket}</value>
</property>
```

>注意，在配置fs.defaultFS为BosFileSystem后，如果启动NameNode和DataNode可能会触发scheme检查失败。
>
>建议在仅使用BosFileSystem时配置fs.defaultFS，且无需启动NameNode和DataNode；否则配置HDFS默认地址。

就像使用原生hdfs那样：

```bash
$ hdfs dfs -ls /
```

## 一次wordcount的实践

**1. 创建数据目录**

用于保存MapReduce任务的输入文件

```bash
$ hdfs dfs -mkdir -p bos://test-bucket/data/wordcount
```

用于保存MapReduce任务的输出文件

```bash
$ hdfs dfs -mkdir bos://test-bucket/output
```

查看这两个新创建的目录

```bash
$ hdfs dfs -ls bos://test-bucket/
```

```bash
Found 2 items
drwxrwxrwx   -          0 1970-01-01 08:00 bos://test-bucket/data
drwxrwxrwx   -          0 1970-01-01 08:00 bos://test-bucket/output
```

如果想要显示文件夹的准确创建时间，可以在`$hadoop_dir/etc/core-site.xml`下配置

```xml
<property>
  <name>fs.bos.object.dir.showtime</name>
  <value>true</value>
</property>
```

**2. 写一个单词文件，并上传到hdfs**

单词文件的内容

```bash
$ cat words.txt
```

```bash
hello baidu
hello bos
hadoop hdfs
hello hadoop
bos hdfs
```

上传words.txt到hdfs

```bash
$ hdfs dfs -put words.txt bos://test-bucket/data/wordcount
```

在hdfs中查看刚刚上传的文件

```bash
$ hdfs dfs -cat bos://test-bucket/data/wordcount/words.txt
```

```bash
hello baidu
hello bos
hadoop hdfs
hello hadoop
bos hdfs
```

查看文件checksum
```bash
$ hdfs dfs -checksum bos://test-bucket/data/wordcount/words.txt
```

**3. 运行wordcount程序**

hadoop自带的wordcount程序在`$hadoop_dir/share/hadoop/mapreduce/`下

```bash
$ hadoop jar hadoop-mapreduce-examples-2.7.7.jar wordcount bos://test-bucket/data/wordcount bos://test-bucket/output/wordcount
```

**4. 查看统计结果**

```bash
$ hdfs dfs -ls bos://test-bucket/output/wordcount/
-rw-rw-rw-   1          0 2020-06-12 16:55 bos://test-bucket/output/wordcount/_SUCCESS
-rw-rw-rw-   1         61 2020-06-12 16:55 bos://test-bucket/output/wordcount/part-r-00000
```

```bash
$ hdfs dfs -cat bos://test-bucket/output/wordcount/part-r-00000
baidu	1
bos	    2
hadoop	2
hdfs	2
hello	3
```

## hadoop distcp使用
DistCp（分布式拷贝）是hadoop自带的用于大规模集群内部和集群之间拷贝的工具。 它使用Map/Reduce实现文件分发，错误处理和恢复，以及报告生成。 它把文件和目录的列表作为map任务的输入，每个任务会完成源列表中部分文件的拷贝。

对于HDFS集群和BOS间的数据拷贝，可以借助BOS HDFS 工具，像标准的Hadoop Distcp那样。

### 前置工作

如上，下载BOS HDFS相关jar包，并做一些必要的配置。

以数据源端是HDFS集群，目的端是BOS为例，检查源和目的端的读写正常。

```bash
$ hadoop fs -get hdfs://host:port/xxx ./
$ hadoop fs -put xxx bos://bucket/xxx
```

### 开始拷贝

**普通拷贝**
```bash
# 从hdfs的src，拷贝到bos指定bucket下的dst路径，默认情况会跳过已经存在的目标文件
$ hadoop distcp hdfs://host:port/src bos://bucket/dst
```
注意：使用CRC校验拷贝前后的数据，BOS HDFS需设置fs.bos.block.size和源HDFS一致，并开启fs.bos.crc32c.checksum.enable；仅支持HDFS的dfs.checksum.combine.mode=COMPOSITE_CRC校验算法。

**更新和覆盖**
```bash
# 拷贝，执行覆盖的唯一标准是源文件和目标文件大小是否相同，如果不同，则源文件替换目标文件
$ hadoop distcp -update hdfs://host:port/src bos://bucket/dst

# 拷贝，并覆盖已经存在的目标文件
$ hadoop distcp -overwrite hdfs://host:port/src bos://bucket/dst
```
注意：BOS HDFS尚不支持append写入，所以目的端是BOS时不支持append更新拷贝。

**拷贝多个源**
```bash
# 指定多个源路径
$ hadoop distcp hdfs://host:port/src1 hdfs://host:port/src2 bos://bucket/dst

# 从文件里获得多个源
# srcList的内容是类似hdfs://host:port/src1、hdfs://host:port/src2的多行源路径
$ hadoop distcp hdfs://host:port/srcList bos://bucket/dst
```
注意：多个源的拷贝可能会有冲突，参考标准hadoop distcp的处理。

**更多配置**
```bash
# 指定拷贝数据时map的数目
# 更多的map数量可能不会提升数据吞吐，反而会带来一些问题，map数量应该根据集群资源和拷贝数据规模综合设定
$ hadoop distcp -m 10 hdfs://host:port/src bos://bucket/dst

# 忽略失败的map，但会保留失败操作日志
$ hadoop distcp -i hdfs://host:port/src bos://bucket/dst

# 动态分配任务
# 默认的分配策略是基于文件大小，在更新拷贝时候，因为有跳过文件，实际拷贝时候有类似"数据倾斜"的问题，耗时最长的map拖慢整体进度
$ hadoop distcp -strategy dynamic -update hdfs://host:port/src bos://bucket/dst
```
更多配置参数，输入hadoop distcp命令即可获取帮助信息。

## 使用进阶

由于自建Hadoop集群的可拓展性有限，且需要大量人力对集群进行运维，如果您对性能及安全性有更高要求，推荐使用百度智能云提供的 [百度 MapReduce（BMR）](https://cloud.baidu.com/product/bmr.html)。BMR 是全托管的 Hadoop/Spark 集群，您可以按需部署并弹性扩展集群，只需专注于大数据处理、分析、报告，拥有多年大规模分布式计算技术积累的百度运维团队全权负责集群运维，能够在性能，安全性和便捷性上有大幅提升。


## 变更记录
【1.0.4】
- 支持CRC32C Checksum校验
- 优化create接口
- 优化open接口
- 优化层级rename接口
- 默认分块调整10MB->12MB
- 顺序读range预读倍数调整2->4

【1.0.3】
- 支持层级bos，delete非空目录，rename目录
- 支持tail cache，默认开启
- 动态调整预读范围
- 使用批量删除objects
- bugfix: 任务结束资源未释放；delete时oom；404日志冗余