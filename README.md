# Hadoop MR Labs

一个基于 Maven 的 Hadoop Java 练习项目，主要用于学习和演示 HDFS 常见操作。

## 项目说明

- [HdfsQuiz.java](/c:/Users/Xiong/IdeaProjects/HadoopMR/src/main/java/com/xhk/hdfs/HdfsQuiz.java) 是小测代码，包含目录上传、文件统计、大文件下载和小文件分类复制等练习。
- [HdfsTest.java](/c:/Users/Xiong/IdeaProjects/HadoopMR/src/main/java/com/xhk/hdfs/HdfsTest.java) 是上课老师示例代码，演示 HDFS 的基础操作，如创建目录、上传下载、删除、重命名、查看文件信息等。

## 目录结构

```text
HadoopMR/
├─ src/
│  └─ main/
│     ├─ java/
│     │  ├─ com/xhk/hdfs/
│     │  │  ├─ HdfsQuiz.java
│     │  │  └─ HdfsTest.java
│     │  └─ org/example/
│     │     └─ Main.java
│     └─ resources/
│        └─ log4j.properties
├─ .mvn/
├─ pom.xml
└─ .gitignore
```

## 主要内容

### HdfsQuiz

小测代码，主要包含以下练习：

- 通过 SFTP 读取远程目录并上传到 HDFS
- 统计 `/jdk` 目录下的文件数和目录数
- 下载 `/jdk` 下大于 1MB 的文件到本地
- 将 `/jdk` 下小于 1MB 的文件复制到新的 HDFS 目录

#### 相对路径计算示例

在下载大文件或复制小文件时，代码并不是把文件“平铺”到目标目录下，而是先计算文件相对于根目录 `/jdk` 的路径，再在目标位置按这个相对路径还原原有目录结构。

假设某个文件在 HDFS 中的路径为：

- 根目录：`/jdk`
- 完整路径：`/jdk/bin/java`

那么代码中的变量值依次为：

- `root = "/jdk"`
- `full = "/jdk/bin/java"`
- `relativePath = "/bin/java"`
- `去掉开头 / 后变成：bin/java`

### HdfsTest

课堂示例代码，主要演示以下 HDFS 基础操作：

- 创建目录
- 上传本地文件到 HDFS
- 从 HDFS 下载文件到本地
- 删除文件或目录
- 重命名和移动文件
- 查看文件详情和块信息
- 判断路径是文件还是目录

## 运行说明

本项目默认连接的 HDFS 地址是 `hdfs://hadoop01:9000`，运行相关测试前需要确保：

- Hadoop 集群已经启动
- 当前环境可以访问 `hadoop01`
- 本地已具备 Maven 和 JDK 环境

