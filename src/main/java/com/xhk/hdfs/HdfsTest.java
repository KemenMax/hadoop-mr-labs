package com.xhk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.eclipse.jetty.util.IO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsTest {
    private FileSystem fs;
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException{
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "hadoop");
    }

    @After
    public void close() throws IOException{
        //3. 关闭资源
        fs.close();
    }

    @Test
    public void hdfsStart() throws Exception {
        // 2. 创建目录
        fs.mkdirs(new Path("/codingHDFS/test0/"));
        fs.mkdirs(new Path("/codingHDFS/test1/"));
    }

    @Test
    public void testPut() throws IOException{
        //参数一:表示删除原数据:
        //参数二:是否允许覆盖:
        //参数三:原数据路径;
        //参数四:目的地路径
        fs.copyFromLocalFile(false, true,
                new Path("filetest/test0/hdfstestfile01.txt"),
                new Path("hdfs://hadoop01:9000//codingHDFS//test1")
                );
    }

    @Test
    public void testGet() throws IOException{
        //参数一:原文件是否删除;
        //参数二:原文件路径HDFS;
        // 参数三:目标地址路径Win;
        //参数四:是否查看本地校验(crc校验码)
        fs.copyToLocalFile(false,
                new Path("hdfs://hadoop01:9000//codingHDFS//test0"),
                new Path("filetest"),
                true);
    }

    @Test
    public void testDel() throws IOException{
        //删除文件
        fs.delete(new Path("hdfs://hadoop01:9000//codingHDFS//test0"), false);
        //删除空目录
        //fs.delete(new Path("hdfs://hadoop01:9000//codingHDFS//test1"，false);
        //删除非空目录
        //fs.delete(new Path("hdfs://hadoop01:9000//codingHDFS//output0320, true")
    }

    @Test
    public void testMv() throws IOException{
        //修改文件名称
        fs.rename(new Path("hdfs://hadoop01:9000/codingHDFS/test0/hdfstestfile01.txt"),
                new Path("hdfs://hadoop01:9000/codingHDFS/test0/hdfstestfile02.txt"));

        //移动文件并改名
        fs.rename(new Path("hdfs://hadoop01:9000/codingHDFS/test0/hdfstestfile02.txt"),
                new Path("hdfs://hadoop01:9000/codingHDFS/test1/test03.txt"));

        //文件夹的更名
        fs.rename(new Path("hdfs://hadoop01:9000/codingHDFS/test1"),
                new Path("hdfs://hadoop01:9000/codingHDFS/test2"));
    }

    @Test
    public void testFileInfo() throws IOException {

        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "========");
            System.out.println("文件权限: "+fileStatus.getPermission());
            System.out.println("文件作者: "+fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }


    @Test
    public void testJudgeFIle() throws IOException{
        //2. 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus){
            //如果是文件
            if (fileStatus.isFile()){
                System.out.println("f: "+fileStatus.getPath().getName());
            }else{
                System.out.println("d: "+fileStatus.getPath().getName());
            }
        }
    }


}