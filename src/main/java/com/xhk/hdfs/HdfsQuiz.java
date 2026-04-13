package com.xhk.hdfs;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Vector;

public class HdfsQuiz {
    private FileSystem fs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        // 建立 HDFS 连接，后续测试统一复用这个文件系统对象
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), configuration, "hadoop");
    }

    @After
    public void close() throws IOException {
        // 测试结束后及时关闭 HDFS 连接
        fs.close();
    }

    @Test
    public void testUploadJdkToHdfs() throws Exception {
        // 创建 SSH 会话，通过 SFTP 读取远程目录内容
        JSch jsch = new JSch();
        Session sshSession = jsch.getSession("hadoop", "hadoop01", 22);
        sshSession.setPassword("123456");
        // 测试环境直接用密码登录，不依赖本机预先维护 known_hosts
        sshSession.setConfig("StrictHostKeyChecking", "no");
        sshSession.connect();

        // 打开 SFTP 通道，遍历 /opt/module 下的所有目录和文件
        ChannelSftp sftpChannel = (ChannelSftp) sshSession.openChannel("sftp");
        sftpChannel.connect();

        try {
            // 这是一次全量导入测试，先清理旧的 /jdk，避免上次失败留下的脏数据影响本次目录结构
            Path jdkRootPath = new Path("/jdk");
            if (fs.exists(jdkRootPath)) {
                fs.delete(jdkRootPath, true);
            }

            // 用两个栈维护远程目录和 HDFS 目录的对应关系
            ArrayDeque<String> remoteDirStack = new ArrayDeque<>();
            ArrayDeque<Path> hdfsDirStack = new ArrayDeque<>();
            remoteDirStack.push("/opt/module/");
            hdfsDirStack.push(jdkRootPath);

            while (!remoteDirStack.isEmpty()) {
                String remoteDir = remoteDirStack.pop();
                Path hdfsDir = hdfsDirStack.pop();

                // 先创建当前 HDFS 目录，再继续处理远程子项
                fs.mkdirs(hdfsDir);

                // 读取当前远程目录下的所有条目
                @SuppressWarnings("unchecked")
                Vector<ChannelSftp.LsEntry> entries = sftpChannel.ls(remoteDir);
                for (ChannelSftp.LsEntry entry : entries) {
                    String fileName = entry.getFilename();
                    // 跳过当前目录和上级目录两个占位项
                    if (".".equals(fileName) || "..".equals(fileName)) {
                        continue;
                    }

                    String remoteChildPath = remoteDir + "/" + fileName;
                    Path hdfsChildPath = new Path(hdfsDir, fileName);

                    // ls 返回的是链接本身属性，软链接需要先解析到真实目标类型
                    SftpATTRS fileAttrs = entry.getAttrs();
                    if (fileAttrs.isLink()) {
                        fileAttrs = sftpChannel.stat(remoteChildPath);
                    }

                    // 目录继续入栈，稍后再遍历其子项
                    if (fileAttrs.isDir()) {
                        remoteDirStack.push(remoteChildPath);
                        hdfsDirStack.push(hdfsChildPath);
                        continue;
                    }

                    // 普通文件直接从远程流复制到 HDFS
                    InputStream inputStream = sftpChannel.get(remoteChildPath);
                    FSDataOutputStream outputStream = fs.create(hdfsChildPath, true);
                    IOUtils.copyBytes(inputStream, outputStream, 4096, true);
                }
            }
        } finally {
            // 无论成功还是失败，都及时释放 SFTP 和 SSH 连接
            sftpChannel.disconnect();
            sshSession.disconnect();
        }
    }

    @Test
    public void testCountJdkEntries() throws IOException {
        // 统计 /jdk 下目录数量和文件数量
        int directoryCount = 0;
        int fileCount = 0;

        // 用栈遍历 HDFS 目录树，避免递归
        ArrayDeque<Path> pathStack = new ArrayDeque<>();
        pathStack.push(new Path("/jdk"));

        while (!pathStack.isEmpty()) {
            Path currentPath = pathStack.pop();

            // 读取当前目录下的直接子项
            FileStatus[] fileStatuses = fs.listStatus(currentPath);
            for (FileStatus fileStatus : fileStatuses) {
                // 子目录继续入栈，并累计目录数量
                if (fileStatus.isDirectory()) {
                    directoryCount++;
                    pathStack.push(fileStatus.getPath());
                    continue;
                }

                // 普通文件直接累计数量
                if (fileStatus.isFile()) {
                    fileCount++;
                }
            }
        }

        System.out.println("文件夹数量(不含根目录): " + directoryCount);
        System.out.println("文件数量: " + fileCount);
    }

    @Test
    public void testDownloadLargeFiles() throws IOException {
        // 先准备本地目录，用于接收从 HDFS 下载的大文件
        Files.createDirectories(new File("filetest/jdkBigFile").toPath());

        // 递归遍历 /jdk 下所有文件，只下载大于 1MB 的文件
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path("/jdk"), true);
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            if (fileStatus.getLen() <= 1024 * 1024L) {
                continue;
            }

            // 按 HDFS 相对路径还原本地目录结构
            String root = new Path("/jdk").toUri().getPath();
            String full = fileStatus.getPath().toUri().getPath();
            String relativePath = full.substring(root.length());
            if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
            }

            File targetFile = new File("filetest/jdkBigFile", relativePath.replace("/", File.separator));
            Files.createDirectories(targetFile.getParentFile().toPath());

            // 直接把 HDFS 文件复制到本地目标路径
            fs.copyToLocalFile(false, fileStatus.getPath(), new Path(targetFile.toURI()), true);
        }
    }

    @Test
    public void testCopySmallFilesToHdfs() throws IOException {
        // 创建 HDFS 目标目录，用于存放小文件
        fs.mkdirs(new Path("/jdkSmallfile"));

        // 递归遍历 /jdk 下所有文件，只复制小于 1MB 的文件
        RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(new Path("/jdk"), true);
        while (fileIterator.hasNext()) {
            LocatedFileStatus fileStatus = fileIterator.next();
            if (fileStatus.getLen() >= 1024 * 1024L) {
                continue;
            }

            // 按原始相对路径在 HDFS 中还原目录结构
            String root = new Path("/jdk").toUri().getPath();
            String full = fileStatus.getPath().toUri().getPath();
            String relativePath = full.substring(root.length());
            if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
            }

            Path targetPath = new Path("/jdkSmallfile", relativePath);
            fs.mkdirs(targetPath.getParent());

            // 把小文件逐个复制到新的 HDFS 目录
            InputStream inputStream = fs.open(fileStatus.getPath());
            FSDataOutputStream outputStream = fs.create(targetPath, true);
            IOUtils.copyBytes(inputStream, outputStream, 4096, true);
        }
    }
}
