package com.xhk.combineTextinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration wd_conf = new Configuration();
        Job wd_job = Job.getInstance(wd_conf);

        // 2 关联本 Driver 程序的 jar
        wd_job.setJarByClass(WordCount.class);

        // 3 关联 Mapper 和 Reducer 的 jar
        wd_job.setMapperClass(WordCountMapper.class);
        wd_job.setReducerClass(WordCountReducer.class);

        // 4 设置 Mapper 输出的 kv 类型
        wd_job.setMapOutputKeyClass(Text.class);
        wd_job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出 kv 类型
        wd_job.setOutputKeyClass(Text.class);
        wd_job.setOutputValueClass(IntWritable.class);

        // 6 设置输入格式为 CombineTextInputFormat
        wd_job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(wd_job, 4194304);

        // 7 设置输入和输出路径
        FileInputFormat.setInputPaths(wd_job, new Path("filetest/test1"));
        FileOutputFormat.setOutputPath(wd_job, new Path("filetest/test1output"));

        // 8 提交 job
        boolean result = wd_job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}