package com.shishan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
* 求各个部门的总工资
* */
public class SJob {

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJarByClass(SJob.class);
        job.setMapperClass(SMapper.class);
        job.setReducerClass(SReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path path=new Path("hdfs://master:9000/outputdata");
        FileSystem fs=path.getFileSystem(conf);

        if(fs.exists(path)){
            fs.delete(path);
        }
        FileOutputFormat.setOutputPath(job,path);
        FileInputFormat.setInputPaths(job,new Path("inputdata"));

        Boolean flag=job.waitForCompletion(true);

        if (flag){
            System.out.printf("yes");
        }else {
            System.out.printf("no");
        }


    }

}
