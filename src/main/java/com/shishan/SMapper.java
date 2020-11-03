package com.shishan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SMapper extends Mapper<LongWritable, Text,Text,Text> {

    //dept表比较小 可以直接放入内存中
    private String [] kv;
    Map<String,String> map=new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br=null;
        try{
            Path[] paths= DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String deptIdName=null;
            for (Path path:paths) {
                if(path.toString().contains("dept")){
                    br=new BufferedReader(new FileReader(path.toString()));
                    while (null != (deptIdName= br.readLine())){
                        map.put(deptIdName.split(",")[0],deptIdName.split(",")[1]);
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            br.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取当前文件名
        String filename=((FileSplit)context.getInputSplit()).getPath().getName();
        if(filename=="emp"){
            kv=value.toString().split(",");
            if(kv[5]!=null&&!"".equals(kv[5])&&kv[7]!=null&&"".equals(kv[7])){
                context.write(new Text(kv[7]),new Text(kv[5]));
            }
        }
    }
}
