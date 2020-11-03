package com.shishan;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SReduce extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum_salary=0;
        int salary=0;
        for(Text value:values){
            salary=Integer.parseInt(value.toString());
            sum_salary=sum_salary+salary;
            salary=0;
        }
        context.write(new Text(key),new Text(sum_salary+""));
    }
}
