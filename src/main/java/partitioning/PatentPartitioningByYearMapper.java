package main.java.partitioning;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PatentPartitioningByYearMapper extends Mapper<Object, Text, IntWritable, Text> {

    private int maxYear = 0;
    private int minYear = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        maxYear = context.getConfiguration().getInt("max_year",0);
        minYear = context.getConfiguration().getInt("min_year",0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split(",");
        String date = sline[1];
        if (date!=null && !date.equals("") && !date.startsWith("fili")) {
            int year = Integer.parseInt(date.substring(0, 4));
            if (year <= maxYear && year >= minYear) context.write(new IntWritable(year), value);
        }
    }

}
