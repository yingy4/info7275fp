package main.java.counting;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PatentCountingByDateMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split(",");
        String date = sline[1];
        if (date!=null && !date.equals("") && !date.startsWith("fili")) {
            context.write(new Text(date), one);
        }
    }

}
