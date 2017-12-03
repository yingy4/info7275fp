package main.java.topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class PatentTopKClassificationMapper extends Mapper<Object, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> tmap = new TreeMap<Integer, Text>();
    private int topk = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        topk = context.getConfiguration().getInt("top_k",0);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split("\t");
        tmap.put(Integer.parseInt(sline[1]),new Text(line));
        if (tmap.size() > topk) {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Text t:tmap.values()) {
            context.write(NullWritable.get(), t);
        }
    }

}
