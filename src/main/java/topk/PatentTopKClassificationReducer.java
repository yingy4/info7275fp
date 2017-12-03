package main.java.topk;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class PatentTopKClassificationReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<Integer, Text> tmap = new TreeMap<Integer, Text>();
    private int topk = 0;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        topk = context.getConfiguration().getInt("top_k",0);
    }

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value:values) {
            String line = value.toString();
            String[] sline = line.split("\t");
            tmap.put(Integer.parseInt(sline[1]), new Text(line));
            if (tmap.size() > topk) {
                tmap.remove(tmap.firstKey());
            }
        }

        for (Text t:tmap.descendingMap().values()) {
            context.write(NullWritable.get(), t);
        }
    }

}
