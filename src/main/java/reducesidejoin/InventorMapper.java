package reducesidejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InventorMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split(",");
        if (!sline[0].startsWith("application")) {
            context.write(new Text(sline[0]), new Text("B"+line));
        }
    }
}
