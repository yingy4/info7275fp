package summarization;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PatentContributionByStateMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split(",");
        String state = sline[5];
        Double contibution = 1 / Double.parseDouble(sline[4]);
        if (state!=null && !state.equals("") && !sline[0].startsWith("application")) {
            context.write(new Text(state), new DoubleWritable(contibution));
        }
    }
}
