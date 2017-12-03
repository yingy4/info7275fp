package partitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PatentPartitioningByYearReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text t : values) {
            context.write(t, NullWritable.get());
        }
    }

}
