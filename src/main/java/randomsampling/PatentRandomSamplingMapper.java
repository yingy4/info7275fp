package main.java.randomsampling;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class PatentRandomSamplingMapper extends Mapper<Object, Text, NullWritable, Text> {

    private float filterPercentage;
    private Random rands = new Random();

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        filterPercentage = context.getConfiguration().getFloat("filter_percentage", 0.0f);
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (rands.nextFloat() < filterPercentage) {
            context.write(NullWritable.get(), value);
        }
    }

}
