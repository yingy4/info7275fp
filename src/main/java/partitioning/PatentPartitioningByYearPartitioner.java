package partitioning;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class PatentPartitioningByYearPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

    private static final String MIN_YEAR = "min.year";

    private Configuration conf = null;
    private int minYear = 0;

    public void setConf(Configuration conf) {
        this.conf = conf;
        minYear = conf.getInt(MIN_YEAR, 0);
    }

    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return key.get() - minYear;
    }

    public static void setMinYear(Job job, int minYear) {
        job.getConfiguration().setInt(MIN_YEAR,minYear);
    }


}
