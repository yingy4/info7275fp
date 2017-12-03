package partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentPartitioningByYear {

    private final static int MAX_YEAR = 2016;
    private final static int MIN_YEAR = 1980;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: PatentPatitioningByYear <in> <out>");
            System.exit(2);
        }

        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        conf.setInt("max_year", MAX_YEAR);
        conf.setInt("min_year", MIN_YEAR);

        Job job = Job.getInstance(conf, "PatentPatitioningByYear");
        job.setJarByClass(PatentPartitioningByYearMapper.class);

        job.setMapperClass(PatentPartitioningByYearMapper.class);
        job.setPartitionerClass(PatentPartitioningByYearPartitioner.class);
        PatentPartitioningByYearPartitioner.setMinYear(job, MIN_YEAR);

        job.setReducerClass(PatentPartitioningByYearReducer.class);
        job.setNumReduceTasks(MAX_YEAR - MIN_YEAR + 1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        FileSystem hdfs = FileSystem.newInstance(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
        hdfs.close();
        int code = job.waitForCompletion(true) ? 0 : 1;

        System.exit(code);
    }

}
