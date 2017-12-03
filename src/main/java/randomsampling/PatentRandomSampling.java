package randomsampling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentRandomSampling {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PatentRandomSampling <in> <out> <percentage>");
            System.exit(2);
        }

        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        float percentage = Float.parseFloat(args[2]);
        System.exit(run(input, outputDir, percentage));
    }

    public static int run(Path input, Path outputDir, float percentage) throws Exception {
        Configuration conf = new Configuration();

        Float filterPercentage = 0.0f;

        filterPercentage = percentage / 100.0f;

        conf.setFloat("filter_percentage", filterPercentage);

        Job job = Job.getInstance(conf, "PatentRandomSampling");
        job.setJarByClass(PatentRandomSamplingMapper.class);

        job.setMapperClass(PatentRandomSamplingMapper.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        FileSystem hdfs = FileSystem.newInstance(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
        hdfs.close();
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
