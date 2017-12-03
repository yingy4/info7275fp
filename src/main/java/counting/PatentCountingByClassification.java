package main.java.counting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentCountingByClassification {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PatentCountingByClassification <in> <out>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        run(input, outputDir);
    }

    public static void run(Path in, Path out) throws Exception  {
        Configuration conf = new Configuration();

        Path input = in;
        Path outputDir = out;

        Job job = Job.getInstance(conf, "PatentCountingByClassification");
        job.setJarByClass(PatentCountingByClassificationMapper.class);

        job.setMapperClass(PatentCountingByClassificationMapper.class);
        job.setReducerClass(PatentCountingByClassificationReducer.class);
        job.setCombinerClass(PatentCountingByClassificationReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
