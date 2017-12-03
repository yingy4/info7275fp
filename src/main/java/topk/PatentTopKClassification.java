package main.java.topk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentTopKClassification {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PatentTopKClassification <in> <out> <topK>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        run(input, outputDir, Integer.parseInt(args[2]));
    }

    public static void run(Path in, Path out, int k) throws Exception {
        Configuration conf = new Configuration();

        Path input = in;
        Path outputDir = out;
        conf.setInt("top_k", k);

        Job job = Job.getInstance(conf, "PatentTopKClassification");
        job.setJarByClass(PatentTopKClassificationMapper.class);

        job.setMapperClass(PatentTopKClassificationMapper.class);
        job.setReducerClass(PatentTopKClassificationReducer.class);
        job.setCombinerClass(PatentTopKClassificationReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(NullWritable.class);
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
