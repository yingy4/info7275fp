package reducesidejoin;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PatentInventorsJoin {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PatentInventorsJoin <in1> <in2> <out> <join_type>");
            System.exit(2);
        }
        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        System.exit(run(input1, input2, outputDir, args[3]));
    }

    public static int run(Path in1, Path in2, Path out, String joinType) throws Exception {
        Configuration conf = new Configuration();
        Path outputDir = out;

        conf.set("join_type", joinType);

        Job job = Job.getInstance(conf, "PatentInventorsJoin");

        job.setJarByClass(InventorMapper.class);

        MultipleInputs.addInputPath(job, in1, TextInputFormat.class, InventorMapper.class);

        MultipleInputs.addInputPath(job, in2, TextInputFormat.class, PatentMapper.class);

        job.setReducerClass(PatentInventorsReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputDir);

        FileSystem hdfs = FileSystem.newInstance(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
        hdfs.close();
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
