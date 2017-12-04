package bloomfiltering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InventorsBloomFiltering {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: InventorsBloomFiltering <in> <out>");
            System.exit(2);
        }

        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        System.exit(run(input, outputDir));
    }

    public static int run(Path input, Path outputDir) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "InventorsBloomFiltering");
        job.setJarByClass(InventorsBloomFilteringMapper.class);

        job.setMapperClass(InventorsBloomFilteringMapper.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);

        FileSystem hdfs = FileSystem.newInstance(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
        hdfs.close();
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
