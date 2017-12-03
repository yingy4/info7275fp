package replicatedjoin;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class PatentClassificationCodeReplicatedJoin {

    static Logger log = Logger.getLogger(PatentClassificationCodeReplicatedJoin.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PatentClassificationCodeReplicatedJoin <in1> <in2> <out> <join_type>");
            System.exit(2);
        }
        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        System.exit(run(input1, input2, outputDir, args[3]));
    }

    public static int run(Path in1, Path in2, Path out, String joinType) throws Exception {
        Configuration conf = new Configuration();

        conf.set("join_type", joinType);

        Job job = Job.getInstance(conf, "PatentClassificationCodeReplicatedJoin");

        // Small dataset to Join
        FileSystem hdfs = FileSystem.newInstance(conf);
        log.info("Find files in path " + in2);
        RemoteIterator<LocatedFileStatus> ri = hdfs.listFiles(
                in2, true);
        boolean atLeastOne = false;
        while (ri.hasNext()) {
            LocatedFileStatus lfs = ri.next();
            Path file = lfs.getPath();
            log.info("Adding file " + file.toString()
                    + " to distributed cache");
            job.addCacheFile(file.toUri());
            atLeastOne = true;
        }

        if (!atLeastOne) {
            String msg = "Was not able to add any file to distributed cache";
            log.error(msg);
            throw new IOException(msg);
        }

        job.setJarByClass(PatentClassificationCodeReplicatedJoinMapper.class);

        job.setMapperClass(PatentClassificationCodeReplicatedJoinMapper.class);
        TextInputFormat.addInputPath(job, in1);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, out);


        if (hdfs.exists(out))
            hdfs.delete(out, true);
        hdfs.close();
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
