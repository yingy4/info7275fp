package main.java.replicatedjoin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class PatentClassificationCodeReplicatedJoinMapper extends Mapper<Object, Text, Text, NullWritable> {


    static Logger log = Logger.getLogger(PatentClassificationCodeReplicatedJoinMapper.class);

    HashMap<String, String> inMemoryTable;
    private String joinType = null;

    public void setup(Context context) throws IOException {
        inMemoryTable = new HashMap<String, String>();
        URI[] uris = context.getCacheFiles();

        joinType = context.getConfiguration().get("join_type");

        if (uris == null) {
            String msg = "Could not find files in distributed cache";
            log.error(msg);
            throw new IOException(msg);
        }

        log.info("will now read in memory " + uris.length + " files");
        for (URI uri : uris) {
            System.out.println("Will now read in memory file " + uri);
            File f = new File(uri);
            FileInputStream fis = new FileInputStream(f);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] sline = line.split("\t");
                inMemoryTable.put(sline[0], line);
            }
            br.close();
        }
    }


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split("\t");

            String joinValue = null;

            joinValue = inMemoryTable.get(sline[0]);

            Text output = new Text();

            if (joinType.equalsIgnoreCase("inner")) {
                if (joinValue != null && !joinValue.isEmpty()) {
                    output.set(value.toString() + "," + joinValue);
                    context.write(output, NullWritable.get());
                }
            } else if (joinType.equalsIgnoreCase("leftouter")) {
                if (joinValue == null || joinValue.isEmpty()) {
                    output.set(value.toString());
                    context.write(output, NullWritable.get());
                } else {
                    output.set(value.toString() + "," + joinValue);
                    context.write(output, NullWritable.get());
                }
            }

    }

}
