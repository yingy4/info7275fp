package main.java.topk;

import main.java.counting.PatentCountingByClassification;
import org.apache.hadoop.fs.Path;

public class PatentTopKClassificationChain {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PatentTopKClassificationChain <in> <out> <topK>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        Path temp = new Path("/temp/ptkc");
        PatentCountingByClassification.run(input,temp);
        PatentTopKClassification.run(temp,outputDir,Integer.parseInt(args[2]));
    }
}
