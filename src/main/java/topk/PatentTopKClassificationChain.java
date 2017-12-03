package topk;

import counting.PatentCountingByClassification;
import replicatedjoin.PatentClassificationCodeReplicatedJoin;
import org.apache.hadoop.fs.Path;

public class PatentTopKClassificationChain {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: PatentTopKClassificationChain <in> <indict> <out> <topK>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        Path temp1 = new Path("output/temp/ptkc");
        Path temp2 = new Path("output/temp/ptkc2");
        int k = Integer.parseInt(args[3]);
        System.exit(run(input,input2,temp1,temp2,outputDir,k));
    }

    public static int run(Path input, Path input2, Path temp1, Path temp2, Path outputDir, int k) throws Exception {
        return PatentCountingByClassification.run(input,temp1)
                + PatentTopKClassification.run(temp1,temp2,k)
                + PatentClassificationCodeReplicatedJoin.run(temp2,input2,outputDir,"inner");
    }
}
