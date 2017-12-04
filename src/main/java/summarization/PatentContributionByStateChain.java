package summarization;

import bloomfiltering.InventorsBloomFiltering;
import org.apache.hadoop.fs.Path;
import reducesidejoin.PatentInventorsJoin;

public class PatentContributionByStateChain {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: PatentContributionByStateChain <inPatent> <inInventors> <out>");
            System.exit(2);
        }
        Path input = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path outputDir = new Path(args[2]);
        System.exit(run(input,input2,outputDir));
    }

    public static int run(Path input1, Path input2, Path outputDir) throws Exception {
        Path temp1 = new Path("output/temp/pcbs1");
        Path temp2 = new Path("output/temp/pcbs2");
        return InventorsBloomFiltering.run(input2, temp1)
                + PatentInventorsJoin.run(input1, temp1, temp2, "inner")
                + PatentContributionByState.run(temp2, outputDir);
    }
}
