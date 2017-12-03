import counting.PatentCountingByDate;
import org.junit.jupiter.api.Assertions;
import topk.PatentTopKClassificationChain;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

class IntegrationTest {

    @Test
    void runPatentCountingByDate() throws Exception {
        Path input = new Path("input/testinput/rs2016.csv");
        Path outputDir = new Path("output/testoutput1");
        Assertions.assertEquals(0, PatentCountingByDate.run(input,outputDir), "PatentCountingByDate failed");
    }

    @Test
    void runPatentTopKClassificationChain() throws Exception {
        String[] ins = new String[4];
        ins[0] = "input/testinput/rs2016.csv";
        ins[1] = "input/dict";
        ins[2] = "output/testoutput2";
        ins[3] = "10";
        Path input = new Path(ins[0]);
        Path input2 = new Path(ins[1]);
        Path outputDir = new Path(ins[2]);
        Path temp1 = new Path("output/temp/ptkc");
        Path temp2 = new Path("output/temp/ptkc2");
        int k = Integer.parseInt(ins[3]);
        Assertions.assertEquals(0, PatentTopKClassificationChain.run(input,input2,temp1,temp2,outputDir,k), "PatentTopKClassificationChain failed");
    }

}