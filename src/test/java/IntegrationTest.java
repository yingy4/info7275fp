import counting.PatentCountingByDate;
import org.junit.jupiter.api.Assertions;
import randomsampling.PatentRandomSampling;
import topk.PatentTopKClassificationChain;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

class IntegrationTest {

    @Test
    void runPatentCountingByDate() throws Exception {
        Path input = new Path("input/testinput/rs2016.csv");
        Path outputDir = new Path("output/testoutput1");
        Assertions.assertEquals(0, PatentCountingByDate.run(input,outputDir), "PatentCountingByDate failed!");
        File f = new File("output/testoutput1/part-r-00000");
        FileInputStream fis = new FileInputStream(f);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        int count = 0;
        while ((line = br.readLine()) != null) {
            count++;
        }
        br.close();
        Assertions.assertEquals(288, count, "Total count not match!");
    }

    @Test
    void runPatentRandomSampling() throws Exception {
        Path input = new Path("input/testinput/rs2016.csv");
        Path outputDir = new Path("output/testoutput3");
        Assertions.assertEquals(0, PatentRandomSampling.run(input, outputDir, 1), "PatentRandomSampling failed!");
        File f = new File("output/testoutput3/part-r-00000");
        FileInputStream fis = new FileInputStream(f);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        int count = 0;
        while ((line = br.readLine()) != null) {
            count++;
        }
        br.close();
        //1% of 2883 record
        Assertions.assertTrue(count <= 60, "Random Sampling not working properly!");
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
        Assertions.assertEquals(0, PatentTopKClassificationChain.run(input,input2,temp1,temp2,outputDir,k), "PatentTopKClassificationChain failed!");

        ArrayList<String> result = new ArrayList<String>();
        result.add("257	Active solid-state devices (e.g., transistors, solid-state diodes),114");
        result.add("370	Multiplex communications,110");
        result.add("514	Drug, bio-affecting and body treating compositions,89");
        result.add("424	Drug, bio-affecting and body treating compositions,77");
        result.add("435	Chemistry: molecular biology and microbiology,70");
        result.add("455	Telecommunications,57");
        result.add("428	Stock material or miscellaneous articles,54");
        result.add("705	Data processing: financial, business practice, management, or cost/price determination,50");
        result.add("709	Electrical computers and digital processing systems: multicomputer data transferring,48");
        result.add("438	Semiconductor device manufacturing: process,45");

        File f = new File("output/testoutput2/part-m-00000");
        FileInputStream fis = new FileInputStream(f);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        String line = null;
        int count = 0;
        while ((line = br.readLine()) != null) {
            Assertions.assertEquals(result.get(count), line, "Result line " + count + " not right!");
            count++;
        }
        br.close();
    }

}