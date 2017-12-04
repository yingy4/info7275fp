package reducesidejoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PatentInventorsReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;

    public void setup(Context context) {
        joinType = context.getConfiguration().get("join_type");
    }

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        listA.clear();
        listB.clear();

        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();
            if (tmp.charAt(0) == 'A') {
                listA.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt(0) == 'B') {
                listB.add(new Text(tmp.toString().substring(1)));
            }
        }
        executeJoinLogic(context);
    }
    private void executeJoinLogic(Context context) throws IOException,InterruptedException {

        if (joinType.equalsIgnoreCase("inner")) {
            // If both lists are not empty, join A with B
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    for (Text B : listB) {
                        context.write(A, B);
                    }
                }
            }
        } else if (joinType.equalsIgnoreCase("leftouter")) {
            // For each entry in A,
            for (Text A : listA) {
                // If list B is not empty, join A and B
                if (!listB.isEmpty()) {
                    for (Text B : listB) {
                        context.write(A, B);
                    }
                } else {
                    // Else, output A by itself
                    context.write(A, EMPTY_TEXT);
                }
            }
        } else if (joinType.equalsIgnoreCase("rightouter")) {
            // For each entry in B,
            for (Text B : listB) {
                // If list A is not empty, join A and B
                if (!listA.isEmpty()) {
                    for (Text A : listA) {
                        context.write(A, B);
                    }
                } else {
                    // Else, output B by itself
                    context.write(EMPTY_TEXT, B);
                }
            }
        } else if (joinType.equalsIgnoreCase("fullouter")) {
            // If list A is not empty
            if (!listA.isEmpty()) {
                // For each entry in A
                for (Text A : listA) {
                    // If list B is not empty, join A with B
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        // Else, output A by itself
                        context.write(A, EMPTY_TEXT);
                    }
                }
            } else {
                // If list A is empty, just output B
                for (Text B : listB) {
                    context.write(EMPTY_TEXT, B);
                }
            }
        } else if (joinType.equalsIgnoreCase("anti")) {
            // If list A is empty and B is empty or vice versa
            if (listA.isEmpty() ^ listB.isEmpty()) {
                // Iterate both A and B with null values
                // The previous XOR check will make sure exactly one of
                // these lists is empty and therefore the list will be
                // skipped
                for (Text A : listA) {
                    context.write(A, EMPTY_TEXT);
                }
                for (Text B : listB) {
                    context.write(EMPTY_TEXT, B);
                }
            }
        }
    }

}
