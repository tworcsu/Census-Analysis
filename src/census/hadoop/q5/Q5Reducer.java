package cs455.hadoop.q5;

import cs455.hadoop.util.IntArrayWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by toddw on 4/7/17.
 */
public class Q5Reducer extends Reducer<Text, IntArrayWritable, Text, Text> {
    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Integer> homeValues = new ArrayList<>();
        int total = 0;
        for (IntArrayWritable val : values) {
            for (int i = 0; i < val.size(); i++) {
                int j = val.get(i);
                if (homeValues.size() <= i) {
                    homeValues.add(i,j);
                }
                else {
                    homeValues.set(i, homeValues.get(i) + j);
                }
                total += j;
            }
        }

        int medianIndex = -1;
        int half = total / 2;
        while (total > half) {
            medianIndex++;
            total -= homeValues.get(medianIndex);
        }

        String[] buckets = {"Less than $15,000", "$15,000 to $19,999", "$20,000 to $24,999", "$25,000 to $29,999",
                            "$30,000 to $34,999", "$35,000 to $39,999", "$40,000 to $44,999", "$45,000 to $49,999",
                            "$50,000 to $59,999", "$60,000 to $74,999", "$75,000 to $99,999", "$100,000 to $124,999",
                            "$125,000 to $149,999", "$150,000 to $174,999", "$175,000 to $199,999", "$200,000 to $249,999",
                            "$250,000 to $299,999", "$300,000 to $399,999", "$400,000 to $499,999", "$500,000 or more"};

        context.write(key, new Text(buckets[medianIndex]));
    }
}
