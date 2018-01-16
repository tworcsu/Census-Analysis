package cs455.hadoop.q6;

import cs455.hadoop.util.IntArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by toddw on 4/11/17.
 */
public class Q6Reducer extends Reducer<Text, IntArrayWritable, Text, Text> {
    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Integer> contractRent = new ArrayList<>();
        int total = 0;
        for (IntArrayWritable val : values) {
            for (int i = 0; i < val.size(); i++) {
                int j = val.get(i);
                if (contractRent.size() <= i) {
                    contractRent.add(i,j);
                }
                else {
                    contractRent.set(i, contractRent.get(i) + j);
                }
                total += j;
            }
        }

        int medianIndex = -1;
        int half = total / 2;
        int running = 0;
        while (running < half) {
            medianIndex++;
            running += contractRent.get(medianIndex);
        }

        String[] buckets = {"Less than $100", "$100 to $149", "$150 to $200", "$200 to $249", "$250 to $299", "$300 to $349",
                            "$350 to $399", "$400 to $449", "$450 to $499", "$500 to $549", "$550 to $599", "$600 to $649",
                            "$650 to $699", "$700 to $749", "$750 to $999", "$1000 or more"};

        context.write(key, new Text(buckets[medianIndex]));
    }
}
