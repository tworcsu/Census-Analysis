package cs455.hadoop.q7;

import cs455.hadoop.util.IntArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * Created by toddw on 4/12/17.
 */
public class Q7Reducer extends Reducer<Text, IntArrayWritable, Text, DoubleWritable> {
    private ArrayList<Double> averages = new ArrayList<>();
    //private HashMap<Double, String> avgsAndStates = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalRooms = 0;
        int totalHouses = 0;
        for (IntArrayWritable val : values) {
            for (int i = 0; i < val.size(); i++) {
                // number of houses with i + 1 rooms
                int j = val.get(i);
                totalHouses += j;
                totalRooms += (j * (i+1));
            }
            double average = 1.0 * totalRooms / totalHouses;
            averages.add(average);
            //avgsAndStates.put(average,key.toString());
        }
        //context.write(new DoubleWritable(1.0 * totalRooms / totalHouses), key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(averages);
        int percentile95Index = (int) Math.ceil(averages.size() * .95);
        double result = averages.get(percentile95Index);
        context.write(new Text(), new DoubleWritable(result));
    }
}
