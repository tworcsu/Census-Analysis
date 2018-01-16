package cs455.hadoop.q8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by toddw on 4/12/17.
 */
public class Q8Reducer extends Reducer<Text, OldPeople, Text, DoubleWritable> {
    private Text bestState;
    private double bestPercentage = 0;
    @Override
    protected void reduce(Text key, Iterable<OldPeople> values, Context context) throws IOException, InterruptedException {
        double persons = 0;
        double personsOver85 = 0;
        for (OldPeople val : values) {
            persons += val.persons.get();
            personsOver85 += val.personsOver85.get();
        }
        double percentOver85 = personsOver85 / persons * 100;
        if (percentOver85 > bestPercentage) {
            bestPercentage = percentOver85;
            bestState = new Text(key.toString());
        }
        //context.write(bestState, new DoubleWritable(bestPercentage));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(bestState, new DoubleWritable(bestPercentage));
    }
}
