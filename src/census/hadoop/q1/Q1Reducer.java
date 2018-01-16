package cs455.hadoop.q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cs455.hadoop.q1.Tenure;

import java.io.IOException;

/**
 * Created by toddw on 4/1/17.
 */
public class Q1Reducer extends Reducer<Text, Tenure, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<Tenure> values, Context context) throws IOException, InterruptedException {
        double owned = 0;
        double rented = 0;
        for (Tenure val : values) {
            owned += val.own.get();
            rented += val.rent.get();
        }
        double all = owned + rented;
        double percentOwned = owned / all * 100;
        double percentRented = rented / all * 100;
        context.write(new Text(key.toString() + " - percentage owned"), new DoubleWritable(percentOwned));
        context.write(new Text(key.toString() + " - percentage rented"), new DoubleWritable(percentRented));
    }
}
