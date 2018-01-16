package cs455.hadoop.q4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by toddw on 4/2/17.
 */
public class Q4Reducer extends Reducer<Text, UrbanAndRural, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<UrbanAndRural> values, Context context) throws IOException, InterruptedException {
        double urban = 0;
        double rural = 0;
        double neither = 0;
        for (UrbanAndRural val : values) {
            urban += val.urban.get();
            rural += val.rural.get();
            neither += val.neither.get();
        }
        double all = urban + rural + neither;
        double percentUrban = urban / all * 100;
        double percentRural = rural / all * 100;

        context.write(new Text(key.toString() + " - percentage urban"), new DoubleWritable(percentUrban));
        context.write(new Text(key.toString() + " - percentage rural"), new DoubleWritable(percentRural));
    }
}
