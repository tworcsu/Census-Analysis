package cs455.hadoop.q9;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by toddw on 4/13/17.
 */
public class Q9Reducer extends Reducer<Text, BigHouses, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<BigHouses> values, Context context) throws IOException, InterruptedException {
        double nineRoomHouses = 0;
        double expensiveHouses = 0;
        for (BigHouses val : values) {
            nineRoomHouses += val.nineRoomHouses.get();
            expensiveHouses += val.expensiveHouses.get();
        }
        double houseRatio = nineRoomHouses / expensiveHouses;
        context.write(key, new DoubleWritable(houseRatio));
    }
}
