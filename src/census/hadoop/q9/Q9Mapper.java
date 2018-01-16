package cs455.hadoop.q9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/13/17.
 */
public class Q9Mapper extends Mapper<LongWritable, Text, Text, BigHouses> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10, 13);
        String segmentNumber = line.substring(24, 28);
        if (summaryLevel.equals("100") && segmentNumber.equals("0002")) {
            String state = line.substring(8, 10);
            if (!(state.equals("VI") || state.equals("PR"))) {
                int nineRoomHouses = Integer.parseInt(line.substring(2460, 2469));
                // number of houses > $250000
                int expensiveHouses = sumFields(line, 3072, 3099, 9);
                context.write(new Text(state), new BigHouses(
                                                new IntWritable(nineRoomHouses), new IntWritable(expensiveHouses)));
            }
        }
    }

    private int sumFields(String line, int start, int end, int fieldSize) {
        int result = 0;
        while (start <= end) {
            result += Integer.parseInt(line.substring(start,start + fieldSize));
            start += fieldSize;
        }
        return result;
    }
}
