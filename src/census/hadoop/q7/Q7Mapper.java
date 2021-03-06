package cs455.hadoop.q7;

import cs455.hadoop.util.IntArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/12/17.
 */
public class Q7Mapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10, 13);
        String segmentNumber = line.substring(24, 28);
        if (summaryLevel.equals("100") && segmentNumber.equals("0002")) {
            String state = line.substring(8, 10);
            if (!(state.equals("VI") || state.equals("PR"))) {
                int[] roomCounts = fieldsToArray(line, 2388, 9, 9);
                IntArrayWritable a = new IntArrayWritable(roomCounts);
                context.write(new Text(state), a);
            }
        }
    }

    private int[] fieldsToArray(String line, int start, int numFields, int fieldSize) {
        int[] result = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            result[i] = Integer.parseInt(line.substring(start,start + fieldSize));
            start += fieldSize;
        }
        return result;
    }
}
