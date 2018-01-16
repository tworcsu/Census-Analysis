package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cs455.hadoop.q1.Tenure;

import java.io.IOException;

/**
 * Created by toddw on 4/1/17.
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, Tenure> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10,13);
        String segmentNumber = line.substring(24,28);

        if (summaryLevel.equals("100") && segmentNumber.equals("0002")) {
            String state = line.substring(8,10);
            if (!(state.equals("VI") || state.equals("PR"))) {
                int ownerOccupied = Integer.parseInt(line.substring(1803, 1812));
                int renterOccupied = Integer.parseInt(line.substring(1812, 1821));
                context.write(new Text(state), new Tenure(new IntWritable(ownerOccupied), new IntWritable(renterOccupied)));
            }
        }
    }
}
