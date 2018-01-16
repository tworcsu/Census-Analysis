package cs455.hadoop.q8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/12/17.
 */
public class Q8Mapper extends Mapper<LongWritable, Text, Text, OldPeople> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10,13);
        String segmentNumber = line.substring(24,28);

        if (summaryLevel.equals("100") && segmentNumber.equals("0001")) {
            String state = line.substring(8,10);
            if (!(state.equals("VI") || state.equals("PR"))) {
                int persons = Integer.parseInt(line.substring(300, 309));
                int personsOver85 = Integer.parseInt(line.substring(1065, 1074));
                context.write(new Text(state), new OldPeople(new IntWritable(persons), new IntWritable(personsOver85)));
            }
        }
    }
}
