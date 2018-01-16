package cs455.hadoop.q4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/2/17.
 */
public class Q4Mapper extends Mapper<LongWritable, Text, Text, UrbanAndRural> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10,13);
        String segmentNumber = line.substring(24,28);
        if (summaryLevel.equals("100") && segmentNumber.equals("0002")) {
            String state = line.substring(8,10);
            if (!(state.equals("VI") || state.equals("PR"))) {
                int urbanInside = Integer.parseInt(line.substring(1821, 1830));
                int urbanOutside = Integer.parseInt(line.substring(1830, 1839));
                int urban = urbanInside + urbanOutside;
                int rural = Integer.parseInt(line.substring(1839, 1848));
                int neither = Integer.parseInt(line.substring(1848,1857));

                context.write(new Text(state), new UrbanAndRural(
                        new IntWritable(urban), new IntWritable(rural), new IntWritable(neither)));
            }
        }
    }
}
