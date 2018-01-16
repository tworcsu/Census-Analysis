package cs455.hadoop.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/3/17.
 */
public class Q3Mapper extends Mapper<LongWritable, Text, Text, HispanicAges> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10,13);
        String segmentNumber = line.substring(24,28);

        if (summaryLevel.equals("100") && segmentNumber.equals("0001")) {
            String state = line.substring(8, 10);

            if (!(state.equals("VI") || state.equals("PR"))) {
                int male0to18 = sumFields(line, 3864, 3972, 9);
                int male19to29 = sumFields(line, 3981, 4017, 9);
                int male30to39 = sumFields(line, 4026, 4035, 9);
                int maleAll = male0to18 + male19to29 + male30to39 + sumFields(line, 4044, 4134, 9);

                int female0to18 = sumFields(line, 4143, 4251, 9);
                int female19to29 = sumFields(line, 4260, 4296, 9);
                int female30to39 = sumFields(line, 4305, 4314, 9);
                int femaleAll = female0to18 + female19to29 + female30to39 + sumFields(line, 4323, 4413, 9);

                context.write(new Text(state),
                        new HispanicAges(
                                new IntWritable(male0to18), new IntWritable(male19to29),
                                new IntWritable(male30to39), new IntWritable(maleAll),
                                new IntWritable(female0to18), new IntWritable(female19to29),
                                new IntWritable(female30to39), new IntWritable(femaleAll)));
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
