package cs455.hadoop.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by toddw on 4/1/17.
 */
public class Q2Mapper extends Mapper<LongWritable, Text, Text, MaritalStatus> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String summaryLevel = line.substring(10,13);
        String segmentNumber = line.substring(24,28);

        if (summaryLevel.equals("100") && segmentNumber.equals("0001")) {
            String state = line.substring(8,10);

            if (!(state.equals("VI") || state.equals("PR"))) {
                int mNever = Integer.parseInt(line.substring(4422, 4431));
                int mMarried = Integer.parseInt(line.substring(4431, 4440));
                int mSeparated = Integer.parseInt(line.substring(4440, 4449));
                int mWidowed = Integer.parseInt(line.substring(4449, 4458));
                int mOther = mMarried + mSeparated + mWidowed;

                int fNever = Integer.parseInt(line.substring(4467, 4476));
                int fMarried = Integer.parseInt(line.substring(4476, 4485));
                int fSeparated = Integer.parseInt(line.substring(4485, 4494));
                int fWidowed = Integer.parseInt(line.substring(4494, 4503));
                int fOther = fMarried + fSeparated + fWidowed;

                context.write(new Text(state), new MaritalStatus(new IntWritable(mNever), new IntWritable(mOther),
                        new IntWritable(fNever), new IntWritable(fOther)));
            }
        }


    }
}
