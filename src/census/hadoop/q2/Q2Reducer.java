package cs455.hadoop.q2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by toddw on 4/1/17.
 */
public class Q2Reducer extends Reducer<Text, MaritalStatus, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<MaritalStatus> values, Context context) throws IOException, InterruptedException {
        double mNever = 0;
        double mOther = 0;
        double fNever = 0;
        double fOther = 0;
        for (MaritalStatus val : values) {
            mNever += val.mNever.get();
            mOther += val.mOther.get();
            fNever += val.fNever.get();
            fOther += val.fOther.get();
        }
        double mAll = mNever + mOther;
        double fAll = fNever + fOther;
        double mPercentNever = mNever / mAll * 100;
        double fPercentNever = fNever / fAll * 100;
        context.write(new Text(key.toString() + " - unmarried males"), new DoubleWritable(mPercentNever));
        context.write(new Text(key.toString() + " - unmarried females"), new DoubleWritable(fPercentNever));

    }
}
