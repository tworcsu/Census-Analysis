package cs455.hadoop.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/1/17.
 */
public class MaritalStatus implements Writable{
    public IntWritable mNever;
    public IntWritable mOther;
    public IntWritable fNever;
    public IntWritable fOther;

    public MaritalStatus() {
        this.mNever = new IntWritable();
        this.mOther = new IntWritable();
        this.fNever = new IntWritable();
        this.fOther = new IntWritable();
    }

    public MaritalStatus(IntWritable mNever, IntWritable mOther, IntWritable fNever, IntWritable fOther) {
        this.mNever = mNever;
        this.mOther = mOther;
        this.fNever = fNever;
        this.fOther = fOther;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        mNever.write(out);
        mOther.write(out);
        fNever.write(out);
        fOther.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mNever.readFields(in);
        mOther.readFields(in);
        fNever.readFields(in);
        fOther.readFields(in);
    }
}
