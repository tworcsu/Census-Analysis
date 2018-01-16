package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by toddw on 4/1/17.
 */
public class Tenure implements Writable {
    public IntWritable own;
    public IntWritable rent;

    public Tenure() {
        this.own = new IntWritable();
        this.rent = new IntWritable();

    }

    public Tenure(IntWritable own, IntWritable rent) {
        this.own = own;
        this.rent = rent;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        own.write(out);
        rent.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        own.readFields(in);
        rent.readFields(in);

    }
}
