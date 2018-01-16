package cs455.hadoop.q4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/2/17.
 */
public class UrbanAndRural implements Writable {
    public IntWritable urban;
    public IntWritable rural;
    public IntWritable neither;

    public UrbanAndRural() {
        this.urban = new IntWritable();
        this.rural = new IntWritable();
        this.neither = new IntWritable();
    }

    public UrbanAndRural(IntWritable urban, IntWritable rural, IntWritable neither) {
        this.urban = urban;
        this.rural = rural;
        this.neither = neither;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        urban.write(out);
        rural.write(out);
        neither.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        urban.readFields(in);
        rural.readFields(in);
        neither.readFields(in);
    }
}
