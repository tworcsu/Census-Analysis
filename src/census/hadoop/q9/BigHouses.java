package cs455.hadoop.q9;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/13/17.
 */
public class BigHouses implements Writable {
    public IntWritable nineRoomHouses;
    public IntWritable expensiveHouses;

    public BigHouses() {
        this.nineRoomHouses = new IntWritable();
        this.expensiveHouses = new IntWritable();

    }

    public BigHouses(IntWritable nineRoomHouses, IntWritable expensiveHouses) {
        this.nineRoomHouses = nineRoomHouses;
        this.expensiveHouses = expensiveHouses;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        nineRoomHouses.write(out);
        expensiveHouses.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nineRoomHouses.readFields(in);
        expensiveHouses.readFields(in);
    }
}
