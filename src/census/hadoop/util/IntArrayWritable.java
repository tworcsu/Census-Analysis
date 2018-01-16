package cs455.hadoop.util;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/10/17.
 */
public class IntArrayWritable implements Writable {
    private int[] array;
    private int length = 0;

    public IntArrayWritable() {
    }

    public IntArrayWritable(int[] array) {
        this.array = array;
        this.length = array.length;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.length = in.readInt();
        array = new int[length];
        for (int i = 0; i < length; i++) {
            array[i] = in.readInt();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeInt(array[i]);
        }
    }

    public int get(int i) {
        return array[i];
    }

    public int size() {
        return length;
    }
}
