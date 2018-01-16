package cs455.hadoop.q8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/12/17.
 */
public class OldPeople implements Writable {
    public IntWritable persons;
    public IntWritable personsOver85;

    public OldPeople() {
        this.persons = new IntWritable();
        this.personsOver85 = new IntWritable();

    }

    public OldPeople(IntWritable persons, IntWritable personsOver85) {
        this.persons = persons;
        this.personsOver85 = personsOver85;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        persons.write(out);
        personsOver85.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        persons.readFields(in);
        personsOver85.readFields(in);

    }
}
