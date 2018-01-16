package cs455.hadoop.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.html.HTMLIsIndexElement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by toddw on 4/3/17.
 */
public class HispanicAges implements Writable {
    public IntWritable male0to18;
    public IntWritable male19to29;
    public IntWritable male30to39;
    public IntWritable maleAll;
    public IntWritable female0to18;
    public IntWritable female19to29;
    public IntWritable female30to39;
    public IntWritable femaleAll;

    public HispanicAges() {
        this.male0to18 = new IntWritable();
        this.male19to29 = new IntWritable();
        this.male30to39 = new IntWritable();
        this.maleAll = new IntWritable();
        this.female0to18 = new IntWritable();
        this.female19to29 = new IntWritable();
        this.female30to39 = new IntWritable();
        this.femaleAll = new IntWritable();
    }

    public HispanicAges(IntWritable male0to18, IntWritable male19to29, IntWritable male30to39, IntWritable maleAll,
                        IntWritable female0to18, IntWritable female19to29, IntWritable female30to39, IntWritable femaleAll) {
        this.male0to18 = male0to18;
        this.male19to29 = male19to29;
        this.male30to39 = male30to39;
        this.maleAll = maleAll;
        this.female0to18 = female0to18;
        this.female19to29 = female19to29;;
        this.female30to39 = female30to39;
        this.femaleAll = femaleAll;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.male0to18.write(out);
        this.male19to29.write(out);
        this.male30to39.write(out);
        this.maleAll.write(out);
        this.female0to18.write(out);
        this.female19to29.write(out);
        this.female30to39.write(out);
        this.femaleAll.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.male0to18.readFields(in);
        this.male19to29.readFields(in);
        this.male30to39.readFields(in);
        this.maleAll.readFields(in);
        this.female0to18.readFields(in);
        this.female19to29.readFields(in);
        this.female30to39.readFields(in);
        this.femaleAll.readFields(in);

    }
}
