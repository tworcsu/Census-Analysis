package cs455.hadoop.q3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by toddw on 4/3/17.
 */
public class Q3Reducer extends Reducer<Text, HispanicAges, Text, DoubleWritable>{
    @Override
    protected void reduce(Text key, Iterable<HispanicAges> values, Context context) throws IOException, InterruptedException {
        double male0to18 = 0;
        double male19to29 = 0;
        double male30to39 = 0;
        double maleAll = 0;
        double female0to18 = 0;
        double female19to29 = 0;
        double female30to39 = 0;
        double femaleAll = 0;

        for (HispanicAges val : values) {
            male0to18 += val.male0to18.get();
            male19to29 += val.male19to29.get();
            male30to39 += val.male30to39.get();
            maleAll += val.maleAll.get();
            female0to18 += val.female0to18.get();
            female19to29 += val.female19to29.get();
            female30to39 += val.female30to39.get();
            femaleAll += val.femaleAll.get();
        }

        double percentMale0to18 = male0to18 / maleAll  * 100;
        double percentMale19to29 = male19to29 / maleAll  * 100;
        double percentMale30to39 = male30to39 / maleAll  * 100;
        double percentFemale0to18 = female0to18 / femaleAll  * 100;
        double percentFemale19to29 = female19to29 / femaleAll  * 100;
        double percentFemale30to39 = female30to39 / femaleAll  * 100;
        
        context.write(new Text(key.toString() + " - Male 0 to 18"), new DoubleWritable(percentMale0to18));
        context.write(new Text(key.toString() + " - Male 19 to 29"), new DoubleWritable(percentMale19to29));
        context.write(new Text(key.toString() + " - Male 30 to 39"), new DoubleWritable(percentMale30to39));
        context.write(new Text(key.toString() + " - Female 0 to 18"), new DoubleWritable(percentFemale0to18));
        context.write(new Text(key.toString() + " - Female 19 to 29"), new DoubleWritable(percentFemale19to29));
        context.write(new Text(key.toString() + " - Female 30 to 39"), new DoubleWritable(percentFemale30to39));
    }
}
