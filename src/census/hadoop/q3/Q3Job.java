package cs455.hadoop.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by toddw on 4/3/17.
 */
public class Q3Job {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // name of job
            Job job = Job.getInstance(conf, "Q1");
            // current class
            job.setJarByClass(Q3Job.class);
            // mapper
            job.setMapperClass(Q3Mapper.class);
            // reducer
            job.setReducerClass(Q3Reducer.class);
            // outputs from mapper (optional if same as reducer output)
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(HispanicAges.class);
            // outputs from reducer (not optional)
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // input path
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // output path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // block until job is complete
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
