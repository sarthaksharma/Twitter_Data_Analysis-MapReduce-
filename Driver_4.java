import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.UUID;

/**
 * Created by sarthak on 27/01/17.
 */

public class Driver_4 extends Configured implements Tool
{

    public static void main(String[] args)  throws Exception
    {
        int exitCode = ToolRunner.run(new Driver_4(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.printf("Usage: %s needs two arguments, input and outputfiles\n", getClass().getSimpleName());
            return -1;
        }

        Job job = new Job();
        job.setJarByClass(Driver_4.class);
        job.setJobName("TopCooccurrence_Temp");

        FileInputFormat.addInputPath(job, new Path(args[0]));
//        String output=args[1]+ "_Temp_"+UUID.randomUUID();
        String output=args[1]+ "_Temp_";
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(96);

        job.setMapperClass(HashtagCombination.Map_Class.class);
        job.setReducerClass(HashtagCombination.Reduce_Class.class);

        int returnValue = job.waitForCompletion(true) ? 0 : 1;

        if (job.isSuccessful())
        {
            System.out.println("Job was successful");
        } else if (!job.isSuccessful())
        {
            System.out.println("Job was not successful");
        }

        if (returnValue==1)
        {
            return returnValue;
        }

        Job job1 = new Job();
        job1.setJarByClass(Driver_4.class);
        job1.setJobName("TopCooccurrence");

        FileInputFormat.addInputPath(job1, new Path(output));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]+ UUID.randomUUID()));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setNumReduceTasks(1);
        job1.setMapperClass(TopCooccurrence.Map_Class.class);
        job1.setReducerClass(TopCooccurrence.Reduce_Class.class);

        int returnValue1 = job1.waitForCompletion(true) ? 0 : 1;

        if (job1.isSuccessful())
        {
            System.out.println("Job was successful");
        } else if (!job1.isSuccessful())
        {
            System.out.println("Job was not successful");
        }


        return returnValue;
    }

}

