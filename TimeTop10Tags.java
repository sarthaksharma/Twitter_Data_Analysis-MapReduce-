import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.json.JSONObject;
import java.text.*;
import java.io.IOException;
import java.util.*;

/**
 * Created by sarthak on 29/01/17.
 */
public class TimeTop10Tags
{


    public static class Map_Class extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        double week ;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String st = value.toString();
            JSONObject obj1 = new JSONObject(st);
            String date_str = null;
            long milliseconds;
            long reference ;
            SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
            SimpleDateFormat sdf1 = new SimpleDateFormat("MMM dd yyyy");


            try {
                Date ref_date = sdf1.parse("Oct 17 2016");
                reference = ref_date.getTime();
                date_str = obj1.getString("created_at");
                Date date = sdf.parse(date_str);
                milliseconds=date.getTime();
                week= (milliseconds - reference)/(604800000.0);
//                System.out.println("############################################            "+ milliseconds +"            "+reference+"            "+week+"           ################################################");

                context.write(new Text((int)week+""), one);
            }
            catch (Exception e)
            {
                System.out.print(e+"\n");
            }
        }
    }

    public static class CaderPartitioner extends
            Partitioner < Text, Text >
    {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks)
        {
            String[] str = value.toString().split("\t");
            int age = Integer.parseInt(str[2]);

            if(numReduceTasks == 0)
            {
                return 0;
            }

            if(age<=20)
            {
                return 0;
            }
            else if(age>20 && age<=30)
            {
                return 1 % numReduceTasks;
            }
            else
            {
                return 2 % numReduceTasks;
            }
        }
    }


    public static class Reduce_Class extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            IntWritable x=new IntWritable(5);
            Iterator<IntWritable> valuesIt = values.iterator();

            while(valuesIt.hasNext())
            {
                sum = sum + (valuesIt.next()).get();
            }
            context.write(key, new IntWritable(sum));
        }
    }



}
