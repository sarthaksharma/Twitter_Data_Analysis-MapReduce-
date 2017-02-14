import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

/**
 * Created by sarthak on 27/01/17.
 */


public class TopCooccurrence
{
    public static class Map_Class extends Mapper<LongWritable, Text, IntWritable, Text>
    {

        private Text word = new Text();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line, "\t");


            word.set(st.nextToken());
            int num = Integer.parseInt(st.nextToken());

            IntWritable count = new IntWritable(num);
            context.write(count, word);
//            System.out.println("############################################        " + word + "        " + count + "        ################################################");

        }
    }

    public static class Reduce_Class extends Reducer<IntWritable ,Text, IntWritable, Text>
        {
            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException
            {
                Iterator<Text> valuesIt = values.iterator();

                while(valuesIt.hasNext())
                {
                    context.write(new IntWritable(-(key.get())), valuesIt.next());
                }

            }

        }

}
