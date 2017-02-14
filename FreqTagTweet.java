import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Created by sarthak on 26/01/17.
 */
public class FreqTagTweet {


    public static class Map_Class extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        private final IntWritable zero = new IntWritable(0);
        private Text word = new Text();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String st = value.toString();
            JSONObject obj1 = new JSONObject(st);
            String id = null;
            JSONArray hashtags = null;
            Integer hash=0;
            String data = null;
            try {
                id = (String) obj1.getString("id_str");
                hashtags =  obj1.getJSONObject("entities").getJSONArray("hashtags");
                Iterator<Object> It = hashtags.iterator();
                int ptr=0;
                while(It.hasNext())
                {
                    data=It.next().toString();
//                    System.out.println("############################################"+data+"################################################");
                     ptr++;
                }
                hash=ptr;
            }

            catch (Exception e)
            {
                System.out.print(e+" \n");
            }

            if(hash!=null && id!=null)
            {
                IntWritable no_of_hashtags= new IntWritable(hash);
                context.write(new Text(id),no_of_hashtags);
            }
            else if(id!=null && hash==null)
            {
                context.write(new Text(id),zero);
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
