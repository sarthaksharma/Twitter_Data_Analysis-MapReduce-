import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;
import org.json.*;

/**
 * Created by sarthak on 25/01/17.
 */

public class FreqUserTweet {

    public static class Map_Class extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        private final IntWritable one = new IntWritable(1);
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
            String name = null;
            String data = null;
//            System.out.println(obj1);
            try {
                id = obj1.getJSONObject("user").getString("id_str");
                name =  obj1.getJSONObject("user").getString("screen_name");
                data = id + " : "+ name;
            }
            catch (Exception e)
            {
//                System.out.print(e+"\n");
            }

            if(name!=null)
            {
                context.write(new Text(data), one);
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



