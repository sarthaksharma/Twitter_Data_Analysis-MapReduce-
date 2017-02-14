import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Iterator;


/**
 * Created by sarthak on 27/01/17.
 */


public class FreqLangTweet {


    public static class Map_Class extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final IntWritable one = new IntWritable(1);

        private Text word = new Text();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String st = value.toString();
            JSONObject obj1 = new JSONObject(st);
            String id = null;
            String name = null;
//            System.out.println(obj1);
            try {
                id = (String) obj1.getString("lang");
//                System.out.println("############################################"+new Text(id)+"################################################");
                if(id!=null)
                {

                    context.write(new Text(id),one);

                }
            }
            catch (Exception e)
            {
//                System.out.print(e+"\n");
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
//            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%"+new IntWritable(sum)+"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

            context.write(key, new IntWritable(sum));
        }
    }


}
