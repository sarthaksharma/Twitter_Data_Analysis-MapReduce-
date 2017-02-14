import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by sarthak on 29/01/17.
 */

public class HashtagCombination
{
    public static class Map_Class extends Mapper<LongWritable, Text, Text, IntWritable>
    {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException
        {
            String st = value.toString();
            JSONObject obj1 = new JSONObject(st);
            JSONArray hashtags = null;
            String data = null;
            String Final_Value=null;
            JSONObject tmp=null;
            try
            {
                hashtags =  obj1.getJSONObject("entities").getJSONArray("hashtags");
                Iterator<Object> It = hashtags.iterator();
                ArrayList<String> Hashtag_Array = new ArrayList<String>();
                while(It.hasNext())
                {
                    tmp=(JSONObject)It.next();
                    data=tmp.getString("text");
                    Hashtag_Array.add(data);
                }
                Collections.sort(Hashtag_Array);
                String[] Hash_Arr = new String[Hashtag_Array.size()];
                Hash_Arr = Hashtag_Array.toArray(Hash_Arr);
                for(int i=0;i<Hashtag_Array.size();i++)
                {
                    for(int j=i+1;j<Hashtag_Array.size();j++)
                    {
                        Final_Value=Hash_Arr[i]+" "+Hash_Arr[j];
//                        System.out.println("############################################"+Final_Value+"################################################");

                    }
                }

                if(Final_Value!=null)
                {
                    context.write(new Text(Final_Value),one);
                }
            }

            catch (Exception e)
            {
//                System.out.print(e+" \n");
            }


        }
    }


    public static class Reduce_Class extends Reducer<Text, IntWritable, Text, Text>
    {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            int sum = 0;
            Iterator<IntWritable> valuesIt = values.iterator();
            while(valuesIt.hasNext())
            {
                sum = sum + (valuesIt.next()).get();
            }
            sum = -sum;
            String sum1 = sum+"";
            context.write(key, new Text(sum1));
        }

    }

}
