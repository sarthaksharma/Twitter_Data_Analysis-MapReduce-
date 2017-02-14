import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * Created by sarthak on 29/01/17.
 */
public class TimeTopTweets
{
    public static class Map_Class extends Mapper<LongWritable, Text, IntWritable, Text>
    {

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        double week ;
        int retweet_count = -1;
        String tweet_id;

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
                retweet_count = obj1.getInt("retweet_count");
                retweet_count=-retweet_count;
                tweet_id =  obj1.getString("id_str");
                date_str = obj1.getString("created_at");
                Date date = sdf.parse(date_str);
                milliseconds=date.getTime();
                week= (milliseconds - reference)/(604800000.0);
                String data =(int)week+"\t"+tweet_id;
                System.out.println("############################################            "+ retweet_count +"            "+data+"            "+week+"           ################################################");
                context.write(new IntWritable(retweet_count),new Text(data));
            }
            catch (Exception e)
            {
                System.out.print(e+"\n");
            }
        }
    }

    public static class CaderPartitioner extends
            Partitioner< IntWritable, Text >
    {
        @Override
        public int getPartition(IntWritable key, Text value, int numReduceTasks)
        {
            String[] str = value.toString().split("\t");
            int week = Integer.parseInt(str[0]);

            if(numReduceTasks == 0)
            {
                return 0;
            }

            else
            {
                return week-1;
            }
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
                context.write(new IntWritable((key.get())), valuesIt.next());
            }

        }

    }



}



