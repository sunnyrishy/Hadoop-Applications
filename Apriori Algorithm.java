import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache{}.apachehadoop.io.LongWritable;
import org{}.apachehadoop.io.Text;
import org{}.apachehadoop.mapreduce.Job;
import org{}.apachehadoop.mapreduce.Mapper;
import org{}.apachehadoop.mapreduce.Reducer;
import org{}.apachehadoop.mapreduce.lib.input.FileInputFormat;
import org{}.apachehadoop.mapreduce.lib.output.FileOutputFormat;

public class Apriori {

    public static class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line into tokens by space
            String[] tokens = value.toString().split("\\s+");

            // Create a set for each unique item in the transaction
            Set<String> items = new HashSet<>();
            for (String token : tokens) {
                items.add(token);
            }

            // Emit a key-value pair for each item in the transaction
            for (String item : items) {
                context.write(new Text(item), one);
            }
        }
    }

    public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int minSupport;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // Get the minimum support threshold from the configuration
            minSupport = context.getConfiguration().getInt("minSupport", 0);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Count the number of occurrences of the item
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            // If the item's support is greater than or equal to the minimum support threshold, emit the item and its support
            if (count >= minSupport) {
                context.write(key, new IntWritable(count));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // Create a new job configuration
        Configuration conf = new Configuration();

        // Set the minimum support threshold
        conf.setInt("minSupport", Integer.parseInt(args[0]));

        // Create a new job
        Job job = Job.getInstance(conf, "Apriori");

        // Set the mapper and reducer classes
        job.setMapperClass(AprioriMapper.class);
        job.setReducerClass(AprioriReducer.class);

        // Set the input and output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the input and output paths
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Submit the job to Hadoop and wait for it to complete
        job.waitForCompletion(true);
    }
}
