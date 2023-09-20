import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;


public class Multijoin{
    public class MapA extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split(",");
            context.write(new Text(line[1]), new Text("A"+line[0]));
        }
    }

    public class MapB extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split(",");
            context.write(new Text(line[0]), new Text("B"+line[1]));
        }
    }

    public class MapC extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split(",");
            context.write(new Text(line[0]), new Text("C"+line[1]));
        }
    }

    
    public class FirstJoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            ArrayList<String> listA = new ArrayList<String>();
            ArrayList<String> listB = new ArrayList<String>();
            for(Text val: values){
                String[] parts = val.toString().split(",");
                if(parts[0].equals("A")){
                    listA.add(parts[1]);
                }else if(parts[0].equals("B")){
                    listB.add(parts[1]);
                }
            }
            for(String A: listA){
                for(String B: listB){
                    context.write(new Text(A), new Text(B));
                }
            }
        }
    }

    public class SecondJoinReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            ArrayList<String> listC = new ArrayList<String>();
            for(Text val: values){
                String[] parts = val.toString().split(",");
                if(parts[0].equals("C")){
                    listC.add(parts[1]);
                }
            }
            for(String C: listC){
                context.write(new Text(key), new Text(C));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "First Join");
        job1.setJarByClass(Multijoin.class);
        job1.setMapperClass(MapA.class);
        job1.setMapperClass(MapB.class);
        job1.setReducerClass(FirstJoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Second Join");
        job2.setJarByClass(Multijoin.class);
        job2.setMapperClass(MapC.class);
        job2.setReducerClass(SecondJoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);
    }
}
