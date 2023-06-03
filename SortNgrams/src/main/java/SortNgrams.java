import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SortNgrams {

    public static class MapperClass extends Mapper<Text, FloatWritable, NgramWithP, Text> {
        @Override
        public void map(Text key, FloatWritable value, Context context) throws IOException,  InterruptedException {
            String[] ngramSplitted = key.toString().split(" ");
            String w1w2 = ngramSplitted[0] + " " + ngramSplitted[1];
            String w3 = ngramSplitted[2];
            float pdel = value.get();
            context.write(new NgramWithP(w1w2, pdel), new Text(w3));
        }
    }

    public static class ReducerClass extends Reducer<NgramWithP,Text,Text,FloatWritable> {

        @Override
        public void reduce(NgramWithP key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for(Text value : values){
                String ngram = key.getW1w2() + " " + value.toString();
                float pdel = key.getPdel();
                context.write(new Text(ngram), new FloatWritable(pdel));
            }
        }

    }

    public static class PartitionerClass extends Partitioner<NgramWithP,Text> {

        @Override
        public int getPartition(NgramWithP key, Text value, int numReducers) {
            return Math.abs(key.getW1w2().hashCode()) % numReducers;
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Template");
        job.setNumReduceTasks(1);
        job.setJarByClass(SortNgrams.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(NgramWithP.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(SortInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
