import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class PdelCalculator {
    public static class MapperClass extends Mapper<Text, IntArrayWritable, IntWritable, Text> {
//        int numOfPartitions;
//        @Override
//        protected void setup(Mapper<Text, IntArrayWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
//            super.setup(context);
//            // get number of partitions
//            Configuration conf = context.getConfiguration();
//            numOfPartitions = Integer.parseInt(conf.get("mapred.task.partition"));
//        }

        /**
         * for each ngram with r0 r1 r writes:
         * <r, ngram>
         * <r0, r1>
         * <r1, r0>
         * if key is "N" writes <i, "N val"> to all reducers.
         */

        @Override
        protected void map(Text key, IntArrayWritable value, Context context) throws IOException, InterruptedException {
            //if key is "N" context.getConfiguration().set("", N);
            System.out.println("reducer started!");
            StringTokenizer itr = new StringTokenizer(key.toString());
            String ngram = "";
            while (itr.hasMoreTokens()){
                ngram += itr.nextToken()+ " ";
            }
            ngram = ngram.substring(0, ngram.length() - 1);
            System.out.println(ngram);
//            if(ngram.equals("N")){
////                System.out.println("written N to conf");
////                context.getConfiguration().set("N", "" +((IntWritable)value.get()[0]).get());
////                System.out.println(context.getConfiguration().get("N"));
//                int numOfReducers = context.getConfiguration().getInt("mapreduce.job.reduces", 0);
//                System.out.println("number of reducers is: " + numOfReducers);
//                for(int i = 0; i < numOfReducers; i++){
//                    context.write(new IntWritable(i), new Text("N\t" + ((IntWritable)value.get()[0]).get()+ "\t") );
//                }
//            }
//            else{
                System.out.println(value.toString());
                IntWritable r0 = (IntWritable)value.get()[0];
                IntWritable r1 = (IntWritable)value.get()[1];
                IntWritable r = (IntWritable)value.get()[2];

                context.write(r, new Text("r\t"+ngram+ "\t"));
                context.write(r0, new Text("r0\t"+r1.get()+ "\t"));
                context.write(r1, new Text("r1\t"+r0.get()+ "\t"));
//            }
            System.out.println("mapper finished!");
        }
    }


    public static class ReducerClass extends Reducer<IntWritable,Text,Text, FloatWritable> {
        int currR = -1;
        int n0 = 0;
        int n1 = 0;
        int t01 = 0;
        int t10 = 0;
        protected Long N = 5186054851L;
        List<Text> gramsWithCurrR = new ArrayList<Text>();
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable,Text,Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            System.out.println("reducer started!");
//            if(N == null){
//                System.out.println("N received:" + N);
//                N = Long.parseLong(context.getConfiguration().get("N"));
//                context.getConfiguration().set("N", ""+N);
//            }
            for(Text val: values){
                String[] arr = val.toString().split("\t");
                String target = arr[0];

                if(currR != key.get()) {
                    float pdel = 0;
                    if(n0 + n1 != 0){ //could be zero...
                        pdel = (float) (t01 + t10) / (float) (N * (n0 + n1));
                    }

                    for (Text g : gramsWithCurrR)
                        context.write(g, new FloatWritable(pdel));

                    n0 = 0;
                    n1 = 0;
                    t01 = 0;
                    t10 = 0;
                    currR = key.get();
                    gramsWithCurrR.clear();
                }
//                if(target.equals("N")){
//                    N = Long.parseLong(arr[1]);
                 if(target.equals("r")){
                    String gram = arr[1];
                    gramsWithCurrR.add(new Text(gram));
                }else if(target.equals("r0")){
                    int r1 = Integer.parseInt(arr[1]);
                    n0++;
                    t01 += r1;
                }else{
                    int r0 = Integer.parseInt(arr[1]);
                    n1++;
                    t10 += r0;
                }

                System.out.println("reducer finished!");
            }
        }

        @Override
        protected void cleanup(Reducer<IntWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            if (currR != -1) {
                float pdel = 0;
                long N = 5186054851L;
                if(n0 + n1 != 0){ //could be zero...
                    pdel = (float) (t01 + t10) / (float) (N * (n0 + n1));
                }
                for (Text g : gramsWithCurrR)
                    context.write(g, new FloatWritable(pdel));
            }
            super.cleanup(context);
        }
    }

    public static class PartitionerClass extends Partitioner<IntWritable, Text> {

        @Override
        public int getPartition(IntWritable key, Text value, int i) {

            int numOfPartition = key.get() % i;
            return numOfPartition;
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("Pdel started!");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Template");
        job.setNumReduceTasks(20);
        job.setJarByClass(PdelCalculator.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        //job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(NgramInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class); 

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
