import java.io.IOException;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import jdk.nashorn.internal.runtime.regexp.joni.Regex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OccurrencesCounter {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

        List<String> stopWords = new ArrayList<String>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String s = "a\n" +
                    "about\n" +
                    "above\n" +
                    "across\n" +
                    "after\n" +
                    "afterwards\n" +
                    "again\n" +
                    "against\n" +
                    "all\n" +
                    "almost\n" +
                    "alone\n" +
                    "along\n" +
                    "already\n" +
                    "also\n" +
                    "although\n" +
                    "always\n" +
                    "am\n" +
                    "among\n" +
                    "amongst\n" +
                    "amoungst\n" +
                    "amount\n" +
                    "an\n" +
                    "and\n" +
                    "another\n" +
                    "any\n" +
                    "anyhow\n" +
                    "anyone\n" +
                    "anything\n" +
                    "anyway\n" +
                    "anywhere\n" +
                    "are\n" +
                    "around\n" +
                    "as\n" +
                    "at\n" +
                    "back\n" +
                    "be\n" +
                    "became\n" +
                    "because\n" +
                    "become\n" +
                    "becomes\n" +
                    "becoming\n" +
                    "been\n" +
                    "before\n" +
                    "beforehand\n" +
                    "behind\n" +
                    "being\n" +
                    "below\n" +
                    "beside\n" +
                    "besides\n" +
                    "between\n" +
                    "beyond\n" +
                    "bill\n" +
                    "both\n" +
                    "bottom\n" +
                    "but\n" +
                    "by\n" +
                    "call\n" +
                    "can\n" +
                    "cannot\n" +
                    "cant\n" +
                    "co\n" +
                    "computer\n" +
                    "con\n" +
                    "could\n" +
                    "couldnt\n" +
                    "cry\n" +
                    "de\n" +
                    "describe\n" +
                    "detail\n" +
                    "do\n" +
                    "done\n" +
                    "down\n" +
                    "due\n" +
                    "during\n" +
                    "each\n" +
                    "eg\n" +
                    "eight\n" +
                    "either\n" +
                    "eleven\n" +
                    "else\n" +
                    "elsewhere\n" +
                    "empty\n" +
                    "enough\n" +
                    "etc\n" +
                    "even\n" +
                    "ever\n" +
                    "every\n" +
                    "everyone\n" +
                    "everything\n" +
                    "everywhere\n" +
                    "except\n" +
                    "few\n" +
                    "fifteen\n" +
                    "fify\n" +
                    "fill\n" +
                    "find\n" +
                    "fire\n" +
                    "first\n" +
                    "five\n" +
                    "for\n" +
                    "former\n" +
                    "formerly\n" +
                    "forty\n" +
                    "found\n" +
                    "four\n" +
                    "from\n" +
                    "front\n" +
                    "full\n" +
                    "further\n" +
                    "get\n" +
                    "give\n" +
                    "go\n" +
                    "had\n" +
                    "has\n" +
                    "hasnt\n" +
                    "have\n" +
                    "he\n" +
                    "hence\n" +
                    "her\n" +
                    "here\n" +
                    "hereafter\n" +
                    "hereby\n" +
                    "herein\n" +
                    "hereupon\n" +
                    "hers\n" +
                    "herself\n" +
                    "him\n" +
                    "himself\n" +
                    "his\n" +
                    "how\n" +
                    "however\n" +
                    "hundred\n" +
                    "i\n" +
                    "ie\n" +
                    "if\n" +
                    "in\n" +
                    "inc\n" +
                    "indeed\n" +
                    "interest\n" +
                    "into\n" +
                    "is\n" +
                    "it\n" +
                    "its\n" +
                    "itself\n" +
                    "keep\n" +
                    "last\n" +
                    "latter\n" +
                    "latterly\n" +
                    "least\n" +
                    "less\n" +
                    "ltd\n" +
                    "made\n" +
                    "many\n" +
                    "may\n" +
                    "me\n" +
                    "meanwhile\n" +
                    "might\n" +
                    "mill\n" +
                    "mine\n" +
                    "more\n" +
                    "moreover\n" +
                    "most\n" +
                    "mostly\n" +
                    "move\n" +
                    "much\n" +
                    "must\n" +
                    "my\n" +
                    "myself\n" +
                    "name\n" +
                    "namely\n" +
                    "neither\n" +
                    "never\n" +
                    "nevertheless\n" +
                    "next\n" +
                    "nine\n" +
                    "no\n" +
                    "nobody\n" +
                    "none\n" +
                    "noone\n" +
                    "nor\n" +
                    "not\n" +
                    "nothing\n" +
                    "now\n" +
                    "nowhere\n" +
                    "of\n" +
                    "off\n" +
                    "often\n" +
                    "on\n" +
                    "once\n" +
                    "one\n" +
                    "only\n" +
                    "onto\n" +
                    "or\n" +
                    "other\n" +
                    "others\n" +
                    "otherwise\n" +
                    "our\n" +
                    "ours\n" +
                    "ourselves\n" +
                    "out\n" +
                    "over\n" +
                    "own\n" +
                    "part\n" +
                    "per\n" +
                    "perhaps\n" +
                    "please\n" +
                    "put\n" +
                    "rather\n" +
                    "re\n" +
                    "same\n" +
                    "see\n" +
                    "seem\n" +
                    "seemed\n" +
                    "seeming\n" +
                    "seems\n" +
                    "serious\n" +
                    "several\n" +
                    "she\n" +
                    "should\n" +
                    "show\n" +
                    "side\n" +
                    "since\n" +
                    "sincere\n" +
                    "six\n" +
                    "sixty\n" +
                    "so\n" +
                    "some\n" +
                    "somehow\n" +
                    "someone\n" +
                    "something\n" +
                    "sometime\n" +
                    "sometimes\n" +
                    "somewhere\n" +
                    "still\n" +
                    "such\n" +
                    "system\n" +
                    "take\n" +
                    "ten\n" +
                    "than\n" +
                    "that\n" +
                    "the\n" +
                    "their\n" +
                    "them\n" +
                    "themselves\n" +
                    "then\n" +
                    "thence\n" +
                    "there\n" +
                    "thereafter\n" +
                    "thereby\n" +
                    "therefore\n" +
                    "therein\n" +
                    "thereupon\n" +
                    "these\n" +
                    "they\n" +
                    "thick\n" +
                    "thin\n" +
                    "third\n" +
                    "this\n" +
                    "those\n" +
                    "though\n" +
                    "three\n" +
                    "through\n" +
                    "throughout\n" +
                    "thru\n" +
                    "thus\n" +
                    "to\n" +
                    "together\n" +
                    "too\n" +
                    "top\n" +
                    "toward\n" +
                    "towards\n" +
                    "twelve\n" +
                    "twenty\n" +
                    "two\n" +
                    "un\n" +
                    "under\n" +
                    "until\n" +
                    "up\n" +
                    "upon\n" +
                    "us\n" +
                    "very\n" +
                    "via\n" +
                    "was\n" +
                    "we\n" +
                    "well\n" +
                    "were\n" +
                    "what\n" +
                    "whatever\n" +
                    "when\n" +
                    "whence\n" +
                    "whenever\n" +
                    "where\n" +
                    "whereafter\n" +
                    "whereas\n" +
                    "whereby\n" +
                    "wherein\n" +
                    "whereupon\n" +
                    "wherever\n" +
                    "whether\n" +
                    "which\n" +
                    "while\n" +
                    "whither\n" +
                    "who\n" +
                    "whoever\n" +
                    "whole\n" +
                    "whom\n" +
                    "whose\n" +
                    "why\n" +
                    "will\n" +
                    "with\n" +
                    "within\n" +
                    "without\n" +
                    "would\n" +
                    "yet\n" +
                    "you\n" +
                    "your\n" +
                    "yours\n" +
                    "yourself\n" +
                    "yourselves";

            String[] stopWordsArr = s.split("\n");
//            for(String word : stopWordsArr){
//                stopWords.add(word.replaceAll("[^\\u05D0-\\u05EA]", ""));
//            }
            stopWords = Arrays.asList(stopWordsArr);
        }

        /**
         * for each ngram writes: <ngram, [partNum, OccurrencesNum]>
         * @param key - line number
         * @param value - details of ngram
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("mapper started!");
            StringTokenizer itr = new StringTokenizer(value.toString());
            int i = 0;
            Text ngram = new Text();
            boolean hasStopWord = false;
            //if w1w2 contains a stop word - don't do anything

            IntWritable occurrences = new IntWritable(0);
            IntWritable part;

            while (itr.hasMoreTokens()) {
                if(i == 0){
                    String w1 = itr.nextToken();
                    String w2 = itr.nextToken();
                    String w3 = itr.nextToken();
                    if(stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3)){
                        hasStopWord = true;
                    }
                    ngram = new Text( w1+ " " + w2 + " "+ w3);
                    i += 2;
                }else if(i == 4){
                    occurrences = new IntWritable(Integer.parseInt(itr.nextToken()));
                    break;
                }else{
                    itr.nextToken();
                }
                i++;
            }
            String[] w1w2w3 = ngram.toString().split(" "); //todo try!!!
            for (String s : w1w2w3) {
//                String wj = s.replaceAll("[^\\u05D0-\\u05EA]", "");
                if (stopWords.contains(s)) {
                    hasStopWord = true;
                    break;
                }
            }
            if (!ngram.toString().matches("^[ a-z]*$")) {
                hasStopWord = true;
            }
//            if (!ngram.toString().matches("^[\\u05D0-\\u05EA\\s]*$")) {
//                hasStopWord = true;
//            }


            if(!hasStopWord){
                if (key.get() %2 ==0) {
                    part = new IntWritable(0);
                }
                else{
                    part = new IntWritable(1);
                }

                IntWritable[] arr = {part, occurrences};
                IntArrayWritable details = new IntArrayWritable();
                details.set(arr);

                context.write(ngram, details);
                System.out.println("printed ngram:" + ngram.toString());

//                IntWritable[] NCounterArr = {occurrences};
//                IntArrayWritable arr1 =  new IntArrayWritable();
//                arr1.set(NCounterArr);
//                context.write(new Text("N"), arr1);
//                System.out.println("mapper finished!");
            }
        }
    }

    public static  class CombinerClass extends Reducer<Text,IntArrayWritable, Text,IntArrayWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntArrayWritable> values, Reducer<Text, IntArrayWritable, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException {
            String ngram = key.toString();

//            if(ngram.equals("N")){
//                int N = 0;
//                for(IntArrayWritable val : values){
//                    N += ((IntWritable)val.get()[0]).get();
//                }
//                IntWritable[] NCounterArr = {new IntWritable(N)};
//                IntArrayWritable arr1 =  new IntArrayWritable();
//                arr1.set(NCounterArr);
//                context.write(new Text("N"), arr1);
//            }else{
                int curr_r0 = 0;
                int curr_r1 = 0;

                for(IntArrayWritable currValue : values){
                    Writable[] currValueArr = currValue.get();
                    if(currValueArr != null){
                        int part = ((IntWritable)currValueArr[0]).get();
                        int occurrence = ((IntWritable)currValueArr[1]).get();
                        if (part == 0)
                            curr_r0 += occurrence;
                        else
                            curr_r1 += occurrence;
                    }else{
                        System.out.println("combiner - int arr writable value is null!");
                    }
                }
                System.out.println("combiner received ngram:" + ngram + ",r0=" + curr_r0 + ",r1=" + curr_r1);

                IntWritable[] arr = {new IntWritable(0), new IntWritable(curr_r0)};
                IntArrayWritable details = new IntArrayWritable();
                details.set(arr);
                context.write(new Text(ngram), details);
                IntWritable[] arr1 = {new IntWritable(1), new IntWritable(curr_r1)};
                IntArrayWritable details1 = new IntArrayWritable();
                details1.set(arr1);
                context.write(new Text(ngram), details1);
//            }
        }
    }
        public static class ReducerClass extends Reducer<Text,IntArrayWritable,Text,IntArrayWritable> {
            int r0 = 0;
            int r1 = 0;
            String currGram = null;
            Integer N = null;


            /**
             * for each ngram, aggregates the number of occurrences in both parts separately, and
             * writes: <ngram, [occurrencesInPart1, occurrencesInPart2, occurrencesOverall]>
             * @param key - ngram
             * @param values - [partNum, OccurrencesNum]
             */
            @Override
            protected void reduce(Text key, Iterable<IntArrayWritable> values, Reducer<Text, IntArrayWritable, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException {
                System.out.println("reducer started!");
                String ngram = key.toString();

//                if(ngram.equals("N")){
//                    if(N == null){
//                        N = 0;
//                    }
//                    for(IntArrayWritable val : values){
//                        N += ((IntWritable)val.get()[0]).get();
//                    }
//                }
//                else{
                    int curr_r0 = 0;
                    int curr_r1 = 0;

                    for(IntArrayWritable currValue : values){
                        Writable[] currValueArr = currValue.get();
                        if(currValueArr != null){
                            System.out.println("int arr writable value length:" + currValueArr.length);
                            int part = ((IntWritable)currValueArr[0]).get();
                            int occurrence = ((IntWritable)currValueArr[1]).get();
                            if (part == 0)
                                curr_r0 += occurrence;
                            else
                                curr_r1 += occurrence;
                        }else{
                            System.out.println("int arr writable value is null!");
                        }
                    }
                    System.out.println("received ngram:" + ngram + ",r0=" + curr_r0 + ",r1=" + curr_r1);

                    if(!ngram.equals(currGram)){

                        if(currGram != null){
                            System.out.println("writing currGram to context:" + currGram);
                            IntWritable[] arr = {new IntWritable(r0), new IntWritable(r1), new IntWritable(r0 + r1)};
                            IntArrayWritable arrayWritable = new IntArrayWritable();
                            arrayWritable.set(arr);
                            context.write(new Text(currGram), arrayWritable);
                        }
                        r0 = curr_r0;
                        r1 = curr_r1;
                        currGram = ngram;
                    }else{
                        r0 += curr_r0;
                        r1 += curr_r1;
                    }
                    System.out.println("reducer finished!");
//                }

            }

            /**
             * writes the last ngram's details
             */
            @Override
            protected void cleanup(Reducer<Text, IntArrayWritable, Text, IntArrayWritable>.Context context) throws IOException, InterruptedException{
                System.out.println("entered cleanup!");
                if(currGram != null){
                    System.out.println("cleanup writing last gram to context-" + currGram);
                    IntWritable[] arr = {new IntWritable(r0), new IntWritable(r1), new IntWritable(r0 + r1)};
                    IntArrayWritable arrayWritable = new IntArrayWritable();
                    arrayWritable.set(arr);
                    context.write(new Text(currGram), arrayWritable);
                }
//                if(N != null){
//                    IntWritable[] arr = {new IntWritable(N)};
//                    IntArrayWritable arrayWritable = new IntArrayWritable();
//                    arrayWritable.set(arr);
//                    context.write(new Text("N"), arrayWritable);
//                }
                super.cleanup(context);
            }
        }

        /**
         * partitions the keys so that each ngram will go to the same reducer.
         */
        public static class PartitionerClass extends Partitioner<Text, IntArrayWritable> {
            public int getPartition(Text key, IntArrayWritable values, int numOfPartitions) {
                System.out.println("patritioner got key :" + key.toString() + "and value:" + values.toString());
                return Math.abs(key.hashCode() % numOfPartitions);
            }
        }


    public static void main(String[] args) throws Exception {
        System.out.println("OccurrencesCounter started!");


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Template");
        job.setNumReduceTasks(100);
        job.setJarByClass(OccurrencesCounter.class);

        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setCombinerClass(CombinerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
