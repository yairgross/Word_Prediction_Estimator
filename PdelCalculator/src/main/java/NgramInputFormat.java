
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


public class NgramInputFormat extends FileInputFormat<Text, IntArrayWritable>{
    @Override
    public RecordReader<Text, IntArrayWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new NgramRecordReader();
    }
}


