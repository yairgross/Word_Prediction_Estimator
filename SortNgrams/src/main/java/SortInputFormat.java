import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class SortInputFormat extends FileInputFormat<Text, FloatWritable> {
    @Override
    public RecordReader<Text, FloatWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new SortRecordReader();
    }




}
