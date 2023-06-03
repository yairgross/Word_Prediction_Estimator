import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import java.io.IOException;

public class SortRecordReader extends RecordReader<Text, FloatWritable> {
    protected LineRecordReader reader;
    protected Text key;
    protected FloatWritable value;

    public SortRecordReader(){
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            String val = reader.getCurrentValue().toString();
            System.out.println("value:" + val);
            String[] ngramAndPdel = val.split("\t");
            System.out.println("length:" + ngramAndPdel.length);
            System.out.println("val 1:" + ngramAndPdel[0]);
            key = new Text(ngramAndPdel[0]);
            value =  new FloatWritable(Float.parseFloat(ngramAndPdel[1]));

            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public FloatWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    public void close() throws IOException {
        reader.close();
    }
}
