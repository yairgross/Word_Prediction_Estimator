import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;

public class NgramRecordReader extends RecordReader<Text,IntArrayWritable> {

    protected LineRecordReader reader;
    protected Text key;
    protected IntArrayWritable value;

    NgramRecordReader() {
        reader = new LineRecordReader();
        key = null;
        value = null;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }


    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (reader.nextKeyValue()) {
            String val = reader.getCurrentValue().toString();
            String[] ngramAndr = val.split("\t");
            key = new Text(ngramAndr[0]);
            String[] rs =  ngramAndr[1].split(",");
            IntWritable[] arr = new IntWritable[rs.length];
            for(int i = 0; i < arr.length; i++){
                arr[i] = new IntWritable(Integer.parseInt(rs[i]));
            }
            value = new IntArrayWritable();
            value.set(arr);
            return true;
        } else {
            key = null;
            value = null;
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public IntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
}
