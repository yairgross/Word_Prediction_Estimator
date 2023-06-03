import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;

public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public String toString() {
        return Arrays.asList(toStrings()).toString().replace("]","").replace("[","")
                .replace(" ",""); //todo check
    }


}
