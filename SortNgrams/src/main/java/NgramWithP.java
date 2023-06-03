
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NgramWithP implements WritableComparable<NgramWithP> {
    protected float pdel;
    protected String w1w2;

    public NgramWithP(String w1w2, float pdel){
        this.pdel = pdel;
        this.w1w2 = w1w2;
    }
    public float getPdel(){
        return this.pdel;
    }
    public String getW1w2(){
        return this.w1w2;
    }


    public int compareTo(NgramWithP other) {
        if(w1w2.compareTo(other.getW1w2()) == 0){
            return Float.compare(other.getPdel(), this.getPdel());
        }
        return w1w2.compareTo(other.getW1w2());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(w1w2);
        dataOutput.writeFloat(pdel);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.w1w2 = dataInput.readUTF();
        this.pdel = dataInput.readFloat();
    }
}
