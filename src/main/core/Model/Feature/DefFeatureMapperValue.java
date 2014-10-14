package main.core.Model.Feature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-22.
 */
public class DefFeatureMapperValue  implements Writable{
    private Text termName;
    private IntWritable idf;

    public DefFeatureMapperValue() {
        termName = new Text();
        idf = new IntWritable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        termName.write(out);
        idf.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        termName.readFields(in);
        idf.readFields(in);
    }

    public Text getTermName() {
        return termName;
    }

    public void setTermName(Text termName) {
        this.termName = termName;
    }

    public IntWritable getIdf() {
        return idf;
    }

    public void setIdf(IntWritable idf) {
        this.idf = idf;
    }

    @Override
    public String toString() {
        return "DefFeatureMapperValue{" +
                "termName=" + termName +
                ", idf=" + idf +
                '}';
    }
}
