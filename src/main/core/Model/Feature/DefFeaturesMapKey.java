package main.core.Model.Feature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-22.
 */
public class DefFeaturesMapKey implements Writable{
    private IntWritable termIndex;
    private IntWritable idf;

    public DefFeaturesMapKey(){
        termIndex = new IntWritable();
        idf = new IntWritable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        termIndex.write(out);
        idf.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        termIndex.readFields(in);
        idf.readFields(in);
    }

    public IntWritable getTermIndex() {
        return termIndex;
    }

    public void setTermIndex(IntWritable termIndex) {
        this.termIndex = termIndex;
    }

    public IntWritable getIdf() {
        return idf;
    }

    public void setIdf(IntWritable idf) {
        this.idf = idf;
    }

    @Override
    public String toString() {
        return "DefFeaturesMapKey{" +
                "termIndex=" + termIndex +
                ", idf=" + idf +
                '}';
    }
}
