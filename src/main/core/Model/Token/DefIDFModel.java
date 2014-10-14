package main.core.Model.Token;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-16.
 */
public class DefIDFModel implements Writable{
    private IntWritable clazzIndex;
    private MapWritable termsIDF;

    public DefIDFModel(){
        clazzIndex = new IntWritable();
        this.termsIDF = new MapWritable();
    }



    @Override
    public void write(DataOutput dataOutput) throws IOException {
        clazzIndex.write(dataOutput);
        termsIDF.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clazzIndex.readFields(dataInput);
        termsIDF.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "IDFModel{" +
                "clazzName=" + clazzIndex +
                '}';
    }

    public IntWritable getClazzIndex() {
        return clazzIndex;
    }

    public void setClazzIndex(IntWritable clazzIndex) {
        this.clazzIndex = clazzIndex;
    }

    public MapWritable getTermsIDF() {
        return termsIDF;
    }

    public void setTermsIDF(MapWritable termsIDF) {
        this.termsIDF = termsIDF;
    }
}
