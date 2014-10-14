package main.core.Model.Feature;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-19.
 */
public class DefFeatureModel implements Writable{
   private IntWritable featuresNum;
   private MapWritable featuresMap;
//    4-20
   private MapWritable featuresIdf;

    public DefFeatureModel(){
        featuresNum = new IntWritable();
        featuresMap = new MapWritable();
        featuresIdf = new MapWritable();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        featuresNum.write(out);
        featuresMap.write(out);
        //    4-20
        featuresIdf.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        featuresNum.readFields(in);
        featuresMap.readFields(in);
        //    4-20
        featuresIdf.readFields(in);
    }

    public IntWritable getFeaturesNum() {
        return featuresNum;
    }

    public void setFeaturesNum(IntWritable featuresNum) {
        this.featuresNum = featuresNum;
    }

    public MapWritable getFeaturesMap() {
        return featuresMap;
    }

    public void setFeaturesMap(MapWritable featuresMap) {
        this.featuresMap = featuresMap;
    }

    public MapWritable getFeaturesIdf() {
        return featuresIdf;
    }

    public void setFeaturesIdf(MapWritable featuresIdf) {
        this.featuresIdf = featuresIdf;
    }

    @Override
    public String toString() {
        return "DefFeatureModel{" +
                "featuresNum=" + featuresNum +
                '}';
    }
}
