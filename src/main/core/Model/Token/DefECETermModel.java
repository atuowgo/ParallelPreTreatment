package main.core.Model.Token;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-4-16.
 */
public class DefECETermModel implements Writable {
    private IntWritable clazzIndex;
    private IntWritable termsNum;
//    单个类中的比例
    private DoubleWritable termsClassRatio;
//    全部数据集的比例
//    private DoubleWritable termsDocPatio;
//    private DoubleWritable termsECE;

    public DefECETermModel(){
        clazzIndex = new IntWritable();
        termsNum = new IntWritable();
        termsClassRatio = new DoubleWritable();
//        termsDocPatio = new DoubleWritable();
//        termsECE = new DoubleWritable();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        clazzIndex.write(out);
        termsNum.write(out);
        termsClassRatio.write(out);
//        termsDocPatio.write(out);
//        termsECE.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clazzIndex.readFields(in);
        termsNum.readFields(in);
        termsClassRatio.readFields(in);
//        termsDocPatio.readFields(in);
//        termsECE.readFields(in);
    }

    public IntWritable getClazzIndex() {
        return clazzIndex;
    }

    public void setClazzIndex(IntWritable clazzIndex) {
        this.clazzIndex = clazzIndex;
    }

    public IntWritable getTermsNum() {
        return termsNum;
    }

    public void setTermsNum(IntWritable termsNum) {
        this.termsNum = termsNum;
    }

    public DoubleWritable getTermsClassRatio() {
        return termsClassRatio;
    }

    public void setTermsClassRatio(DoubleWritable termsClassRatio) {
        this.termsClassRatio = termsClassRatio;
    }

//    public DoubleWritable getTermsDocPatio() {
//        return termsDocPatio;
//    }
//
//    public void setTermsDocPatio(DoubleWritable termsDocPatio) {
//        this.termsDocPatio = termsDocPatio;
//    }
//
//    public DoubleWritable getTermsECE() {
//        return termsECE;
//    }
//
//    public void setTermsECE(DoubleWritable termsECE) {
//        this.termsECE = termsECE;
//    }


    @Override
    public String toString() {
        return "DefECETermModel{" +
                "clazzIndex=" + clazzIndex +
                ", termsNum=" + termsNum +
                ", termsClassRatio=" + termsClassRatio +
                '}';
    }
}
