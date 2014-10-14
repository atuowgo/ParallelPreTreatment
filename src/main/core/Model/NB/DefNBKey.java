package main.core.Model.NB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-22.
 */
public class DefNBKey implements WritableComparable{
    private Text clazzIndex;
//    clazzTotalNum为文档个数
    private IntWritable clazzTotalNum;
    private IntWritable termsTotalNum;

    public DefNBKey(){
        clazzIndex = new Text();
        clazzTotalNum = new IntWritable();
        termsTotalNum = new IntWritable();
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        clazzIndex.write(out);
        clazzTotalNum.write(out);
        termsTotalNum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clazzIndex.readFields(in);
        clazzTotalNum.readFields(in);
        termsTotalNum.readFields(in);
    }

    public Text getClazzIndex() {
        return clazzIndex;
    }

    public void setClazzIndex(Text clazzIndex) {
        this.clazzIndex = clazzIndex;
    }

    public IntWritable getClazzTotalNum() {
        return clazzTotalNum;
    }

    public void setClazzTotalNum(IntWritable clazzTotalNum) {
        this.clazzTotalNum = clazzTotalNum;
    }

    public IntWritable getTermsTotalNum() {
        return termsTotalNum;
    }

    public void setTermsTotalNum(IntWritable termsTotalNum) {
        this.termsTotalNum = termsTotalNum;
    }

    @Override
    public String toString() {
        return "DefNBKey{" +
                "clazzIndex=" + clazzIndex +
                ", clazzTotalNum=" + clazzTotalNum +
                ", termsTotalNum=" + termsTotalNum +
                '}';
    }
}
