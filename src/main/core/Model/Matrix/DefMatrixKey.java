package main.core.Model.Matrix;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-21.
 */
public class DefMatrixKey implements WritableComparable{
    private IntWritable clazzIndex;
    private Text fileName;
    private IntWritable fileTermsNum;

    public DefMatrixKey() {
        clazzIndex = new IntWritable();
        fileName = new Text();
        fileTermsNum = new IntWritable();
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        clazzIndex.write(out);
        fileName.write(out);
        fileTermsNum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clazzIndex.readFields(in);
        fileName.readFields(in);
        fileTermsNum.readFields(in);
    }

    public IntWritable getClazzIndex() {
        return clazzIndex;
    }

    public void setClazzIndex(IntWritable clazzIndex) {
        this.clazzIndex = clazzIndex;
    }

    public Text getFileName() {
        return fileName;
    }

    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }

    public IntWritable getFileTermsNum() {
        return fileTermsNum;
    }

    public void setFileTermsNum(IntWritable fileTermsNum) {
        this.fileTermsNum = fileTermsNum;
    }

    @Override
    public String toString() {
        return "DefMatrixKey{" +
                "clazzIndex=" + clazzIndex +
                ", fileName=" + fileName +
                ", fileTermsNum=" + fileTermsNum +
                '}';
    }
}
