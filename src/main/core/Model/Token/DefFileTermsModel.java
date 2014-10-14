package main.core.Model.Token;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-14.
 */
public class DefFileTermsModel implements Writable {
    private Text fileName;
    private Text clazzName;
    private IntWritable termsNum;
    private MapWritable fileTerms;

    public DefFileTermsModel(){
        fileName = new Text();
        clazzName = new Text();
        fileTerms = new MapWritable();
        this.termsNum = new IntWritable();
    }

    public DefFileTermsModel(Text clazzName, Text fileName, IntWritable termsNum, MapWritable fileTerms) {
        this.clazzName = clazzName;
        this.fileName = fileName;
        this.termsNum = termsNum;
        this.fileTerms = fileTerms;
    }

    public Text getFileName() {
        return fileName;
    }

    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }

    public Text getClazzName() {
        return clazzName;
    }

    public void setClazzName(Text clazzName) {
        this.clazzName = clazzName;
    }

    public IntWritable getTermsNum() {
        return termsNum;
    }

    public void setTermsNum(IntWritable termsNum) {
        this.termsNum = termsNum;
    }

    public MapWritable getFileTerms() {
        return fileTerms;
    }

    public void setFileTerms(MapWritable fileTerms) {
        this.fileTerms = fileTerms;
    }

    @Override
    public String toString() {
        return "DefFileTermsModel{" +
                "fileName=" + fileName +
                ", clazzName=" + clazzName +
                ", fileTerms=" + fileTerms +
                ", termsNum=" + termsNum +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        fileName.write(dataOutput);
        clazzName.write(dataOutput);
        termsNum.write(dataOutput);
        fileTerms.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fileName.readFields(dataInput);
        clazzName.readFields(dataInput);
        termsNum.readFields(dataInput);
        fileTerms.readFields(dataInput);
    }
}
