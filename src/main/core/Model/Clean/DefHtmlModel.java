package main.core.Model.Clean;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-3-9.
 */
public class DefHtmlModel implements Writable{
    private Text clazz;
    private Text fileName;
    private Text content;

    public DefHtmlModel(){
        clazz = new Text();
        fileName = new Text();
        content = new Text();
    }

    public DefHtmlModel(String clazz,String fileName,String content){
        this.clazz = new Text(clazz);
        this.fileName = new Text(fileName);
        this.content = new Text(content);
    }

    public DefHtmlModel(Text clazz, Text fileName, Text content) {
        this.clazz = clazz;
        this.fileName = fileName;
        this.content = content;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        clazz.write(dataOutput);
        fileName.write(dataOutput);
        content.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clazz.readFields(dataInput);
        fileName.readFields(dataInput);
        content.readFields(dataInput);
    }

    @Override
    public String toString() {

        return "DefHtmlModel{" +
                "clazz=" + clazz +
                ", fileName=" + fileName +
                ", content=" + content +
                '}';
    }

    public Text getClazz() {
        return clazz;
    }

    public void setClazz(Text clazz) {
        this.clazz = clazz;
    }

    public Text getFileName() {
        return fileName;
    }

    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }

    public Text getContent() {
        return content;
    }

    public void setContent(Text content) {
        this.content = content;
    }
}
