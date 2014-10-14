package main.core.Model.NB;

import main.core.Util.LabelProUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by adam on 14-3-24.
 */
public class DefNBAggreateModel implements Writable{
    private double[] percentageList;
    private Vector[] NBModelList;
    private int clazzNum;
    private int[] itemsList;

    private DefNBAggreateModel(){}

    private DefNBAggreateModel(int clazzNum){
        this.clazzNum = clazzNum;
        percentageList = new double[clazzNum];
        NBModelList = new Vector[clazzNum];
        itemsList = new int[clazzNum];
    }

    public static DefNBAggreateModel getInstanceFromPath(Path percentagePath,Path modelPath,
                                     Configuration conf,FileSystem fileSystem) throws IOException {
//        加载类比例文件
        Properties labelPro = new Properties();
        FSDataInputStream labelIn = fileSystem.open(percentagePath);
        labelPro.load(labelIn);

//        整合NB分类器模型

        FileStatus[] modelStatus = fileSystem.listStatus(modelPath);
        SequenceFile.Reader reader;

        int clazzNum = modelStatus.length;
        DefNBAggreateModel model = new DefNBAggreateModel(clazzNum);
        model.setClazzNum(clazzNum);

        int i = 0;
        double[] perList = new double[clazzNum];
        Vector[] vectors = new Vector[clazzNum];
        int[] items = new int[clazzNum];

        for(FileStatus f : modelStatus){
            DefNBKey key = new DefNBKey();
            VectorWritable value = new VectorWritable();
            reader = new SequenceFile.Reader(fileSystem,f.getPath(),conf);
            assert(reader!=null);
            reader.next(key,value);

            double d = Double.parseDouble(labelPro.getProperty(LabelProUtil.parseLabelStr(i)));
            perList[i] = d;
            vectors[i] = value.get();
            items[i] = key.getTermsTotalNum().get();
            reader.close();
            i++;
        }

        model.setPercentageList(perList);
        model.setNBModelList(vectors);
        return model;
    }

    public static void persistModel(Path persistPath,FileSystem fileSystem,Configuration conf,DefNBAggreateModel model)
            throws IOException {
        SequenceFile.Writer writer =
                new SequenceFile.Writer(fileSystem,conf,persistPath, Text.class,DefNBAggreateModel.class);

//        assert(writer!=null);
        writer.append(new Text("NBAggreateModel"),model);

        writer.close();
    }

    public double[] getPercentageList() {
        return percentageList;
    }

    public void setPercentageList(double[] percentageList) {
        this.percentageList = percentageList;
    }

    public Vector[] getNBModelList() {
        return NBModelList;
    }

    public void setNBModelList(Vector[] NBModelList) {
        this.NBModelList = NBModelList;
    }

    public int getClazzNum() {
        return clazzNum;
    }

    public void setClazzNum(int clazzNum) {
        this.clazzNum = clazzNum;
    }

    public int[] getItemsList() {
        return itemsList;
    }

    public void setItemsList(int[] itemsList) {
        this.itemsList = itemsList;
    }

    @Override
    public String toString() {
        return "DefNBAggreateModel{\n" +
                "itermsList="+Arrays.toString(itemsList)+
                ",\npercentageList=" + Arrays.toString(percentageList) +
                ", \nNBModelList=" + Arrays.toString(NBModelList) +
                ", \nclazzNum=" + clazzNum +
                "\n}";
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
