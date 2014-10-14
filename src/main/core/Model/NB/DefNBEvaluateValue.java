package main.core.Model.NB;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by adam on 14-4-15.
 */
public class DefNBEvaluateValue implements Writable {
    private IntWritable clazzNum;
    private ArrayWritable testNumArray;
    private ArrayWritable relateNumArray;
    private ArrayWritable testAndRelateNumArray;

    public DefNBEvaluateValue() {
        clazzNum = new IntWritable();
        testNumArray = new ArrayWritable(IntWritable.class);
        relateNumArray = new ArrayWritable(IntWritable.class);
        testAndRelateNumArray = new ArrayWritable(IntWritable.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        clazzNum.write(out);
        testNumArray.write(out);
        relateNumArray.write(out);
        testAndRelateNumArray.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clazzNum.readFields(in);
        testNumArray.readFields(in);
        relateNumArray.readFields(in);
        testAndRelateNumArray.readFields(in);
    }

    public IntWritable getClazzNum() {
        return clazzNum;
    }

    public void setClazzNum(IntWritable clazzNum) {
        this.clazzNum = clazzNum;
    }

    public void setArrayData(ArrayWritable list,int datas[]){
        IntWritable writableDatas[] = new IntWritable[datas.length];

        for(int i=0;i<datas.length;i++){
            if (writableDatas[i] == null)
                writableDatas[i] = new IntWritable();
            writableDatas[i].set(datas[i]);
        }

        list.set(writableDatas);
    }

    public ArrayWritable getArrayData(ArrayDataType type){
        switch (type){
            case Test:
                return this.testNumArray;
            case Relate:
                return this.relateNumArray;
            case TestAndRelate:
                return this.testAndRelateNumArray;
        }

        return null;
    }

    public int[] getNavieData(ArrayDataType type){

        switch (type){
            case Test:
                return translateToInt(testNumArray.get());
            case Relate:
                return translateToInt(relateNumArray.get());
            case TestAndRelate:
                return translateToInt(testAndRelateNumArray.get());
        }

        return null;

    }

    private int[] translateToInt(Writable writableDatas[]){
        int[] datas = new int[writableDatas.length];
        IntWritable intWritable = null;
        for(int i=0;i<writableDatas.length;i++){
            intWritable = (IntWritable) writableDatas[i];
            datas[i] = intWritable.get();
        }
        return datas;
    }

    public static enum ArrayDataType{
        Test,Relate,TestAndRelate
    }
}
