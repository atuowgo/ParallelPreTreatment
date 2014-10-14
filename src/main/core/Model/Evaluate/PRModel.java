package main.core.Model.Evaluate;

/**
 * Created by adam on 14-4-15.
 */
public class PRModel {
    private int clazzNum;
    private double recallRatio[];
    private double precisionRatio[];
    private double fMeasure[];

    public PRModel(){

    }

    public PRModel(int clazzNum){
        this.clazzNum = clazzNum;
        recallRatio = new double[clazzNum];
        precisionRatio = new double[clazzNum];
        fMeasure = new double[clazzNum];
    }

    public int getClazzNum() {
        return clazzNum;
    }

    public void setClazzNum(int clazzNum) {
        this.clazzNum = clazzNum;

    }

    public void initData(){
        recallRatio = new double[clazzNum];
        precisionRatio = new double[clazzNum];
        fMeasure = new double[clazzNum];
    }

    public double[] getRecallRatio() {
        return recallRatio;
    }

    public void setRecallRatio(double[] recallRatio) {
        this.recallRatio = recallRatio;
    }

    public double[] getPrecisionRatio() {
        return precisionRatio;
    }

    public void setPrecisionRatio(double[] precisionRatio) {
        this.precisionRatio = precisionRatio;
    }

    public double[] getfMeasure() {
        return fMeasure;
    }

    public void setfMeasure(double[] fMeasure) {
        this.fMeasure = fMeasure;
    }
}
