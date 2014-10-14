package main.core.ModelDumper;

import main.core.Model.NB.DefNBEvaluateValue;
import main.core.Model.Evaluate.PRModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Created by adam on 14-4-15.
 */
public class NBEvaluateDumper {

    private NBEvaluateDumper(){}

    public static DefNBEvaluateValue getNBEvaluateInstace(Path modelPath,Configuration conf) throws IOException {
        Text key = new Text();
        DefNBEvaluateValue value = new DefNBEvaluateValue();

        SequenceFile.Reader reader = new SequenceFile.Reader(modelPath.getFileSystem(conf),modelPath,conf);

        reader.next(key,value);

        return value;
    }

    public static void dumpIntroduce(String introduce,OutputStream out,boolean closeOut) throws IOException {
        strToBytes("说明:\n",out);
        strToBytes(introduce+"\n",out);
        if (closeOut)
            out.close();
    }

    public static void dumpInstace(DefNBEvaluateValue value,OutputStream out,boolean closeOut) throws IOException {
        strToBytes("NB测试的结果如下 :\n",out);
        strToBytes("测试集数目:\n",out);
        strToBytes(Arrays.toString(value.getNavieData(DefNBEvaluateValue.ArrayDataType.Test)),out);
        strToBytes("\n",out);
        strToBytes("相关类数目：\n",out);
        strToBytes(Arrays.toString(value.getNavieData(DefNBEvaluateValue.ArrayDataType.Relate)),out);
        strToBytes("\n",out);
        strToBytes("即相关又是测试结果的数目：\n",out);
        strToBytes(Arrays.toString(value.getNavieData(DefNBEvaluateValue.ArrayDataType.TestAndRelate)),out);
        strToBytes("\n",out);

        if (closeOut)
            out.close();

    }

    public static void dumpPRModel(PRModel prModel,OutputStream out,boolean closeOut) throws IOException {
        strToBytes("NB测试的效果如下：\n",out);
        strToBytes("准确率：\n",out);
        strToBytes(Arrays.toString(prModel.getPrecisionRatio()),out);
        strToBytes("\n",out);
        strToBytes("召回率：\n",out);
        strToBytes(Arrays.toString(prModel.getRecallRatio()),out);
        strToBytes("\n",out);
        strToBytes("F1测量值：\n",out);
        strToBytes(Arrays.toString(prModel.getfMeasure()),out);
        strToBytes("\n",out);

        if (closeOut)
            out.close();

    }

    private static void strToBytes(String str,OutputStream out) throws IOException {
        out.write(str.getBytes());
    }

    public static void cauculatePR(DefNBEvaluateValue value,PRModel prModel){
        int[] testNumList = value.getNavieData(DefNBEvaluateValue.ArrayDataType.Test);
        int[] relateNumList = value.getNavieData(DefNBEvaluateValue.ArrayDataType.Relate);
        int[] testAndRealteNUmList = value.getNavieData(DefNBEvaluateValue.ArrayDataType.TestAndRelate);

        int clazzNum = testNumList.length;
        prModel.setClazzNum(clazzNum);
        prModel.initData();

        double precision,recall;
        for(int i=0;i<clazzNum;i++){
            precision = prModel.getPrecisionRatio()[i] = ((double)testAndRealteNUmList[i] / relateNumList[i]);
            recall = prModel.getRecallRatio()[i] = ((double)testAndRealteNUmList[i] / testNumList[i]);

            prModel.getfMeasure()[i] = (2 * precision * recall ) / ( precision + recall);
        }
    }
}
