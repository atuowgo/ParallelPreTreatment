package core;

import main.core.Model.Matrix.DefMatrixKey;
import main.core.Model.NB.DefNBAggreateModel;
import main.core.Model.NB.DefNBEvaluateValue;
import main.core.Model.Evaluate.PRModel;
import main.core.ModelDumper.ExternPathDumper;
import main.core.ModelDumper.NBEvaluateDumper;
import main.core.Treatment.DeletePathFilter;
import main.core.Treatment.NBTargetClazzTreater;
import main.core.Util.HadoopConfUtil;
import main.core.Util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by adam on 14-3-26.
 */
public class TestEvaluate {

    @Test
    public void testNBEvaluate01() throws IOException {
        Path modelPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/model"));
        Path percentagePath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/percentagePro"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = modelPath.getFileSystem(conf);

        DefNBAggreateModel model = DefNBAggreateModel.getInstanceFromPath(percentagePath,modelPath,conf,fs);

        System.out.println(model);

        Path testPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defMatrix/test/part-r-00000"));

        DefMatrixKey key = new DefMatrixKey();
        VectorWritable value = new VectorWritable();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,testPath,conf);

        while (reader.next(key,value)){
            int  r = NBTargetClazzTreater.findTargetClazz(value.get(), model);
            System.out.println(key+" \n "+r);
        }
    }

    @Test
    public void testPath(){
        Path modelPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/model"));
        Path percentagePath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/percentagePro"));

        System.out.println(modelPath.toString());
    }

    @Test
    public void testArray(){
        int list[] = {4,3,5};

        list[2]++;

        System.out.println(Arrays.toString(list));
    }

    @Test
    public void testOut() throws IOException {
        OutputStream out = System.out;
        OutputStream out2 = new FileOutputStream("/home/adam/dev/tmp/testOut.txt");

        out.write("hello".getBytes());
        out2.write("hello".getBytes());
        out2.close();
    }

    @Test
    public void testDump() throws IOException {
        Path modelPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/evaluate/evaluateModel"));
        System.out.println(modelPath);
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = modelPath.getFileSystem(conf);
//        fs.rename(modelPath,new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/evaluate/evaluateModel")));
        DefNBEvaluateValue value = NBEvaluateDumper.getNBEvaluateInstace(modelPath,conf);

        PRModel prModel = new PRModel();
        NBEvaluateDumper.cauculatePR(value,prModel);

        NBEvaluateDumper.dumpInstace(value,System.out,false);
        NBEvaluateDumper.dumpPRModel(prModel,System.out,false);

        String localOutputPath = "/home/adam/dev/tmp/mahoutLocalOut";
        String markStr = "defNB__8000";
        String uuid = UUID.randomUUID().toString();
        localOutputPath += "/"+uuid;
        String introducePath = localOutputPath + "/"+markStr;
        String instanceOutPath = localOutputPath + "/evaluateResulte.txt";
        String prOutPath = localOutputPath + "/prResulte.txt";

        FileSystem localFileSystem = FileSystem.getLocal(conf);
        FSDataOutputStream introduceOut = localFileSystem.create(new Path(introducePath));
        FSDataOutputStream instanceOut = localFileSystem.create(new Path(instanceOutPath));
        FSDataOutputStream prOut = localFileSystem.create(new Path(prOutPath));

        NBEvaluateDumper.dumpIntroduce(markStr,introduceOut,true);
        NBEvaluateDumper.dumpInstace(value, instanceOut, true);
        NBEvaluateDumper.dumpPRModel(prModel, prOut, true);
        System.out.println("输出路径为 ： " + localOutputPath);

        String pathToDel = "/ParallelPreTreatment";
        Path hdfsDelPath = new Path(HadoopConfUtil.createHdfsUrl(pathToDel));
        PathFilter delFilter = new DeletePathFilter();
        JobUtil.cleanPath(hdfsDelPath,fs,delFilter);
    }
      
    @Test
    public void testDelete() throws IOException {
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        String pathToDel = "/ParallelPreTreatment";
        Path hdfsDelPath = new Path(HadoopConfUtil.createHdfsUrl(pathToDel));
        FileSystem fs = hdfsDelPath.getFileSystem(conf);
        PathFilter delFilter = new DeletePathFilter();
        JobUtil.cleanPath(hdfsDelPath.getParent(),fs,delFilter);
    }

    @Test
    public void testDumpPro() throws IOException {
        Path testPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/percentagePro"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = testPath.getFileSystem(conf);
        Properties properties = ExternPathDumper.getPropertiesInstaceByPath(testPath,fs);
        ExternPathDumper.dumpTestNumPro(properties,System.out,3,false);
    }

    @Test
    public void testUUID(){
        UUID uuid = UUID.randomUUID();
//        UUID uuid1 = UUID.fromString("nb");
        System.out.println(uuid);
    }


}
