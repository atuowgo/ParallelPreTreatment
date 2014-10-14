package view;

import main.core.ModelDumper.*;
import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by adam on 14-5-1.
 */
public class testDump {
    private static Configuration conf;
    private FileSystem fs;

    @BeforeClass
    public static void beforeClass(){
        conf = HadoopConfUtil.getNewConfiguration();
    }

    @Test
    public void testDumpClean() throws IOException {
        String srcPath = "/ParallelPreTreatment/defClean/train/00000";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefHtmlModelDumper.dumpModelFromPath(hdfsSrcPath,conf);
    }

    @Test
    public void testDumpFileTerms() throws IOException {
        String srcPath = "/ParallelPreTreatment/defToken/terms/train/part-r-00000";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefFileTermsDumper.dumpFromPath(hdfsSrcPath,conf);
    }

    @Test
    public void testDumpIdf() throws IOException {
        String srcPath = "/ParallelPreTreatment/defToken/idf/part-r-00000";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefIDFModelDumper.dumpFromPath(hdfsSrcPath,conf);
    }

    @Test
    public void testDumpFeature() throws IOException {
        String srcPath = "/ParallelPreTreatment/defFeature/aggreateECEModel";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefFeatureDumper.dumpFromPath(hdfsSrcPath,conf);
    }

    @Test
    public void testDumpMatrix() throws IOException {
        String srcPath = "/ParallelPreTreatment/defMatrix/train/part-r-00000";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefMatrixDumper.dumpFromPath(hdfsSrcPath,conf);
    }

    @Test
    public void testDumpNBModel() throws IOException {
        String srcPath = "/ParallelPreTreatment/defNB/model/part-r-00000";
        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));

        DefNBDumper.dumpModelFromPath(hdfsSrcPath,conf);
    }

//    @Test
//    public void testDumpNBEvaluate(){
//        String srcPath = "/ParallelPreTreatment/defNB/evaluate/evaluateModel";
//        Path hdfsSrcPath = new Path(HadoopConfUtil.createHdfsUrl(srcPath));
//
//
//    }

    @AfterClass
    public static void afterClass(){
        conf = null;
    }
}
