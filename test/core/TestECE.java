package core;

import main.core.Model.Feature.DefFeatureModel;
import main.core.ModelDumper.DefECEModelDumper;
import main.core.ModelDumper.DefFeatureDumper;
import main.core.ModelDumper.ExternPathDumper;
import main.core.Util.HadoopConfUtil;
import main.core.Util.OptionsRepo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-4-17.
 */
public class TestECE {

    @Test
    public void testSumECE() throws IOException {
        String path = "/ParallelPreTreatment/tmp/defECEFeature/sumTmp/part-r-00002";
        Path testPath = new Path(HadoopConfUtil.createHdfsUrl(path));
        Configuration conf = HadoopConfUtil.getNewConfiguration();

        DefECEModelDumper.dumpECEModelFromPath(testPath,testPath.getFileSystem(conf),conf);
    }

    @Test
    public void testSetECE() throws IOException {
        String path = "/ParallelPreTreatment/tmp/defECEFeature/eceTmp/part-r-00000";
        Path testPath = new Path(HadoopConfUtil.createHdfsUrl(path));
        Configuration conf = HadoopConfUtil.getNewConfiguration();

        DefECEModelDumper.dumpECEFileFromPath(testPath,testPath.getFileSystem(conf),conf);
    }

    @Test
    public void testDump() throws IOException {
        String path = "/ParallelPreTreatment/defFeature/aggreateECEModel";
        Path srcPath = new Path(HadoopConfUtil.createHdfsUrl(path));
        Configuration conf = HadoopConfUtil.getNewConfiguration();

        DefFeatureModel model = DefFeatureDumper.getFeatureInstace(srcPath,conf);

        DefFeatureDumper.dumpFeature(model,System.out,false);
    }

    @Test
    public void testProStr() throws IOException {
        String path = "/ParallelPreTreatment/extern/termsNumPro";
        Path hdfsPath = new Path(HadoopConfUtil.createHdfsUrl(path));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = hdfsPath.getFileSystem(conf);
        Properties pro = ExternPathDumper.getPropertiesInstaceByPath(hdfsPath,fs);
        String parseStr = OptionsRepo.createOptionsFromPro(pro);
        System.out.println(parseStr);
        Map<String,String> optionsMap = OptionsRepo.parseOptionsValue(parseStr);

        for(Map.Entry<String,String> e : optionsMap.entrySet()){
            System.out.println(e.getKey()+" : "+e.getValue());
        }
    }
}
