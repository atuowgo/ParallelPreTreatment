package core;

import main.core.Model.Feature.DefFeatureModel;
import main.core.Model.Feature.DefFeaturesMapKey;
import main.core.Util.HadoopConfUtil;
import main.core.Util.LabelProUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by adam on 14-3-16.
 */
public class TestDF {

    @Test
    public void test01(){
        String str1 = "00000";
        String str2 = "00001";
        String str3 = "00012";
        String str4 = "00123";

        assert(LabelProUtil.reParseLableInt(str1)==0);
        assert(LabelProUtil.reParseLableInt(str2)==1);
        assert(LabelProUtil.reParseLableInt(str3)==12);
        assert(LabelProUtil.reParseLableInt(str4)==123);
    }

    @Test
    public void test02(){
        String p = "part-r-00000";

        int ch = p.lastIndexOf("-");
        p = p.substring(++ch);

        System.out.println(p);
    }

    @Test
    public void testParseLable(){
        String fileName = "00001-asdfasfasdfasdfafasdf";

        System.out.println(LabelProUtil.parseDFLable(fileName,"-"));
        System.out.println(LabelProUtil.parseDFPartitioner(fileName,"-"));
    }

    @Test
    public void readFeture() throws IOException {
        String path = "/ParallelPreTreatment/defDFFeture/part-r-00000";
        Path p = new Path(HadoopConfUtil.createHdfsUrl(path));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        SequenceFile.Reader reader = new SequenceFile.Reader(
                p.getFileSystem(conf),p,conf
        );

        Text key = new Text();
        DoubleWritable value = new DoubleWritable();

        int i = 0;
        while(reader.next(key,value)){
            System.out.println(key.toString()+" : "+value.get());
            i++;
        }

        System.out.println(i++);
    }

    @Test
    public void testDFValue() throws IOException {
        String patString = "/ParallelPreTreatment/defFeature/aggreateCECModel";
//        String patString = "/ParallelPreTreatment/defDFFeture/aggreDFModel";
        Path p = new Path(HadoopConfUtil.createHdfsUrl(patString));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = p.getFileSystem(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,p,conf);

        Text key = new Text();
        DefFeatureModel value = new DefFeatureModel();
        MapWritable mapWritable = new MapWritable();

        boolean next = reader.next(key,value);
//        assert(next==false);
        mapWritable = value.getFeaturesMap();
        int[] docVector = new int[value.getFeaturesNum().get()];
        IntWritable index = new IntWritable();

//        for(int i=0;i<docVector.length;i++){
//            docVector[i] = 0;
//        }

        for(Writable k : mapWritable.keySet()){
            System.out.println(k+" : "+mapWritable.get(k));
            index = (IntWritable) mapWritable.get(k);
//            System.out.println("------------------------------------------------------------"+index.get());

//            if (docVector[index.get()]==0){
//                docVector[index.get()] = 1;
//            }else {
//                System.out.println("------------------------------------------------------------"+index.get());
//            }
        }

        System.out.println("featuresNum : "+value.getFeaturesNum());
//        System.out.println("-----------------------------");
//        for(double d : docVector){
//            System.out.print(d+" ");
//            assert(d==1);
//        }



    }

    @Test
    public void testReadFeatureNew() throws IOException {
        Path p = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defDFFeture/part-r-00000"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = p.getFileSystem(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,p,conf);

        Text numKey = new Text();
        DefFeatureModel mapModel = new DefFeatureModel();
        MapWritable mapValue = new MapWritable();

        reader.next(numKey,mapModel);

        System.out.println("num : "+numKey);

        Text mapTermKey = new Text();

        mapValue = mapModel.getFeaturesMap();

        for(Writable w : mapValue.keySet()){
            DefFeaturesMapKey mapFeatureMapValue = new DefFeaturesMapKey();
            mapTermKey = (Text) w;
            mapFeatureMapValue = (DefFeaturesMapKey) mapValue.get(mapTermKey);

            System.out.println("key : "+mapTermKey+" : value : "+mapFeatureMapValue);
        }
        System.out.println("size : "+mapModel.getFeaturesNum());
    }

    @Test
    public void testSingleDF(){
        System.out.println(Integer.MAX_VALUE);
    }





}
