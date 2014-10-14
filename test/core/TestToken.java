package core;

import main.core.Model.Token.DefIDFModel;
import main.core.ModelDumper.DefFileTermsDumper;
import main.core.ModelDumper.DefIDFModelDumper;
import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by adam on 14-3-12.
 */
public class TestToken {

    @Test
    public void testStringHash(){
        String a = "abc";
        String d = "abc";
        String e = new String("abc");
        String b = "abc1";
        String c = "asdfasdfa";

        System.out.println(a.hashCode());
        System.out.println(d.hashCode());
        System.out.println(e.hashCode());
        System.out.println(b.hashCode());
        System.out.println(c.hashCode());

    }

    @Test
    public void test01() throws InterruptedException, IOException, ClassNotFoundException {
//        new TestPart().testPartition();
    }

    @Test
    public void testReadTerms() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defToken/terms/train/part-r-00000"));

        DefFileTermsDumper.dumpFromPath(path,HadoopConfUtil.getNewConfiguration());
//        FileSystem fs = path.getFileSystem(HadoopConfUtil.getNewConfiguration());
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,HadoopConfUtil.getNewConfiguration());
//
//        IntWritable key = new IntWritable();
//        DefFileTermsModel value = new DefFileTermsModel();
//        MapWritable mapWritable = new MapWritable();
//        IntWritable mapKey = new IntWritable();
//        Text mapValue = new Text();
//
//        int i = 0;
//        while(reader.next(key,value)){
//            i++;
//            System.out.println("key : "+key.get());
//            System.out.println("size : "+value.getTermsNum());
//            System.out.println(value);
//            mapWritable = value.getFileTerms();
//
//            for(Map.Entry<Writable, Writable> e : mapWritable.entrySet()){
//                System.out.println(e.getKey()+" : "+e.getValue());
//            }
//
//
//
//            System.out.println();
//            System.out.println(" -------------------- ");
//        }
//
//        System.out.println("size : "+i);
    }

    @Test
    public void testReadIdf() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defToken/idf/part-r-00000"));
        FileSystem fs = path.getFileSystem(HadoopConfUtil.getNewConfiguration());
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,HadoopConfUtil.getNewConfiguration());

        Text key = new Text();
        IntWritable value = new IntWritable();


        int i = 0;
        while(reader.next(key,value)){
            System.out.println(key+" : "+value);
            i++;
        }

        System.out.println(i);


    }

    @Test
    public void test03(){
        System.out.println(HadoopConfUtil.getHadoopHomePath());
    }

    @Test
    public void testDumpIdf() throws IOException {
        String idfString = "/ParallelPreTreatment/defToken/idfModel/00002";
        Path hdfsIdf = new Path(HadoopConfUtil.createHdfsUrl(idfString));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        DefIDFModel model = DefIDFModelDumper.getIDFInstance(hdfsIdf,conf);

        DefIDFModelDumper.dumpInstace(model,System.out,false);
    }






}
