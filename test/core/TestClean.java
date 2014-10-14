package core;

import junit.framework.Assert;
import main.core.Job.Default.Clean.DefHtmlCleanJob;
import main.core.Model.Clean.DefHtmlModel;
import main.core.ModelDumper.DefHtmlModelDumper;
import main.core.Treatment.CleanPathFilter;
import main.core.Treatment.Default.DefHtmlTreater;
import main.core.Treatment.HtmlTreater;
import main.core.Util.HadoopConfUtil;
import main.core.Util.JobUtil;
import main.core.Util.LabelProUtil;
import main.core.Util.OptionsRepo;
import main.framework.Options.Default.DefHtmlCleanOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-3-9.
 */
public class TestClean {

    @Test
    public void testParseHtml(){
        String t = "<p>asafsdfa<a>sssss</a><p>adas</p>";
        Document doc = Jsoup.parse(t);
        System.out.println(doc.text());
    }

    @Test
    public void testModel() throws IOException {
        String t = "/home/adam/dev/tmp/t.html";

        HtmlTreater treater = DefHtmlTreater.getInstance();
        DefHtmlModel model = new DefHtmlModel();
        Document doc = null;
        treater.parseHtml(t,"a","t1.html",model);
        System.out.println(model);
    }

    @Test
    public void testJob(){
        DefHtmlCleanJob cleanJob = new DefHtmlCleanJob();
//        cleanJob.startJob();
    }

    @Test
    public void testOptionsValue(){
        DefHtmlCleanOptions options = new DefHtmlCleanOptions();
        options.setTaskName("clean");
        options.setInputPath("Iinput");
        options.setTmpPath("Itmp");
        options.setOutputPath("Ioutput");
        options.setHtmlTreater("Itreater");

        System.out.println(options.optionsValue());

        OptionsRepo.parseOptionsValue(options.optionsValue());

    }

    @Test
    public void testReadModel() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defClean/train/00000"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();

        DefHtmlModelDumper.dumpModelFromPath(path,conf);
//        SequenceFile.Reader reader = new SequenceFile.Reader(path.getFileSystem(HadoopConfUtil.getNewConfiguration()),
//                path,HadoopConfUtil.getNewConfiguration());
//        IntWritable fileName = new IntWritable();
//        DefHtmlModel model = new DefHtmlModel();
//
//        while (reader.next(fileName,model)){
//            System.out.println(fileName+" : "+model);
//        }
    }

    @Test
    public void testPro() throws IOException {
        Path p = new Path(HadoopConfUtil.createHdfsUrl
                ("/ParallelPreTreatment/extern/labelList"));
        FileSystem fs = p.getFileSystem(HadoopConfUtil.getNewConfiguration());
        FSDataOutputStream out = fs.create(p);
        Properties clazzPro = new Properties();
        clazzPro.setProperty("0","asda");
        clazzPro.store(out,"labelList");

        out.close();
    }

    @Test
    public void testParseLabelNum(){
        int a = 0,b = 1, c = 10;

        Assert.assertEquals(LabelProUtil.parseLabelStr(a),"00000");
        Assert.assertEquals(LabelProUtil.parseLabelStr(b),"00001");
        Assert.assertEquals(LabelProUtil.parseLabelStr(c),"00010");
    }

    @Test
    public void testChangeName() throws IOException {
        Path p = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defClean/arts1"));
        FileSystem fileSystem = p.getFileSystem(HadoopConfUtil.getNewConfiguration()
        );

        long d = System.currentTimeMillis();
        fileSystem.rename(p,new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defClean/arts")));
        System.out.println(System.currentTimeMillis() - d);
    }

    @Test
    public void testRemovePath() throws IOException {
        Path input = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defToken/terms"));
        FileSystem fs = input.getFileSystem(HadoopConfUtil.getNewConfiguration());

//        JobUtil.cleanPath(input,fs,new CleanPathFilter());
        assert(JobUtil.getClazzNum(input,fs,new CleanPathFilter()) == 3);
    }
}
