package main.core.Job.Default.Clean;

import main.core.Job.TaskJob;
import main.core.Model.Clean.DefHtmlModel;
import main.core.Treatment.Default.DefHtmlTreater;
import main.core.Treatment.HtmlTreater;
import main.core.Util.HadoopConfUtil;
import main.core.Util.LabelProUtil;
import main.core.Util.LogUtil;
import main.core.Util.OptionsRepo;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-3-10.
 */
public class DefHtmlCleanJob implements TaskJob{
    private static volatile int  taskNum = 0;
    private Map<String,String> optionsValueMap;
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();
    private Integer[] clazzNumList;
    private Integer[] testNumList;
    private float trainPecentage;

    @Override
    public boolean startJob(String value) throws InterruptedException {
        boolean resu = true;

        Properties labelPro = new Properties();
        //            更改 2014 - 3 - 23
        Properties testNumPro = new Properties();
        Properties percentagePro = new Properties();
        optionsValueMap = OptionsRepo.parseOptionsValue(value);
//        System.out.println(optionsValueMap.get("inputPath"));

        trainPecentage = Float.parseFloat(optionsValueMap.get("trainPecentage"));
        System.out.println("traniPecentage : "+trainPecentage);
        trainPecentage *= 0.01;
        File inputPath = new File(optionsValueMap.get("inputPath"));
        String[] clazz = inputPath.list();
        int clazzNum = clazz.length;
        consoleLog.info("清洗阶段，共收有 "+clazzNum+" 类");
        clazzNumList = new Integer[clazzNum];
        testNumList = new Integer[clazzNum];
        for(int i=0;i<clazzNum;i++){
            System.err.println(clazz[i] + " : " + i);
            cleaner threadCleaner = new cleaner(optionsValueMap,clazz[i],clazzNum,i,clazzNumList,trainPecentage);

//            更改 2014 - 3 - 23
            threadCleaner.setTestNumList(testNumList);
            Thread thread = new Thread(threadCleaner);

            thread.start();
        }

        while (taskNum!=clazzNum){
            if (taskNum<0){
                resu = false;
                break;
            }
            Thread.sleep(300);
        }

        if (resu){
            Path labelPath = new Path(HadoopConfUtil.createHdfsUrl(
                    "/ParallelPreTreatment/extern/labelPro"
            ));
            //            更改 2014 - 3 - 23
            Path testNumPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/testNumPro"));
            Path percentagePath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/percentagePro"));
            try {
                FileSystem fs = labelPath.getFileSystem(HadoopConfUtil.getNewConfiguration());
                FSDataOutputStream out = fs.create(labelPath);
                //            更改 2014 - 3 - 23
                FSDataOutputStream testNumOut = fs.create(testNumPath);
                FSDataOutputStream percentageOut = fs.create(percentagePath);
                int totalTrainNum = 0;
                for(int i=0;i<clazzNum;i++){
                    labelPro.setProperty(LabelProUtil.parseLabelStr(i),clazz[i]+"/"+clazzNumList[i]);

                    totalTrainNum += clazzNumList[i];
                    //            更改 2014 - 3 - 23
                    testNumPro.setProperty(LabelProUtil.parseLabelStr(i),String.valueOf(testNumList[i]));
                }

                for(int i=0;i<clazzNum;i++){
                    percentagePro.setProperty(LabelProUtil.parseLabelStr(i),String.valueOf((double)clazzNumList[i]/(double)totalTrainNum));
                }



                labelPro.store(out,"labelList");
                //            更改 2014 - 3 - 23
                testNumPro.store(testNumOut,"testNumList");

                percentagePro.store(percentageOut,"percentageList");

                out.close();
                testNumOut.close();
                percentageOut.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


            consoleLog.info("清洗完成");
        }
        else{
            consoleLog.error("出现错误，查看日志文件");
        }
        return resu;
    }

    static class cleaner implements Runnable{

        private Map<String,String> optionsValueMap;
        private String clazzName;
        private SequenceFile.Writer trainWriter;
        private SequenceFile.Writer testWriter;
        private Configuration conf = HadoopConfUtil.getNewConfiguration();
        private FileSystem fs;
        private HtmlTreater treater;
        private int clazzNum;
        private int clazzIndex;
        private static Log errorLog = LogUtil.getErrorLog();
        private static Log consoleLog = LogUtil.getConsoleLog();
        private int errorFileNum;
        private Integer[] clazzNumList = null;
        private int sum = 0;
        private float trainPecentage;

        private Integer[] testNumList = null;
        private int testSum = 0;

        cleaner(Map<String, String> optionsValueMap,String clazzName,int clazzNum,int clazzIndex,
                Integer[] clazzNumList,float trainPecentage) {
            this.optionsValueMap = optionsValueMap;
            this.clazzName = clazzName;
            this.clazzNum = clazzNum;
            this.clazzIndex = clazzIndex;
            this.clazzNumList = clazzNumList;
            this.trainPecentage = trainPecentage;
        }

        @Override
        public void run() {
            treater = DefHtmlTreater.getInstance();
            String inputPath = optionsValueMap.get("inputPath")+"/"+clazzName;
            String trainOutputPath = optionsValueMap.get("outputPath")+"/train/"+LabelProUtil.parseLabelStr(clazzIndex);
            String testOutputPath = optionsValueMap.get("outputPath")+"/test/"+LabelProUtil.parseLabelStr(clazzIndex);

            Path trainHdfsPath = new Path(HadoopConfUtil.createHdfsUrl(trainOutputPath));
            Path testHdfsPath = new Path(HadoopConfUtil.createHdfsUrl(testOutputPath));

            String[] filePaths = new File(inputPath).list();

            int trainNum = (int)(trainPecentage*filePaths.length);
            int testNum = filePaths.length - trainNum;

//            System.out.println(clazzName+" : "+filePaths.length);
//            System.out.println("trainNum"+ " :"+trainNum);
//            System.out.println("testNum"+" : "+testNum);
            try {
                fs = trainHdfsPath.getFileSystem(conf);
                if (!fs.exists(trainHdfsPath.getParent())){
                    fs.mkdirs(trainHdfsPath.getParent());
                    consoleLog.info("创建路径 "+trainHdfsPath.getParent());
                }

                if (!fs.exists(trainHdfsPath.getParent())){
                    fs.mkdirs(testHdfsPath.getParent());
                    consoleLog.info("创建路径 "+testHdfsPath.getParent());
                }

                trainWriter = new SequenceFile.Writer(fs,conf,trainHdfsPath, IntWritable.class, DefHtmlModel.class);
                testWriter = new SequenceFile.Writer(fs,conf,testHdfsPath,IntWritable.class,DefHtmlModel.class);

                int totalIndex = 0;

                for(int i=0;i<trainNum;i++){
//                    System.out.println(clazzName+" : index : "+trainNum );
                    parseFile(inputPath,filePaths[totalIndex],trainWriter,true);
                    totalIndex++;
                }

                for(int i=0;i<testNum;i++){
//                    System.out.println(clazzName+" : index : "+trainNum );
                    parseFile(inputPath,filePaths[totalIndex],testWriter,false);
                    totalIndex++;
                }


                trainWriter.close();
                testWriter.close();
            } catch (IOException e) {
                errorLog.error("清洗文件错误",e);
                taskNum -= clazzNum;
                return;
            }

            if (errorFileNum>0){
                consoleLog.error("类别 ： "+clazzName+"    错误总数 ："+errorFileNum+"\n请查看日志文件查看详细文件");
                errorLog.error("类别 ： "+clazzName+"  错误总数 ："+errorFileNum);
            }
            taskNum++;
            clazzNumList[clazzIndex] = sum;
            //            更改 2014 - 3 - 23
            testNumList[clazzIndex] = testSum;
        }

        private void parseFile(String inputPath,String fileName,SequenceFile.Writer writer,boolean isTrain) throws IOException {
//            boolean r = treater.parseHtml(inputPath+"/"+filePath,clazzName,filePaths[i],model);
            DefHtmlModel model = new DefHtmlModel();
            boolean r = treater.parseHtml(inputPath+"/"+fileName,this.clazzName,fileName,model);
            if (!r){
                consoleLog.error("文件错误 :"+inputPath+"/"+fileName);
                errorFileNum++;
            }
            else{
                writer.append(new IntWritable(clazzIndex),model);
                if (isTrain)
                    sum++;
                else
                    testSum++;
            }


        }

        public Integer[] getTestNumList() {
            return testNumList;
        }

        public void setTestNumList(Integer[] testNumList) {
            this.testNumList = testNumList;
        }

    }
}
