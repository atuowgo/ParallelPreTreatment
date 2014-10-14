package main.core.Job.Default.Token;

import main.core.Job.TaskJob;
import main.core.Model.Token.DefFileTermsModel;
import main.core.Model.Clean.DefHtmlModel;
import main.core.Model.Token.DefIDFModel;
import main.core.ModelDumper.DefIDFModelDumper;
import main.core.Treatment.CleanPathFilter;
import main.core.Treatment.DeletePathFilter;
import main.core.Util.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-3-12.
 */
public class DefHtmlTokenJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public boolean startJob(String optionsValue) throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String trainInputPath = optionsValueMap.get("inputPath") + "/train/";
        String trainTermsOutputPath = optionsValueMap.get("outputPath") + "/terms/train";
        String idfOutputPath = optionsValueMap.get("outputPath") + "/idf";
        String idfModelOutput = optionsValueMap.get("outputPath")+"/idfModel";

        String tmpPath = optionsValueMap.get("tmpPath");

        String testInputPath = optionsValueMap.get("inputPath") + "/test/";
        String testTermsOutputPath = optionsValueMap.get("outputPath") + "/terms/test";

        Path hdfsTrainInput = new Path(HadoopConfUtil.createHdfsUrl(trainInputPath));
        Path hdfsTrainTermsOutput = new Path(HadoopConfUtil.createHdfsUrl(trainTermsOutputPath));
        Path hdfsIdfOutput = new Path(HadoopConfUtil.createHdfsUrl(idfOutputPath));
        Path hdfsIdfModelPutput = new Path(HadoopConfUtil.createHdfsUrl(idfModelOutput));

        Path hdfsTestInput = new Path(HadoopConfUtil.createHdfsUrl(testInputPath));
        Path hdfsTestOutputPath = new Path(HadoopConfUtil.createHdfsUrl(testTermsOutputPath));

        Path hdfsTmpPath = new Path(HadoopConfUtil.createHdfsUrl(tmpPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
//        Configuration secConf = HadoopConfUtil.getNewConfiguration();

        FileSystem fs = hdfsTrainInput.getFileSystem(conf);
        int clazzNum = fs.listStatus(hdfsTrainInput).length;

//        int clazzNum = 1;
        consoleLog.info("开始分词 ... ");

        String termsNumPro = "/ParallelPreTreatment/extern/termsNumPro";
        Path hdfsTermsNumPro = new Path(HadoopConfUtil.createHdfsUrl(termsNumPro));

        conf.set("tmpPath", hdfsTmpPath.toString());
        conf.set("idfModelPath",hdfsIdfModelPutput.toString());
//        对训练集分词
        Job tokenJob = HadoopUtil.prepareJob(
                hdfsTrainInput,
                hdfsTrainTermsOutput,
                SequenceFileInputFormat.class,
                TokenMapper.class,
                IntWritable.class,
                DefFileTermsModel.class,
                TokenReducer.class,
                IntWritable.class,
                DefFileTermsModel.class,
                SequenceFileOutputFormat.class,
                conf
        );

        tokenJob.setPartitionerClass(TokenPartitioner.class);
        tokenJob.setNumReduceTasks(clazzNum);
        tokenJob.setMapOutputValueClass(DefFileTermsModel.class);

        boolean stepOne = tokenJob.waitForCompletion(true);


        if (!stepOne) {
            consoleLog.info("分词步骤未完成 。。。。");
            errorLog.error("分词步骤未完成 。。。。 ");

            return false;
        }

        CleanPathFilter filter = new CleanPathFilter();

//        清楚job的日志
        JobUtil.cleanPath(hdfsTrainTermsOutput, fs, filter);

        margeTermsNum(hdfsTmpPath, conf, clazzNum, hdfsTermsNumPro);


        DeletePathFilter deletePathFilter = new DeletePathFilter();

        JobUtil.cleanPath(hdfsTmpPath.getParent(), fs, deletePathFilter);

        consoleLog.info("分词结束");

        consoleLog.info("统计词项的倒排文档数 ... ");

        Job idfJob = HadoopUtil.prepareJob(
                hdfsTrainTermsOutput,
                hdfsIdfOutput,
                SequenceFileInputFormat.class,
                IDFMapper.class,
                Text.class,
                IntWritable.class,
                IDFReducer.class,
                Text.class,
                IntWritable.class,
                SequenceFileOutputFormat.class,
                conf
//                secConf
        );

        idfJob.setNumReduceTasks(clazzNum);
        idfJob.setPartitionerClass(IDFPartitioner.class);
        boolean stepTwo = idfJob.waitForCompletion(true);

        if (!stepTwo) {
            consoleLog.error("统计词项出错 。。。。");
            errorLog.error("统计词项出错 。。。。");
            return false;
        }

        JobUtil.cleanPath(hdfsIdfOutput, fs, filter);
        consoleLog.info("统计结束，作业完成 ... ");

        consoleLog.info("测试集分词 。。。。");
        Job testTokenJob = HadoopUtil.prepareJob(
                hdfsTestInput,
                hdfsTestOutputPath,
                SequenceFileInputFormat.class,
                TokenMapper.class,
                IntWritable.class,
                DefFileTermsModel.class,
                SequenceFileOutputFormat.class,
                conf
//                secConf
        );

        testTokenJob.setPartitionerClass(TokenPartitioner.class);
        testTokenJob.setNumReduceTasks(clazzNum);
//        tokenJob.setMapOutputValueClass(DefFileTermsModel.class);

        boolean stepThree = testTokenJob.waitForCompletion(true);
        if (!stepThree) {
            consoleLog.error("测试集分词出错 。。。。");
            errorLog.error("测试集分词出错 。。。。");
            return false;
        }

        JobUtil.cleanPath(hdfsTestOutputPath, fs, filter);


        consoleLog.info("测试集分词结束 。。。。。 ");
        return true;
    }

    private void margeTermsNum(Path tmpPath, Configuration conf, int clazzNum, Path externPath) throws IOException {
        FileSystem fs = tmpPath.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(tmpPath);

        Properties termsNumPro = new Properties();

        for (int i = 0; i < clazzNum; i++) {
            SequenceFile.Reader reader = new SequenceFile.Reader(
                    fs,
                    statuses[i].getPath(),
                    conf
            );

            IntWritable key = new IntWritable(), value = new IntWritable();
            reader.next(key, value);
            termsNumPro.setProperty(LabelProUtil.parseLabelStr(key.get()), String.valueOf(value.get()));
            reader.close();
        }

        FSDataOutputStream out = fs.create(externPath);
        termsNumPro.store(out, "termsNumPro");
        out.close();
    }

    static class TokenPartitioner extends Partitioner<IntWritable, DefFileTermsModel> {

        @Override
        public int getPartition(IntWritable intWritable, DefFileTermsModel defFileTermsModel, int i) {
            return intWritable.get();
//            return 0;
        }
    }

    static class IDFPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            return intWritable.get();
//            return 0;
        }
    }

    //    分区/不分区都行
    static class TokenMapper extends Mapper<IntWritable, DefHtmlModel, IntWritable, DefFileTermsModel> {
        Analyzer analyzer = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            analyzer = new StandardAnalyzer(Version.LUCENE_43);
        }

        @Override
        protected void map(IntWritable key, DefHtmlModel value, Context context) throws IOException, InterruptedException {
            String content = value.getContent().toString();
            TokenStream stream = analyzer.tokenStream("terms", new StringReader(content));
            CharTermAttribute termAttribute = stream.addAttribute(CharTermAttribute.class);

            DefFileTermsModel termsModel = new DefFileTermsModel();
            MapWritable termsMap = new MapWritable();

            termsModel.setFileName(value.getFileName());
            termsModel.setClazzName(value.getClazz());


            IntWritable termNum = new IntWritable();

            stream.reset();
            while (stream.incrementToken()) {
                Text termName = new Text();
                termName.set(termAttribute.toString());

                if (termsMap.containsKey(termName)) {
                    termNum = (IntWritable) termsMap.get(termName);
                    termNum.set(termNum.get() + 1);

                    termsMap.put(termName, termNum);
                } else {
//                    termNum.set(1);
//                    termsMap.put(termName, termNum);
                    termsMap.put(termName, new IntWritable(1));
                }
            }

//            MapWritableDumper.mapDumper(termsMap);
            termsModel.setFileTerms(termsMap);
            termsModel.setTermsNum(new IntWritable(termsMap.size()));

            stream.close();
            context.write(key, termsModel);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            analyzer.close();
            super.cleanup(context);
        }
    }

    static class TokenReducer extends Reducer<IntWritable, DefFileTermsModel, IntWritable, DefFileTermsModel> {
        private int clazzTermsNum = 0;
        private int clazzIndex = -1;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DefFileTermsModel> values, Context context)
                throws IOException, InterruptedException {
//            if (clazzIndex==-1)
            clazzIndex = key.get();

            Iterator<DefFileTermsModel> iter = values.iterator();
            while (iter.hasNext()) {
                DefFileTermsModel model = iter.next();
                clazzTermsNum += model.getTermsNum().get();
                context.write(key, model);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String fileName = clazzIndex + "_termsNum";
            String tmpPath = conf.get("tmpPath") + "/" + fileName;
            Path hdfsTmpPath = new Path(tmpPath);
            SequenceFile.Writer writer = new SequenceFile.Writer(
                    hdfsTmpPath.getFileSystem(conf),
                    conf,
                    hdfsTmpPath,
                    IntWritable.class,
                    IntWritable.class
            );
            writer.append(new IntWritable(clazzIndex), new IntWritable(clazzTermsNum));
            writer.syncFs();
            writer.close();

            super.cleanup(context);
        }
    }


//    IDF


    //    同一分区下进行，每个类一个区
    static class IDFMapper extends Mapper<IntWritable, DefFileTermsModel, Text, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(IntWritable key, DefFileTermsModel value, Context context) throws IOException, InterruptedException {
            MapWritable termsMap = value.getFileTerms();

            for (Writable term : termsMap.keySet()) {
                context.write((Text) term, key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    //    分区号代表类名，再转储成idf列表
    static class IDFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MapWritable termsIdf;
        private String idfModelPath;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            termsIdf = new MapWritable();
            idfModelPath = conf.get("idfModelPath");
            int clazzIndex =  conf.getInt("mapred.task.partition",0);
            idfModelPath += "/"+LabelProUtil.parseLabelStr(clazzIndex);
            System.out.println("modelPath : "+idfModelPath);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> v = values.iterator();

            while (v.hasNext()) {
                sum++;
                v.next();
            }

//            4-20
            termsIdf.put(new Text(key.toString()),new IntWritable(sum));
            context.write(key, new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            DefIDFModel idfModel = new DefIDFModel();
            idfModel.setTermsIDF(termsIdf);
            Path disPath = new Path(idfModelPath);
//            4-20
            DefIDFModelDumper.persistenceMode(idfModel,disPath,context.getConfiguration());
            super.cleanup(context);
        }
    }
}
