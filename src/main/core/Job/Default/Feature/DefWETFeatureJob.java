package main.core.Job.Default.Feature;

import main.core.Job.TaskJob;
import main.core.Job.UtilMapperReducer.IdentiferMapper;
import main.core.Model.Token.DefECETermModel;
import main.core.Model.Feature.DefFeatureModel;
import main.core.Model.Token.DefFileTermsModel;
import main.core.ModelDumper.ExternPathDumper;
import main.core.Treatment.CleanPathFilter;
import main.core.Treatment.DeletePathFilter;
import main.core.Util.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-4-16.
 */
public class DefWETFeatureJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public boolean startJob(String optionsValue)
            throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String inputPath = optionsValueMap.get("inputPath") + "/terms/train";
        String outputPath = optionsValueMap.get("outputPath");
        String tmpPath = optionsValueMap.get("tmpPath");
        String featureNumStr = optionsValueMap.get("featureNum");

        String aggreateOutPath = outputPath + "/aggreateWETModel";

        Path hdfsInputPath = new Path(HadoopConfUtil.createHdfsUrl(inputPath));
        Path hdfsOutputPath = new Path(HadoopConfUtil.createHdfsUrl(outputPath));
        Path hdfsTmpPath = new Path(HadoopConfUtil.createHdfsUrl(tmpPath));

        Path hdfsAggreateOutPath = new Path(HadoopConfUtil.createHdfsUrl(aggreateOutPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
        conf.set("featureNum", featureNumStr);

        FileSystem fs = hdfsInputPath.getFileSystem(conf);
        int clazzNum = fs.listStatus(hdfsInputPath).length;
//        int clazzNum = 3;

        consoleLog.info("defWET开始。。。。。。");
        conf.set("clazzNum", String.valueOf(clazzNum));

        String percentagePath = "/ParallelPreTreatment/extern/percentagePro";
        Path hdfsPercentagePath = new Path(HadoopConfUtil.createHdfsUrl(percentagePath));
        String termsNumPath = "/ParallelPreTreatment/extern/termsNumPro";
        Path hdfsTermsNumPath = new Path(HadoopConfUtil.createHdfsUrl(termsNumPath));
        Properties percentagePro,termsNumPro;
        percentagePro = ExternPathDumper.getPropertiesInstaceByPath(hdfsPercentagePath,fs);
        termsNumPro = ExternPathDumper.getPropertiesInstaceByPath(hdfsTermsNumPath,fs);

        if (percentagePro==null || termsNumPro==null){
            return false;
        }

        conf.set("percentageProStr",OptionsRepo.createOptionsFromPro(percentagePro));
        conf.set("termsNumProStr",OptionsRepo.createOptionsFromPro(termsNumPro));

        Path eceTermsSumTmp = new Path(hdfsTmpPath, "sumTmp");
        Job eceTermsSumJob = HadoopUtil.prepareJob(
                hdfsInputPath,
                eceTermsSumTmp,
                SequenceFileInputFormat.class,
                ECETermsSumMapper.class,
                Text.class,
                DefECETermModel.class,
                ECETermsSumReducer.class,
                Text.class,
                DefECETermModel.class,
                SequenceFileOutputFormat.class,
                conf
        );
        eceTermsSumJob.setNumReduceTasks(clazzNum);
        eceTermsSumJob.setPartitionerClass(ECEModelPartitioner.class);

        boolean stepOne = eceTermsSumJob.waitForCompletion(true);
        if (!stepOne) {
            return false;
        }

        PathFilter cleanFilter = new CleanPathFilter();
        JobUtil.cleanPath(eceTermsSumTmp, fs, cleanFilter);

        Path eceTmp = new Path(hdfsTmpPath, "wetTmp");
        Job eceJob = HadoopUtil.prepareJob(
                eceTermsSumTmp,
                eceTmp,
                SequenceFileInputFormat.class,
                IdentiferMapper.class,
                Text.class,
                DefECETermModel.class,
                ECEReuder.class,
                Text.class,
                DoubleWritable.class,
                SequenceFileOutputFormat.class,
                conf
        );

        boolean stepTwo = eceJob.waitForCompletion(true);
        if (!stepTwo) {
            return false;
        }

        JobUtil.cleanPath(eceTmp, fs, cleanFilter);

        Job eceSortJob = HadoopUtil.prepareJob(
                eceTmp,
                hdfsOutputPath,
                SequenceFileInputFormat.class,
                ECESortMapper.class,
                DoubleWritable.class,
                Text.class,
                ECESortReducer.class,
                Text.class,
                DefFeatureModel.class,
                SequenceFileOutputFormat.class,
                conf
        );

        boolean stepTree = eceSortJob.waitForCompletion(true);
        if (!stepTree) {
            return false;
        }

        JobUtil.cleanPath(hdfsOutputPath, fs, cleanFilter);

        String aggreSrcOutputPath = outputPath + "/part-r-00000";
        Path hdfsAggreSrc = new Path(HadoopConfUtil.createHdfsUrl(aggreSrcOutputPath));
        fs.rename(hdfsAggreSrc, hdfsAggreateOutPath);

        PathFilter deleteFilter = new DeletePathFilter();
        JobUtil.cleanPath(hdfsTmpPath.getParent(), fs, deleteFilter);

        consoleLog.info("defWET结束。。。。。。");

        return true;
    }

    static class ECEModelPartitioner
            extends Partitioner<Text, DefECETermModel> {

        @Override
        public int getPartition(Text text, DefECETermModel defECETermModel, int numPartitions) {
            return defECETermModel.getClazzIndex().get();
//            return 0;
        }
    }

    static class ECETermsSumMapper
            extends Mapper<IntWritable, DefFileTermsModel, Text, DefECETermModel> {
        @Override
        protected void map(IntWritable key, DefFileTermsModel value, Context context)
                throws IOException, InterruptedException {
            MapWritable termsMap = value.getFileTerms();
            String termsStr;
            int termsNum;
//            存储clazzIndex和每个文档termsNum
            for (Map.Entry<Writable, Writable> mapE : termsMap.entrySet()) {
                termsStr = ((Text) mapE.getKey()).toString();
                termsNum = ((IntWritable) mapE.getValue()).get();
                DefECETermModel model = new DefECETermModel();
                model.setClazzIndex(key);
                model.setTermsNum(new IntWritable(termsNum));
                context.write(new Text(termsStr), model);
            }
        }
    }

    static class ECETermsSumReducer
            extends Reducer<Text, DefECETermModel, Text, DefECETermModel> {
        private String clazzIndex = null;
        private double clazzPercentage = 0;
        private int clazzTermsSum = 0;
//        Properties percentagePro, termsNumPro;
        private Map<String,String> percentageMap;
        private Map<String,String> termsNumMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();

            String percentageStr = conf.get("percentageProStr");
            String termsNumStr = conf.get("termsNumProStr");

            percentageMap = OptionsRepo.parseOptionsValue(percentageStr);
            termsNumMap = OptionsRepo.parseOptionsValue(termsNumStr);
        }

        @Override
        protected void reduce(Text key, Iterable<DefECETermModel> values, Context context)
                throws IOException, InterruptedException {
            Iterator<DefECETermModel> iter = values.iterator();
//            单个类中的termsNum
            int termNum = 0;
            DefECETermModel sumModel = new DefECETermModel();

            while (iter.hasNext()) {
                DefECETermModel model = iter.next();
//                初始化clazzIndex
                if (clazzIndex == null) {
                    clazzIndex = LabelProUtil.parseLabelStr(model.getClazzIndex().get());
                    clazzPercentage = Double.parseDouble(percentageMap.get(clazzIndex));
                    clazzTermsSum = Integer.parseInt(termsNumMap.get(clazzIndex));
                }
                termNum += model.getTermsNum().get();
            }

//            统计单个类中的termsNum和clazzTermRatio
            sumModel.setTermsNum(new IntWritable(termNum));
            sumModel.setClazzIndex(new IntWritable(LabelProUtil.reParseLableInt(clazzIndex)));
            double termRatio = ((double) termNum) / clazzTermsSum;
            double clazzTermEce = termRatio * Math.log(termRatio / clazzPercentage);
            sumModel.setTermsClassRatio(new DoubleWritable(clazzTermEce));
            context.write(key, sumModel);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    static class ECEReuder extends
            Reducer<Text, DefECETermModel, Text, DoubleWritable> {
        private int setTotalTermsNum;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Map<String,String> termsNumMap;

            String termsNumStr = conf.get("termsNumProStr");
            termsNumMap = OptionsRepo.parseOptionsValue(termsNumStr);

            int clazzNum = Integer.parseInt(conf.get("clazzNum"));
            for (int i = 0; i < clazzNum; i++) {
                setTotalTermsNum += Integer.parseInt(termsNumMap.get(LabelProUtil.parseLabelStr(i)));
            }

        }

        @Override
        protected void reduce(Text key, Iterable<DefECETermModel> values, Context context)
                throws IOException, InterruptedException {
            Iterator<DefECETermModel> iter = values.iterator();

            int setTermsNum = 0;
            double setRatio = 0;
            while (iter.hasNext()) {
                DefECETermModel model = iter.next();
                setTermsNum += model.getTermsNum().get();
                setRatio += model.getTermsClassRatio().get();
            }

            double termsSetRatio = ((double) setTermsNum) / setTotalTermsNum;
            double termEce = termsSetRatio * setRatio;

            context.write(key, new DoubleWritable(termEce));
        }
    }

    static class ECESortMapper
            extends Mapper<Text, DoubleWritable, DoubleWritable, Text> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context)
                throws IOException, InterruptedException {
            double v = value.get();
            context.write(new DoubleWritable(-v), key);
        }
    }

    static class ECESortReducer
            extends Reducer<DoubleWritable, Text, Text, DefFeatureModel> {
        private int featureNum, index;
        private DefFeatureModel featureModel;
        private MapWritable featureMap;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            String featureNumStr = context.getConfiguration().get("featureNum");
            featureNum = Integer.parseInt(featureNumStr);
            if (featureNum == 0) {
                featureNum = Integer.MAX_VALUE;
            }

            featureMap = new MapWritable();
            featureModel = new DefFeatureModel();
            index = 0;
        }

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            while (index < featureNum && iter.hasNext()) {
                Text outKey = new Text(iter.next().toString());
                IntWritable outValue = new IntWritable(index++);
                featureMap.put(outKey, outValue);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            featureModel.setFeaturesNum(new IntWritable(featureMap.size()));
            featureModel.setFeaturesMap(featureMap);
            context.write(new Text("WETFeatreu"), featureModel);
            super.cleanup(context);
        }
    }
}
