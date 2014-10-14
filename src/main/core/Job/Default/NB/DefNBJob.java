package main.core.Job.Default.NB;

import main.core.Job.TaskJob;
import main.core.Model.Evaluate.PRModel;
import main.core.Model.Matrix.DefMatrixKey;
import main.core.Model.NB.DefNBAggreateModel;
import main.core.Model.NB.DefNBEvaluateValue;
import main.core.Model.NB.DefNBKey;
import main.core.ModelDumper.NBEvaluateDumper;
import main.core.Treatment.CleanPathFilter;
import main.core.Treatment.NBTargetClazzTreater;
import main.core.Util.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by adam on 14-3-22.
 */
public class DefNBJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();

    @Override
    public boolean startJob(String optionsValue)
            throws InterruptedException, IOException, ClassNotFoundException {
        Map<String, String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String matrixInputPath = optionsValueMap.get("inputPath") + "/train/";
        String modelOutputPath = optionsValueMap.get("outputPath") + "/model";

        String modelAggreateOutputPath = optionsValueMap.get("outputPath") + "/aggreateModel";

        String testInputPath = optionsValueMap.get("inputPath") + "/test";
        String evalueOutputPath = optionsValueMap.get("outputPath") + "/evaluate";

        String localOutputPath = optionsValueMap.get("localOutputPath");

        Path hdfsMatrixInput = new Path(HadoopConfUtil.createHdfsUrl(matrixInputPath));
        Path hdfsModelOutput = new Path(HadoopConfUtil.createHdfsUrl(modelOutputPath));

        Path hdfsAggreateOutput = new Path(HadoopConfUtil.createHdfsUrl(modelAggreateOutputPath));

        Path hdfsTestInput = new Path(HadoopConfUtil.createHdfsUrl(testInputPath));
        Path hdfsEvalueOutput = new Path(HadoopConfUtil.createHdfsUrl(evalueOutputPath));

        String labelProPath = "/ParallelPreTreatment/extern/labelPro";
        Path hdfsLabelPath = new Path(HadoopConfUtil.createHdfsUrl(labelProPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();

        FileSystem fs = hdfsMatrixInput.getFileSystem(conf);

        int clazzNum = fs.listStatus(hdfsMatrixInput).length;
//        int clazzNum = 1;
//        训练NB模型
        consoleLog.info("开始训练NB分类模型。。。。。。");
        DistributedCache.addCacheFile(hdfsLabelPath.toUri(), conf);
        Job modelJob = HadoopUtil.prepareJob(
                hdfsMatrixInput,
                hdfsModelOutput,
                SequenceFileInputFormat.class,
                NBMapper.class,
                DefNBKey.class,
                VectorWritable.class,
                NBReducer.class,
                DefNBKey.class,
                VectorWritable.class,
                SequenceFileOutputFormat.class,
                conf
        );

        modelJob.setPartitionerClass(NBPartitioner.class);
        modelJob.setNumReduceTasks(clazzNum);
        boolean stepOne = modelJob.waitForCompletion(true);

        if (!stepOne) {

            return false;
        }
//
        PathFilter filter = new CleanPathFilter();
//
        JobUtil.cleanPath(hdfsModelOutput, fs, filter);

        consoleLog.info("模型生成完成。。。。 ");

//        NB测试
        consoleLog.info("NB测试开始。。。。。。");

        conf.set("clazzNum", String.valueOf(clazzNum));
        conf.set("modelPath", hdfsModelOutput.toString());

        String percentagePath = "/ParallelPreTreatment/extern/percentagePro";
        Path hdfsPercentagePath = new Path(HadoopConfUtil.createHdfsUrl(percentagePath));
        conf.set("percentagePath", hdfsPercentagePath.toString());

        Job evaluateJob = HadoopUtil.prepareJob(
                hdfsTestInput,
                hdfsEvalueOutput,
                SequenceFileInputFormat.class,
                NBEvaluateMapper.class,
                Text.class,
                DefNBEvaluateValue.class,
                NBEvaluateReducer.class,
                Text.class,
                DefNBEvaluateValue.class,
                SequenceFileOutputFormat.class,
                conf
        );

        boolean stepTwo = evaluateJob.waitForCompletion(true);

        if (!stepTwo) {

            return false;
        }

        JobUtil.cleanPath(hdfsEvalueOutput, fs, filter);

        Path hdfsSrcOutputPath = new Path(HadoopConfUtil.createHdfsUrl(evalueOutputPath + "/part-r-00000"));
        Path hdfsDstOutputPath = new Path(HadoopConfUtil.createHdfsUrl(evalueOutputPath + "/evaluateModel"));

        fs.rename(hdfsSrcOutputPath, hdfsDstOutputPath);

        consoleLog.info("NB测试结束。。。。。。");

        DefNBEvaluateValue value = NBEvaluateDumper.getNBEvaluateInstace(hdfsDstOutputPath, conf);
        PRModel prModel = new PRModel();

        NBEvaluateDumper.cauculatePR(value, prModel);

        String markStr = optionsValueMap.get("markStr");

        String uuid = UUID.randomUUID().toString();
        localOutputPath += "/"+uuid;
        String introducePath = localOutputPath + "/"+markStr;
        String instanceOutPath = localOutputPath + "/evaluateResult.txt";
        String prOutPath = localOutputPath + "/prResult.txt";

        FileSystem localFileSystem = FileSystem.getLocal(conf);
        FSDataOutputStream introduceOut = localFileSystem.create(new Path(introducePath));
        FSDataOutputStream instanceOut = localFileSystem.create(new Path(instanceOutPath));
        FSDataOutputStream prOut = localFileSystem.create(new Path(prOutPath));

        NBEvaluateDumper.dumpIntroduce(markStr,introduceOut,true);
        NBEvaluateDumper.dumpInstace(value, instanceOut, true);
        NBEvaluateDumper.dumpPRModel(prModel, prOut, true);
        consoleLog.info("输出路径为 ： "+localOutputPath);

//        String pathToDel = "/ParallelPreTreatment";
//        Path hdfsDelPath = new Path(HadoopConfUtil.createHdfsUrl(pathToDel));
//        PathFilter delFilter = new DeletePathFilter();
//        JobUtil.cleanPath(hdfsDelPath,fs,delFilter);
        return true;
    }

    //    训练NB模型
    static class NBPartitioner extends Partitioner<DefNBKey, VectorWritable> {

        @Override
        public int getPartition(DefNBKey defNBKey, VectorWritable vectorWritable, int numPartitions) {
            String indexStr = defNBKey.getClazzIndex().toString();

            return LabelProUtil.reParseLableInt(indexStr);
//            return 0;
        }
    }

    static class NBMapper extends Mapper<DefMatrixKey, VectorWritable, DefNBKey, VectorWritable> {
        private Vector sumVector = null;
        private String clazzIndexStr;
        private int vectorNum;
        private int clazzNum;
        private int labelNum;
        private int termsNum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit split = (FileSplit) context.getInputSplit();
            String fileName = split.getPath().getName();
//
            clazzIndexStr = LabelProUtil.parseStrR(fileName);
            vectorNum = -1;
//
            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Properties labelPro = new Properties();
            labelPro.load(new FileInputStream(paths[0].toString()));

            String clazzPro = labelPro.getProperty(clazzIndexStr);
            int index = clazzPro.indexOf("/") + 1;
            clazzPro = clazzPro.substring(index);
            labelNum = Integer.parseInt(clazzPro);
        }

        @Override
        protected void map(DefMatrixKey key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            Vector vector = value.get();
//            termsNum += vector.zSum();
            if (vectorNum == -1) {
                vectorNum = vector.size();
                sumVector = new DenseVector(vectorNum);
            }
//            文档个数
            clazzNum++;
//            sumVector = sumVector.plus(vector);
//            统计类别中词项个数和统计类别的词频矩阵
            for (int i = 0; i < vectorNum; i++) {
                termsNum += vector.get(i);
                sumVector.set(i, sumVector.get(i) + vector.get(i));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            DefNBKey key = new DefNBKey();
            key.setClazzTotalNum(new IntWritable(clazzNum));
            key.setClazzIndex(new Text(clazzIndexStr));
            key.setTermsTotalNum(new IntWritable(termsNum));
            context.write(key, new VectorWritable(sumVector));
            super.cleanup(context);
        }
    }

    //    多个map part汇总
    static class NBReducer extends Reducer<DefNBKey, VectorWritable, DefNBKey, VectorWritable> {
        private int totalNum;
        private int termsTotalNum;
        private String clazzIndexStr = null;
        private Vector vector;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(DefNBKey key, Iterable<VectorWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<VectorWritable> iterVector = values.iterator();
            if (clazzIndexStr == null) {
                clazzIndexStr = key.getClazzIndex().toString();
                VectorWritable v = iterVector.next();
                vector = new DenseVector(v.get());
            }

            totalNum += key.getClazzTotalNum().get();
            termsTotalNum += key.getTermsTotalNum().get();
            while (iterVector.hasNext()) {
                vector.plus(iterVector.next().get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            DefNBKey key = new DefNBKey();
            key.setClazzIndex(new Text(clazzIndexStr));
            key.setClazzTotalNum(new IntWritable(totalNum));
            key.setTermsTotalNum(new IntWritable(termsTotalNum));

//            更改4-14
            vector = vector.plus(1);
            vector = vector.divide(termsTotalNum + vector.size());
//            vector = vector.divide(termsTotalNum);
            context.write(key, new VectorWritable(vector));
            super.cleanup(context);
        }
    }

    //    NB测试
    static class NBEvaluateMapper extends Mapper<DefMatrixKey, VectorWritable, Text, DefNBEvaluateValue> {
        private DefNBEvaluateValue evaluateValue;
        private DefNBAggreateModel aggreateModel;
        private String modelPath;
        private String percentagePath;
        private int clazzNum;
        private Configuration conf;
        private int testNumList[];
        private int realteNumList[];
        private int testAndRealteNumList[];

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            conf = context.getConfiguration();
            clazzNum = Integer.parseInt(conf.get("clazzNum"));
            modelPath = conf.get("modelPath");
            percentagePath = conf.get("percentagePath");

            Path hdfsModelPath = new Path(modelPath);
            Path hdfsPercentagePath = new Path(percentagePath);
            aggreateModel = DefNBAggreateModel.getInstanceFromPath(hdfsPercentagePath,
                    hdfsModelPath, conf, hdfsModelPath.getFileSystem(conf));

            evaluateValue = new DefNBEvaluateValue();
            testNumList = new int[clazzNum];
            realteNumList = new int[clazzNum];
            testAndRealteNumList = new int[clazzNum];
            evaluateValue.setClazzNum(new IntWritable(clazzNum));

        }

        @Override
        protected void map(DefMatrixKey key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            int srcClazz = key.getClazzIndex().get();
            testNumList[srcClazz]++;
//            realteNumList[srcClazz]++;
            Vector srcVector = value.get();
            int targetClazz = NBTargetClazzTreater.findTargetClazz(srcVector, aggreateModel);
            realteNumList[targetClazz]++;
            if (srcClazz == targetClazz)
                testAndRealteNumList[srcClazz]++;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            evaluateValue.setArrayData(
                    evaluateValue.getArrayData(DefNBEvaluateValue.ArrayDataType.Test), testNumList);
            evaluateValue.setArrayData(
                    evaluateValue.getArrayData(DefNBEvaluateValue.ArrayDataType.Relate), realteNumList);
            evaluateValue.setArrayData(
                    evaluateValue.getArrayData(DefNBEvaluateValue.ArrayDataType.TestAndRelate), testAndRealteNumList);

            context.write(new Text("mapper"), evaluateValue);
            super.cleanup(context);
        }
    }

    static class NBEvaluateReducer extends Reducer<Text, DefNBEvaluateValue, Text, DefNBEvaluateValue> {
        private int clazzNum;
        private int testNumList[];
        private int realteNumList[];
        private int testAndRealteNumList[];
        private DefNBEvaluateValue value;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            clazzNum = Integer.parseInt(conf.get("clazzNum"));
            testNumList = new int[clazzNum];
            realteNumList = new int[clazzNum];
            testAndRealteNumList = new int[clazzNum];

            value = new DefNBEvaluateValue();
            value.setClazzNum(new IntWritable(clazzNum));
        }

        @Override
        protected void reduce(Text key, Iterable<DefNBEvaluateValue> values, Context context) throws IOException, InterruptedException {
            Iterator<DefNBEvaluateValue> itr = values.iterator();

            DefNBEvaluateValue value = null;

            int testNum[];
            int realteNum[];
            int testAndRealteNum[];
            while (itr.hasNext()) {
                value = itr.next();

                testNum = value.getNavieData(DefNBEvaluateValue.ArrayDataType.Test);
                realteNum = value.getNavieData(DefNBEvaluateValue.ArrayDataType.Relate);
                testAndRealteNum = value.getNavieData(DefNBEvaluateValue.ArrayDataType.TestAndRelate);

                for (int i = 0; i < clazzNum; i++) {
                    testNumList[i] += testNum[i];
                    realteNumList[i] += realteNum[i];
                    testAndRealteNumList[i] += testAndRealteNum[i];
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            value.setArrayData(
                    value.getArrayData(DefNBEvaluateValue.ArrayDataType.Test), testNumList);
            value.setArrayData(
                    value.getArrayData(DefNBEvaluateValue.ArrayDataType.Relate), realteNumList);
            value.setArrayData(
                    value.getArrayData(DefNBEvaluateValue.ArrayDataType.TestAndRelate), testAndRealteNumList);
            context.write(new Text("reducer"), value);
            super.cleanup(context);
        }
    }
}
