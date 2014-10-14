package main.core.Job.Default.Matrix;

import main.core.Job.TaskJob;
import main.core.Model.Feature.DefFeatureModel;
import main.core.Model.Token.DefFileTermsModel;
import main.core.Model.Matrix.DefMatrixKey;
import main.core.Treatment.CleanPathFilter;
import main.core.Util.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by adam on 14-3-19.
 */
public class DefMatrixJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public boolean startJob(String optionsValue) throws InterruptedException, IOException,
            ClassNotFoundException {
        Map<String,String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String trainInputPath = optionsValueMap.get("inputPath")+"/terms/train";
        String trainOutputPath = optionsValueMap.get("outputPath")+"/train";
        String featurePath = optionsValueMap.get("featurePath");

        String testInputPath = optionsValueMap.get("inputPath")+"/terms/test";
        String testOutputPath = optionsValueMap.get("outputPath")+"/test";

        Path hdfsTrainInput = new Path(HadoopConfUtil.createHdfsUrl(trainInputPath));
        Path hdfsTrainOutput = new Path(HadoopConfUtil.createHdfsUrl(trainOutputPath));
        Path hdfsFeature = new Path(HadoopConfUtil.createHdfsUrl(featurePath));

        Path hdfsTestInput = new Path(HadoopConfUtil.createHdfsUrl(testInputPath));
        Path hdfsTestOutput= new Path(HadoopConfUtil.createHdfsUrl(testOutputPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();

        FileSystem fs = hdfsTrainInput.getFileSystem(conf);

        FileStatus[] cacheFeature = fs.listStatus(hdfsFeature);

        for(FileStatus f : cacheFeature){
            DistributedCache.addCacheFile(f.getPath().toUri(),conf);
        }

        int clazzNum = fs.listStatus(hdfsTrainInput).length;

        consoleLog.info("开始创建训练集文本词矩阵 。。。。 ");
        Job trainJob = HadoopUtil.prepareJob(
                hdfsTrainInput,
                hdfsTrainOutput,
                SequenceFileInputFormat.class,
                DefMatrixMapper.class,
                DefMatrixKey.class,
                VectorWritable.class,
                SequenceFileOutputFormat.class,
                conf
        );

        trainJob.setNumReduceTasks(clazzNum);
        trainJob.setPartitionerClass(DefMatrixPartitioner.class);

        boolean stepOne = trainJob.waitForCompletion(true);

        if (!stepOne){
            consoleLog.info("训练集文本词矩阵未完成 。。。。");
            errorLog.error("训练集文本词矩阵未完成 。。。。 ");

            return false;
        }

        CleanPathFilter filter = new CleanPathFilter();

//        清楚job的日志
        JobUtil.cleanPath(hdfsTrainOutput, fs, filter);

        consoleLog.info("训练集文本词矩阵创建完成 。。。。");

        consoleLog.info("测试集文本词矩阵创建完成 。。。。");
        Job testJob = HadoopUtil.prepareJob(
                hdfsTestInput,
                hdfsTestOutput,
                SequenceFileInputFormat.class,
                DefMatrixMapper.class,
                DefMatrixKey.class,
                VectorWritable.class,
                SequenceFileOutputFormat.class,
                conf
        );

        testJob.setNumReduceTasks(clazzNum);
        testJob.setPartitionerClass(DefMatrixPartitioner.class);

        boolean stepTwo = testJob.waitForCompletion(true);
        if (!stepTwo){
            consoleLog.error("测试集文本词矩阵未完成 。。。。。");
            errorLog.error("测试集文本词矩阵未完成 。。。。。 ");
        }

        JobUtil.cleanPath(hdfsTestOutput,fs,filter);
        consoleLog.info("测试集文本词矩阵创建完成 。。。。。 ");

        return true;
    }

    static class DefMatrixPartitioner extends Partitioner<DefMatrixKey,VectorWritable>{

        @Override
        public int getPartition(DefMatrixKey key, VectorWritable value, int numPartitions) {
            int partitoner = key.getClazzIndex().get();
//            System.out.println(key);
            return partitoner;
        }
    }

    static class DefMatrixMapper extends Mapper<IntWritable,DefFileTermsModel,DefMatrixKey,VectorWritable> {
        private MapWritable featuresMap;
        private int clazzFeaturesNum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path[] cachePaths = DistributedCache.getLocalCacheFiles(conf);
            Path cacheFeaturePath = null;
            FileSplit split = (FileSplit) context.getInputSplit();
//            String clazzFileName = split.getPath().getName();

            cacheFeaturePath = cachePaths[0];
//            for(Path p : cachePaths){
//                if (clazzFileName.equals(p.getName())){
//                    cacheFeaturePath = p;
//                    break;
//                }
//            }

            FileSystem fs = FileSystem.getLocal(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs,cacheFeaturePath,conf);
            Text key = new Text();
            DefFeatureModel valueModel = new DefFeatureModel();

            reader.next(key,valueModel);

            featuresMap = valueModel.getFeaturesMap();
            clazzFeaturesNum = valueModel.getFeaturesNum().get();
            reader.close();
        }

        @Override
        protected void map(IntWritable key, DefFileTermsModel value, Context context) throws IOException, InterruptedException {
            Vector vector = new DenseVector(clazzFeaturesNum);

            MapWritable fileTermsMap = value.getFileTerms();

            for(Map.Entry<Writable,Writable> e : fileTermsMap.entrySet()){
                IntWritable location = (IntWritable) e.getValue();
                System.out.println(e.getKey()+" : "+location);
                if (featuresMap.containsKey(e.getKey())){
                    location = (IntWritable) featuresMap.get(e.getKey());
                    vector.set(location.get(),((IntWritable) e.getValue()).get());
                }

            }
//            String partitonKey = key.get()+"-"+value.getFileName().toString();
            DefMatrixKey outKey = new DefMatrixKey();
            outKey.setFileName(value.getFileName());
            outKey.setClazzIndex(key);
            outKey.setFileTermsNum(value.getTermsNum());
//            存储稀疏向量
//            Vector sparseVector = new RandomAccessSparseVector(vector);
            Vector sparseVector = new SequentialAccessSparseVector(vector);
//            System.out.println(outKey);
//            System.out.println("vector : "+sparseVector);
//            System.out.println("-------------------------------------");
            context.write(outKey,new VectorWritable(sparseVector));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

}
