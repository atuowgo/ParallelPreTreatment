package main.core.Job.Default.Feature;

import main.core.Job.TaskJob;
import main.core.Model.Feature.DefFeatureModel;
import main.core.Treatment.CleanPathFilter;
import main.core.Util.*;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-3-16.
 */
public class DefDFFetureJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public boolean startJob(String optionsValue) throws InterruptedException, IOException, ClassNotFoundException {
        Map<String,String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String inputPath = optionsValueMap.get("inputPath")+"/idf";
        String outputPath = optionsValueMap.get("outputPath");
        String fetureNumStr = optionsValueMap.get("fetureNum");



        Path hdfsInput = new Path(HadoopConfUtil.createHdfsUrl(inputPath));
        Path hdfsOutput = new Path(HadoopConfUtil.createHdfsUrl(outputPath));

        String labelPath = "/ParallelPreTreatment/extern/labelPro";
        Path hdfsLabel = new Path(HadoopConfUtil.createHdfsUrl(labelPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
        conf.set("fetureNum",fetureNumStr);

        FileSystem fs = hdfsInput.getFileSystem(conf);
        int clazzNum = fs.listStatus(hdfsInput).length;

//        将类标签文件和类文件数作为分布式缓冲加载到各个节点
        DistributedCache.addCacheFile(hdfsLabel.toUri(),conf);

        consoleLog.info("开始进行 DF 特征选择 。。。。 ");

        Job job = HadoopUtil.prepareJob(
                hdfsInput,
                hdfsOutput,
                SequenceFileInputFormat.class,
                DFFetureMapper.class,
                DoubleWritable.class,
                Text.class,
                DFFetureReducer.class,
                Text.class,
                DefFeatureModel.class,
                SequenceFileOutputFormat.class,
                conf
        );

        job.setPartitionerClass(DFFeturePartitioner.class);
        job.setNumReduceTasks(clazzNum);

        boolean stepOne = job.waitForCompletion(true);


        if (!stepOne){
            consoleLog.info("分词步骤未完成 。。。。");
            errorLog.error("分词步骤未完成 。。。。 ");

            return false;
        }

        CleanPathFilter filter = new CleanPathFilter();

//        清楚job的日志
        JobUtil.cleanPath(hdfsOutput, fs, filter);

        consoleLog.info("DF 特征选择结束 。。。。");
        return true;
    }




//    根据值做分区
    static class DFFeturePartitioner extends Partitioner<DoubleWritable,Text>{

    @Override
    public int getPartition(DoubleWritable key, Text value, int numPartitions) {
        int partitionerNum = LabelProUtil.parseDFPartitioner(value.toString(),"-");
        return partitionerNum;
    }
}

//    分区计算每个特征的DF，更改输出文件项，加上0000N-便于分区
    static class DFFetureMapper extends Mapper<Text,IntWritable,DoubleWritable,Text>{
        private Path hdfsLabel;
        private String clazzFileName;
        private Properties labelProperties;
        private int clazzTotalNum;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            hdfsLabel = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
            InputSplit inputSplit = context.getInputSplit();
            FileSplit split = (FileSplit) inputSplit;
            clazzFileName = split.getPath().getName();
            int index = clazzFileName.lastIndexOf("-");
            clazzFileName = clazzFileName.substring(++index);

            labelProperties = new Properties();
            labelProperties.load(new FileInputStream(hdfsLabel.toString()));

            String clazzPro = labelProperties.getProperty(clazzFileName);
            index = clazzPro.indexOf("/")+1;
            clazzPro = clazzPro.substring(index);
            clazzTotalNum = Integer.parseInt(clazzPro);
        }

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            double df = 0;
//            df = (double)value.get() / (double)clazzTotalNum;
//            df = Math.log()

            String tagValue = clazzFileName+"-"+key.toString();
            context.write(new DoubleWritable(-df),new Text(tagValue));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

//    排序特征的DF，选出前M个特征，后在统计为Vector类型，保留为文件便于以后做Join
    static class DFFetureReducer extends Reducer<DoubleWritable,Text,Text,DefFeatureModel>{
        private int fetureNum;
        private int index = 0;
        private DefFeatureModel featureModel = new DefFeatureModel();
        private MapWritable featuresMap = new MapWritable();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String fetureStr = context.getConfiguration().get("fetureNum");
            fetureNum = Integer.parseInt(fetureStr);
        }

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fileName = null;
            Iterator<Text> v = values.iterator();

            while(v.hasNext()){
                fileName = v.next().toString();
                fileName = LabelProUtil.parseDFLable(fileName, "-");
                featuresMap.put(new Text(fileName),new IntWritable(index++));
            }
//            while(index<fetureNum && v.hasNext()){
//                fileName = v.next().toString();
//                fileName = LabelProUtil.parseDFLable(fileName, "-");
//                featuresMap.put(new Text(fileName),new IntWritable(index++));
//            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            featureModel.setFeaturesNum(new IntWritable(featuresMap.size()));
            featureModel.setFeaturesMap(featuresMap);
            context.write(new Text(),featureModel);
            super.cleanup(context);
        }
    }

    static class DefSingleDFMapper extends Mapper<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    static class DefSingleDFReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
//        private int fetureNum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
//            String fetureStr = context.getConfiguration().get("fetureNum");
//            fetureNum = Integer.parseInt(fetureStr);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> v = values.iterator();

            int keySum = 0;
            while (v.hasNext()){
                keySum += v.next().get();
            }

            context.write(key,new IntWritable(keySum));
        }
    }

    static class DefSingleSortMapper extends Mapper<Text,IntWritable,DoubleWritable,Text>{
        private int fetureNum = 0;
        private int clazzNum = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String fetureStr = context.getConfiguration().get("fetureNum");
            fetureNum = Integer.parseInt(fetureStr);
        }

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {

        }
    }
}
