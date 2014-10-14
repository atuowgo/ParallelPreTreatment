package main.core.Job.Default.Feature;

import main.core.Job.TaskJob;
import main.core.Job.UtilMapperReducer.IdentiferMapper;
import main.core.Model.Feature.DefFeatureModel;
import main.core.Treatment.CleanPathFilter;
import main.core.Treatment.DeletePathFilter;
import main.core.Util.HadoopConfUtil;
import main.core.Util.JobUtil;
import main.core.Util.LogUtil;
import main.core.Util.OptionsRepo;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by adam on 14-3-25.
 */
public class DefDFSingleJob implements TaskJob {
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public boolean startJob(String optionsValue)
            throws InterruptedException, IOException, ClassNotFoundException {
        Map<String,String> optionsValueMap = OptionsRepo.parseOptionsValue(optionsValue);
        String inputPath = optionsValueMap.get("inputPath")+"/idf";
        String outputPath = optionsValueMap.get("outputPath");
        String tmpPath = optionsValueMap.get("tmpPath");
        String fetureNumStr = optionsValueMap.get("fetureNum");

        System.out.println("DF FeatureNum : "+fetureNumStr);


        String aggreOutputPath = outputPath+"/aggreDFModel";

        Path hdfsInput = new Path(HadoopConfUtil.createHdfsUrl(inputPath));
        Path hdfsOutput = new Path(HadoopConfUtil.createHdfsUrl(outputPath));
        Path hdfsTmp = new Path(HadoopConfUtil.createHdfsUrl(tmpPath));

        Path hdfsAggreOutput = new Path(HadoopConfUtil.createHdfsUrl(aggreOutputPath));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
        conf.set("fetureNum",fetureNumStr);

        FileSystem fs = hdfsInput.getFileSystem(conf);
        int clazzNum = fs.listStatus(hdfsInput).length;

        Job sumJob = HadoopUtil.prepareJob(
                hdfsInput,
                hdfsTmp,
                SequenceFileInputFormat.class,
                IdentiferMapper.class,
                Text.class,
                IntWritable.class,
                SingleDFReducer.class,
                IntWritable.class,
                Text.class,
                SequenceFileOutputFormat.class,
                conf
        );

        boolean stepOne = sumJob.waitForCompletion(true);

        CleanPathFilter filter = new CleanPathFilter();

        JobUtil.cleanPath(hdfsTmp,fs,filter);

        Job sortJob = HadoopUtil.prepareJob(
                hdfsTmp,
                hdfsOutput,
                SequenceFileInputFormat.class,
                IdentiferMapper.class,
                IntWritable.class,
                Text.class,
                SortDFReducer.class,
                Text.class,
                DefFeatureModel.class,
                SequenceFileOutputFormat.class,
                conf
        );

        boolean steoTwo = sortJob.waitForCompletion(true);

        JobUtil.cleanPath(hdfsOutput,fs,filter);

        JobUtil.cleanPath(hdfsTmp.getParent()   ,fs,new DeletePathFilter());

        String aggreSrcOutputPath = outputPath+"/part-r-00000";
        Path hdfsAggreSrc = new Path(HadoopConfUtil.createHdfsUrl(aggreSrcOutputPath));
        fs.rename(hdfsAggreSrc,hdfsAggreOutput);
        return true;
    }

    static class SingleDFReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int keySum = 0;

            Iterator<IntWritable> iter = values.iterator();

            while (iter.hasNext())
                keySum += iter.next().get();

            context.write(new IntWritable(-keySum),new Text(key.toString()));
        }

    }


    static class SortDFReducer extends Reducer<IntWritable,Text,Text,DefFeatureModel>{
        private int featuresNum;
        private DefFeatureModel featureModel;
        private MapWritable featureMap;

        private int index = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            String featuesNumStr = context.getConfiguration().get("fetureNum");
            featuresNum = Integer.parseInt(featuesNumStr);

            if (featuresNum == 0)
                featuresNum = Integer.MAX_VALUE;

            featureModel = new DefFeatureModel();
            featureMap = new MapWritable();

            System.out.println("sum : "+featuresNum);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Iterator<Text> iter = values.iterator();
            while(index < featuresNum && iter.hasNext()){
                Text outKey = new Text(iter.next().toString());
                IntWritable outValue = new IntWritable(index++);
//                System.out.println(outKey+" : "+key);
                featureMap.put(outKey,outValue);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            featureModel.setFeaturesNum(new IntWritable(featureMap.size()));
            featureModel.setFeaturesMap(featureMap);
            System.out.println(featureModel.getFeaturesNum());
            context.write(new Text("DFSingleFeatreu"),featureModel);
            super.cleanup(context);
        }


    }
}
