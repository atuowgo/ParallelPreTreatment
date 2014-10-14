package core;

import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

/**
 * Created by adam on 14-3-16.
 */
public class TestSort {

    public static void testSort() throws IOException, ClassNotFoundException, InterruptedException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/test/in/prehadoop/test"));
        Path outPath = new Path(HadoopConfUtil.createHdfsUrl("/test/out/prehadoop/"));

        Job job = HadoopUtil.prepareJob(
                path,
                outPath,
                TextInputFormat.class,
                RandomMapper.class,
                LongWritable.class,
                Text.class,
                RandomReduce.class,
                Text.class,
                LongWritable.class,
                TextOutputFormat.class,
                HadoopConfUtil.getNewConfiguration()
        );

        job.waitForCompletion(true);
    }

    static class RandomMapper extends Mapper<Object,Text,LongWritable,Text>{
        Random random = new Random();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Text text = new Text();
            text.set(new Date().toString());

            for(int i=0;i<100000;i++)
                context.write(new LongWritable(random.nextInt(10000)*(-1)),text);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
    }

    static class RandomReduce extends Reducer<LongWritable,Text,Text,LongWritable>{
        private final int totalNum = 50000;
        private int num = 0;

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> v = values.iterator();
            long n;
            while(num<totalNum && v.hasNext()){
                n = key.get();
                context.write(v.next(),new LongWritable(n*(-1)));
                num++;
            }
        }
    }
}
