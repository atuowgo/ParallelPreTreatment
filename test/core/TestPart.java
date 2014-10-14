package core;


import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by adam on 14-3-13.
 */
public class TestPart {
    public void testPartition() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("/home/adam/dev/hadoop/hadoop12/conf/core-site.xml"));
//        conf.set("mapred.reduce.tasks","2");
//        conf.set("fs.default.name", "hdfs://localhost:9000");
//        conf.set("mapred.job.tracker", "localhost:9001");
//        Job job = HadoopUtil.prepareJob(
//                new Path(HadoopConfUtil.createHdfsUrl("/test")),
//                new Path(HadoopConfUtil.createHdfsUrl("/testout/t.txt")),
//                TextInputFormat.class,
//                MyMapper.class,
//                LongWritable.class,
//                Text.class,
//                MyRecude.class,
//                Text.class,
//                IntWritable.class,
//                TextOutputFormat.class,
//                HadoopConfUtil.getNewConfiguration()
//        );
        Job job = new Job(conf);

        job.setJarByClass(TestPart.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyRecude.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(HadoopConfUtil.createHdfsUrl("/test")));
        FileOutputFormat.setOutputPath(job, new Path(HadoopConfUtil.createHdfsUrl("/testout3")));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(3);
        job.setPartitionerClass(MyPartioins.class);

          job.submit();
        job.waitForCompletion(true);
    }

    public static class MyPartioins extends Partitioner<Text,IntWritable> {

        List<Text> list = new ArrayList<Text>();

        @Override
        public int getPartition(Text key,IntWritable value, int i) {
            System.out.println(key.toString()+" : "+value+" : "+i);
            if (key.toString().equals("d")||key.toString().equals("add"))
                return 1;
            else return 0;
        }
    }

    public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String v = value.toString();
            String[] vs = v.split(" ");
            context.write(new Text(vs[0]),new IntWritable(vs.length));
        }
    }

    public static class MyRecude extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();

            while (iter.hasNext()){
                sum += iter.next().get();
            }

            context.write(key,new IntWritable(sum));
        }
    }
}
