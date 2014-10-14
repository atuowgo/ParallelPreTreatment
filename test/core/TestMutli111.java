package core;

import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by adam on 14-3-16.
 */
public class TestMutli111 {

    public void testMulti() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = HadoopUtil.prepareJob(
                new Path(HadoopConfUtil.createHdfsUrl("/test/in/prehadoop/test")),
                new Path(HadoopConfUtil.createHdfsUrl("/test/out/prehadoop/")),
                TextInputFormat.class,
                MyMapper.class,
                IntWritable.class,
                Text.class,
                MyReducer.class,
                IntWritable.class,
                Text.class,
                TextOutputFormat.class,
                HadoopConfUtil.getNewConfiguration()
        );

        job.setPartitionerClass(MyPartitioner.class);

        MultipleOutputs.addNamedOutput(job, "part0", TextOutputFormat.class, Object.class, Text.class);
        MultipleOutputs.addNamedOutput(job,"part1",TextOutputFormat.class,Object.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"part2",TextOutputFormat.class,Object.class,Text.class);
        MultipleOutputs.addNamedOutput(job,"part3",TextOutputFormat.class,Object.class,Text.class);

        job.waitForCompletion(true);
    }


    static class MyPartitioner extends Partitioner<IntWritable,Text> {

        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {
            System.out.println(intWritable+" : "+text+" : "+i);
            if (i==1)
                return 0;
            else
                return intWritable.get();
        }
    }

    static class MyMapper extends Mapper<Object,Text,IntWritable,Text> {
//        private MultipleOutputs multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
//            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            multipleOutputs.close();
            super.cleanup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().trim().equals("")){
                String[] v = value.toString().split("=");

                context.write(new IntWritable(Integer.parseInt(v[0])),new Text(v[1]));
//                if (v[0].equals("0")) {
//                    multipleOutputs.write("part0", new IntWritable(Integer.parseInt(v[0])), new Text(v[1]));
//                }
//
//                if (v[0].equals("1")) {
//                    multipleOutputs.write("part1", new IntWritable(Integer.parseInt(v[0])), new Text(v[1]));
//                }
//
//                if (v[0].equals("2")) {
//                    multipleOutputs.write("part2", new IntWritable(Integer.parseInt(v[0])), new Text(v[1]));
//                }
//
//                if (v[0].equals("3")) {
//                    multipleOutputs.write("part3", new IntWritable(Integer.parseInt(v[0])), new Text(v[1]));
//                }



            }


        }
    }

    static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        private MultipleOutputs multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            multipleOutputs = new MultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
            super.cleanup(context);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            System.out.println("reduce : "+key.get());
            Iterator<Text> t = values.iterator();

            while (t.hasNext()){
                Text text = new Text();
                text.set("reduce"+t.next().toString());
//                context.write(key,text);

                if (key.get()==0) {
                    multipleOutputs.write("part0", key, text);
                }

                if (key.get()==1) {
                    multipleOutputs.write("part1", key, text);                }

                if (key.get()==2) {
                    multipleOutputs.write("part2", key, text);                }

                if (key.get()==3) {
                    multipleOutputs.write("part3", key, text);
                }
            }
        }
    }
}
