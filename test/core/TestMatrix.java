package core;

import main.core.ModelDumper.DefMatrixDumper;
import main.core.Util.HadoopConfUtil;
import main.core.Util.LabelProUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * Created by adam on 14-3-19.
 */
public class TestMatrix {

    @Test
    public void testMatrix() throws IOException {

        JobConf job = new JobConf();
        job.setInputFormat(CompositeInputFormat.class);
        job.set("mapred.join.expr", CompositeInputFormat.compose(
                "outer", SequenceFileInputFormat.class, new Path(HadoopConfUtil.createHdfsUrl("/test/left")),
                new Path(HadoopConfUtil.createHdfsUrl("/test/right/"))));
        job.setOutputFormat(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(HadoopConfUtil.createHdfsUrl("/test/out")));
        job.setMapperClass(MatrixMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
//        job.setPartitionerClass(MatrixPartitioner.class);

        JobClient.runJob(job);
    }

    @Test
    public void testPre() throws IOException {
        Scanner scanner1 = new Scanner(new File("/home/adam/dev/tmp/ttt1"));
        Scanner scanner3 = new Scanner(new File("/home/adam/dev/tmp/ttt3"));

        Scanner scanner2 = new Scanner(new File("/home/adam/dev/tmp/ttt2"));
        Scanner scanner4 = new Scanner(new File("/home/adam/dev/tmp/ttt4"));

        Path path1 = new Path(HadoopConfUtil.createHdfsUrl("/test/left/t1"));
        Path path3 = new Path(HadoopConfUtil.createHdfsUrl("/test/left/t3"));

        Path path2= new Path(HadoopConfUtil.createHdfsUrl("/test/right/t2"));
        Path path4 = new Path(HadoopConfUtil.createHdfsUrl("/test/right/t4"));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = path1.getFileSystem(conf);

        SequenceFile.Writer writer1 = new SequenceFile.Writer(fs,conf,path1,IntWritable.class,Text.class);
        SequenceFile.Writer writer3 = new SequenceFile.Writer(fs,conf,path3,IntWritable.class,Text.class);

        SequenceFile.Writer writer2 = new SequenceFile.Writer(fs,conf,path2,IntWritable.class,Text.class);
        SequenceFile.Writer writer4 = new SequenceFile.Writer(fs,conf,path4,IntWritable.class,Text.class);

        put(scanner1,writer1);
        put(scanner3,writer3);
        put(scanner2,writer2);
        put(scanner4,writer4);

    }



    public void put(Scanner scanner,SequenceFile.Writer writer) throws IOException {
        String line = null;
        String[] parseLine = null;
        while (scanner.hasNext()){
            line = scanner.nextLine();
            parseLine = line.split(" ");
            System.out.println(parseLine[0]+ " : "+parseLine[1]);
            writer.append(new IntWritable(Integer.parseInt(parseLine[0])),new Text(parseLine[1]));
        }

        writer.close();
    }

    public static class MatrixPartitioner implements Partitioner<IntWritable, Text> {

        @Override
        public int getPartition(IntWritable key, Text value, int numPartitions) {
            System.out.println("partitioner : "+key+" : "+value);
            return 0;
        }

        @Override
        public void configure(JobConf job) {

        }
    }

    public static class MatrixMapper extends MapReduceBase implements
            Mapper<IntWritable,TupleWritable,IntWritable,Text>{

        @Override
        public void configure(JobConf job) {
            super.configure(job);
        }

        @Override
        public void map(IntWritable key, TupleWritable value,
                        OutputCollector<IntWritable, Text> output, Reporter reporter)
                throws IOException {
//            FileSplit split = (FileSplit) reporter.getInputSplit();
//            System.out.println("fileName : "+split.getPath().getName());
            System.out.println("key : "+key);
            System.out.println(value.get(0)+" : "+value.get(1));
            System.out.println(" ----------------------------- ");
            Text text = new Text();
            text.set(value.get(0).toString()+" : "+value.get(1).toString());
            output.collect(key, text);
        }
    }

    @Test
    public void testParsePartitiner(){
        String a = "0-adsfasfasfasdfaa";
        String a1 = "1-adsfasfasfasdfaa";

        assert(0== LabelProUtil.parseMatrixPartitioner(a,"-"));
        assert(1== LabelProUtil.parseMatrixPartitioner(a1,"-"));

        assert("adsfasfasfasdfaa".equals(LabelProUtil.parseMatrixFileName(a, "-")));
    } 
    @Test
    public void testParseR(){
        String a = "part-r-00000";
        String b = "part-r-00001";

        assert("00000".equals(LabelProUtil.parseStrR(a)));
        assert("00001".equals(LabelProUtil.parseStrR(b)));
    }

    @Test
    public void testVector(){
        double[] d = {1,2,0,0,1,0,0,2,0,0,0,4};
        Vector vector = new DenseVector(d);
        Vector vector1 = new RandomAccessSparseVector(vector);


        Vector vector2 = new DenseVector(12);

        System.out.println(vector);
        System.out.println(vector1);

        vector2.set(0,2);
        vector2.set(3,0);
        vector2.set(11,2);

        System.out.println(vector2);

        RandomAccessSparseVector vector3 = new RandomAccessSparseVector(vector);
        System.out.println(vector.plus(vector3));

    }

    @Test
    public void testReadMatrix() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defMatrix/test/part-r-00002"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
        DefMatrixDumper.dumpFromPath(path,conf);
//        FileSystem fs = path.getFileSystem(conf);
//
//        DefMatrixKey key = new DefMatrixKey();
//        VectorWritable value = new VectorWritable();
//
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
//
//        int i = 0;
//
////        reader.next(key,value);
////
////        System.out.println("key : "+key);
////        System.out.println("value : "+value);
////
////        Vector vector = value.get();
////
////        vector.assign(new DoubleFunction() {
////            @Override
////            public double apply(double x) {
////                System.out.print(x+" ");
////                return x;
////            }
////        });
//
////        Vector vector = value.get();
//
////        vector = vector.assign(new DoubleFunction() {
////            @Override
////            public double apply(double x) {
////                System.out.print(x +"  ");
////                return x+1;
////            }
////        });
//
////        System.out.println(vector);
//
//
//        Vector vector2 = new DenseVector();
//        while (reader.next(key,value)){
//            Vector vector = value.get();
////            vector2 = vector;
//            System.out.println("key : "+key);
////            System.out.println("size : "+vector.getNumNonZeroElements());
//            System.out.println("value : "+vector);
////            System.out.println(" ------------------------ ");
//            i++;
//        }
//
//        System.out.println("-----------------------");
////        System.out.println((DenseVector)vector2);
//
//        System.out.println(i);
    }

    @Test
    public void testVectorVolumn() throws IOException {
        double[] v = {1.0,2.0,0,0,0,3.3,4.1,0,0,0,4};
        Vector DenseVectr = new DenseVector(v);
        Vector RanVector = new RandomAccessSparseVector(DenseVectr);

        Path dpath = new Path(HadoopConfUtil.createHdfsUrl("/test/in/dVector"));
        Path rpath = new Path(HadoopConfUtil.createHdfsUrl("/test/in/rVector"));

        Configuration conf = HadoopConfUtil.getNewConfiguration();
        FileSystem fs = dpath.getFileSystem(conf);

        SequenceFile.Writer dWriter = new SequenceFile.Writer(fs,conf,dpath,Text.class,VectorWritable.class);
        SequenceFile.Writer rWriter = new SequenceFile.Writer(fs,conf,rpath,Text.class,VectorWritable.class);

        for(int i = 0;i<2000;i++){
            dWriter.append(new Text(i+""),new VectorWritable(DenseVectr));
            rWriter.append(new Text(i+""),new VectorWritable(RanVector));
        }

        dWriter.close();
        rWriter.close();
    }

    @Test
    public void testRandomAss(){
        double[] v =
                {1,2,3,0,0,3,0,0,4,0,0,5,0,0,0,0,0,0,0,0,0,0,0,0,0,9,0,0,0,0,0,0,0,0,0,0,0,0,0,
                 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,99,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                 0,0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,77};
        Vector vector = new RandomAccessSparseVector(new DenseVector(v));

        System.out.print(vector);
    }

    @Test
    public void testT(){
        Vector vector = new DenseVector(13);
        vector.set(10,9.8);
        vector.set(1,9);
        vector.set(11,8);
        vector.set(3,8);

        Vector vector1 = new RandomAccessSparseVector(vector);

        System.out.println(vector1);
    }


}
