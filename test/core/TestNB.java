package core;

import main.core.ModelDumper.DefNBDumper;
import main.core.Util.HadoopConfUtil;
import main.core.Util.LabelProUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleFunction;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by adam on 14-3-22.
 */
public class TestNB {

    @Test
    public void testVector(){
        double[] d = {0,0,2,2,2,1,2.1,0,0,0,0,0,6};
        Vector v = new DenseVector(d);
        Vector vector = new RandomAccessSparseVector(v);

        System.out.println(vector);
        vector.assign(new DoubleFunction() {
            int i=0;
            @Override
            public double apply(double x) {
                System.out.println(++i);
                if (x!=0)
                    return 1;
                else return 0;
            }
        });
        System.out.println(vector);

        vector = vector.divide(2.0);
        System.out.println(vector);

        vector = vector.plus(vector);

        System.out.println(vector);

//        Vector anotherVector;
//        anotherVector.assign(v);
//
//        System.out.println(anotherVector);

//        Vector anotherV = new RandomAccessSparseVector();
//        anotherV.set(4,0.9);
//        System.out.println(anotherV);

        String p = "part-r-00001";
        String indexLabel = LabelProUtil.parseStrR(p);
        System.out.println(indexLabel);
        System.out.println(LabelProUtil.reParseLableInt(indexLabel));
    }

    @Test
    public void testReadModel() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/model/part-r-00000"));
        Configuration configuration = HadoopConfUtil.getNewConfiguration();
        DefNBDumper.dumpModelFromPath(path,configuration);
//        FileSystem fs = path.getFileSystem(configuration);
//
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,configuration);
//
//        DefNBKey key = new DefNBKey();
//        VectorWritable value = new VectorWritable();
//
//
//        reader.next(key, value);
//        System.out.println(value.get().zSum());
//
//        System.out.print(key+" : "+value);

    }

    @Test
    public void testNMTarget(){
        double[] d = {0.3,0.11,2,0,0,0,22,0.9,0,0.1};
        Vector v = new DenseVector(d);



        v.assign(new DoubleFunction() {
            int index = 0;
            @Override
            public double apply(double x) {
                System.out.print(index++ + " : "+x +"   ");
                return x;
            }
        });

        System.out.println();
        System.out.println(v.size()+" : "+v);

        Iterator<Vector.Element> nonZero = v.nonZeroes().iterator();

        Vector.Element e = null;
        while (nonZero.hasNext()){
            e = nonZero.next();

            System.out.println(e.index()+ " : "+e.get());
        }



    }

    @Test
    public void testTargetClazz(){
        double[] srcD = {0,0.4,1,4,0,0,9};
        double[] srcD2 = {0.3,0.4,1,4,0,0,9};
        double[] model = {0.62,0.60,0.5,0.49,0.41,0.1,0.1};
        double[] model2 = {0.65,0.48,0.45,0.39,0.21,0.1,0.1};

        Vector srcV = new DenseVector(srcD);
        Vector srcV2 = new DenseVector(srcD2);
        Vector modeV = new DenseVector(model);
        Vector modeV2 = new DenseVector(model2);

        Vector[] s_v = {srcV,srcV2};
        Vector[] m_v = {modeV,modeV2};
        double[] pD = {0.47,0.53};
//        System.out.println(NBTargetClazzTreater.findTargetClazz(s_v,m_v,pD));
    }

//    @Test
//    public void testAggreate() throws IOException {
//        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/aggreateModel"));
//        Configuration conf = HadoopConfUtil.getNewConfiguration();
//        FileSystem fs = path.getFileSystem(conf);
//
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
//
//        Text key = new Text();
//        DefNBAggreateModel value = new DefNBAggreateModel();
//
//        reader.next(key,value);
//
//        for(int i=0;i<value.getClazzList().get().length;i++){
//            DoubleWritable p = new DoubleWritable();
//            Text c = new Text();
//            VectorWritable v = new VectorWritable();
//
//            System.out.println(value.getPercentageList().get()[i]+" : "+value.getClazzList().get()[i]+" : "+value.getModelList().get()[i]);
//        }
//
//    }

    @Test
    public void testNewAggre() throws IOException {
        Path path = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/evaluate/evaluateModel"));
        Path perPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/extern/percentagePro"));
//        Path persistPath = new Path(HadoopConfUtil.createHdfsUrl("/ParallelPreTreatment/defNB/aggreateModel/nbModel"));
        Configuration conf = HadoopConfUtil.getNewConfiguration();
//        DefNBDumper.dumAggreModelFromPath(path,perPath,conf);
//        FileSystem fs = path.getFileSystem(conf);
//
//        DefNBAggreateModel model = DefNBAggreateModel.getInstanceFromPath(perPath,  path,conf,fs);
//
//        System.out.println(model);

//        DefNBAggreateModel.persistModel(persistPath,fs,conf,model);
    }
}
