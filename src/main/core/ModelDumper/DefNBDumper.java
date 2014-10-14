package main.core.ModelDumper;

import main.core.Model.NB.DefNBKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by adam on 14-5-1.
 */
public class DefNBDumper {

    private DefNBDumper(){}

    public static void dumpModelFromPath(Path path,Configuration configuration) throws IOException {
        FileSystem fs = path.getFileSystem(configuration);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,configuration);

        DefNBKey key = new DefNBKey();
        VectorWritable value = new VectorWritable();

        reader.next(key, value);
        System.out.print(key+" : "+value);
    }

//    public static void dumAggreModelFromPath(Path srcPath,Path percentagePath,Configuration conf) throws IOException {
//        FileSystem fs = srcPath.getFileSystem(conf);
//
//        DefNBAggreateModel model = DefNBAggreateModel.getInstanceFromPath(srcPath,percentagePath,conf,fs);
//
//        System.out.println(model);
//    }
}
