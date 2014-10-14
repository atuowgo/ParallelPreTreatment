package main.core.ModelDumper;

import main.core.Model.Matrix.DefMatrixKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created by adam on 14-5-1.
 */
public class DefMatrixDumper {

    private DefMatrixDumper(){}

    public static void dumpFromPath(Path path,Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        DefMatrixKey key = new DefMatrixKey();
        VectorWritable value = new VectorWritable();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,path,conf);
        while (reader.next(key,value)){
            Vector vector = value.get();
            System.out.println("key : "+key);
            System.out.println("value : "+vector);
        }
    }
}
