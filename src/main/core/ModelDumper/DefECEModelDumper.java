package main.core.ModelDumper;

import main.core.Model.Token.DefECETermModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by adam on 14-4-17.
 */
public class DefECEModelDumper {

    private DefECEModelDumper(){}

    public static void dumpECEModelFromPath(Path srcPath,FileSystem fs,Configuration conf) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,srcPath,conf);
        Text key = new Text();
        DefECETermModel valueModel = new DefECETermModel();

        int i=0;
        while(reader.next(key,valueModel)){
            System.out.println("key : "+key);
            System.out.println("value : "+valueModel);
            i++;
        }
        System.out.println("size : "+i);
    }

    public static void dumpECEFileFromPath(Path srcPath,FileSystem fs,Configuration conf) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,srcPath,conf);
        Text key = new Text();
        DoubleWritable valueModel = new DoubleWritable();
        int i=0;
        while(reader.next(key,valueModel)){
            System.out.println("key : "+key);
            System.out.println("value : "+valueModel);
            i++;
        }
        System.out.println("size : "+i);
    }
}
