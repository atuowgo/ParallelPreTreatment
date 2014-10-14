package main.core.ModelDumper;

import main.core.Model.Token.DefFileTermsModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by adam on 14-4-13.
 */
public class DefFileTermsDumper {

    private DefFileTermsDumper(){}

    public static void dumpFromPath(Path srcPath,Configuration conf) throws IOException {
        FileSystem fs = srcPath.getFileSystem(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, srcPath,conf);

        IntWritable key = new IntWritable();
        DefFileTermsModel value = new DefFileTermsModel();
        MapWritable mapWritable = null;

        while(reader.next(key,value)){
            System.out.println("key : "+key.get());
            System.out.println("size : "+value.getTermsNum());
            System.out.println(value);
            mapWritable = value.getFileTerms();

            for(Map.Entry<Writable, Writable> e : mapWritable.entrySet()){
                System.out.println(e.getKey()+" : "+e.getValue());
            }
            System.out.println(">>> ---------------------------------------- <<<");
        }

    }
}
