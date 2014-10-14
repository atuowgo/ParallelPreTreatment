package main.core.ModelDumper;

import main.core.Model.Token.DefIDFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by adam on 14-4-20.
 */
public class DefIDFModelDumper {
    private DefIDFModelDumper(){}

    public static DefIDFModel getIDFInstance(Path idfModelPath,Configuration conf)
            throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(idfModelPath.getFileSystem(conf),idfModelPath,conf);
        IntWritable key = new IntWritable();
        DefIDFModel value = new DefIDFModel();

        reader.next(key,value);
        reader.close();
        return value;
    }

    public static void persistenceMode(DefIDFModel model,Path disPath,Configuration conf)
            throws IOException {
        SequenceFile.Writer writer = new SequenceFile.Writer(disPath.getFileSystem(conf),conf,disPath,
                IntWritable.class, DefIDFModel.class);

        writer.append(model.getClazzIndex(),model);
        writer.syncFs();
        writer.close();
    }

    public static void dumpInstace(DefIDFModel model,OutputStream out,boolean closeOut)
            throws IOException {
        MapWritable idfMap = model.getTermsIDF();

        strToByte(idfMap.toString()+"\n",out);
        MapWritableDumper.mapDumper(idfMap,out,false);

        if (closeOut)
            out.close();
    }

    public static void dumpFromPath(Path srcPath,Configuration conf) throws IOException {
        FileSystem fs = srcPath.getFileSystem(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, srcPath,conf);

        Text key = new Text();
        IntWritable value = new IntWritable();

        while(reader.next(key,value)){
            System.out.println(key+" : "+value);
        }
    }

    private static void strToByte(String str,OutputStream out)
            throws IOException {
        out.write(str.getBytes());
    }
}
