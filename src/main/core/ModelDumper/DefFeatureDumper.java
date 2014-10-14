package main.core.ModelDumper;

import main.core.Model.Feature.DefFeatureModel;
import main.core.Model.Token.DefIDFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by adam on 14-4-17.
 */
public class DefFeatureDumper {
    private DefFeatureDumper(){}

    public static DefFeatureModel getFeatureInstace(Path srcPath,Configuration conf) throws IOException {
        Text key = new Text();
        DefFeatureModel model = new DefFeatureModel();

        SequenceFile.Reader reader = new SequenceFile.Reader(srcPath.getFileSystem(conf),srcPath,conf);

        reader.next(key,model);

        return  model;
    }

    public static void addFeatureIdf(Path featurePath,Path idfPath,Configuration conf) throws IOException {
        DefIDFModel idfModel = DefIDFModelDumper.getIDFInstance(idfPath,conf);
        DefFeatureModel featureModel = getFeatureInstace(featurePath,conf);
        
    }

    public void persistendModel(DefFeatureModel model,Path disPath,Configuration conf,FileSystem fs) throws IOException {
        SequenceFile.Writer writer = new SequenceFile.Writer(fs,conf,disPath,Text.class,DefFeatureModel.class);
        writer.append(new Text(),model);
        writer.syncFs();
        writer.close();
    }

    public static void dumpFeature(DefFeatureModel model,OutputStream out,boolean closeOut) throws IOException {
        MapWritable featureMap = model.getFeaturesMap();

        strToByte(model.toString()+"\n",out);
        MapWritableDumper.mapDumper(featureMap,out,false);

        if (closeOut)
            out.close();
    }

    public static void dumpFromPath(Path srcPath,Configuration conf) throws IOException {
        DefFeatureModel model = getFeatureInstace(srcPath,conf);
        dumpFeature(model,System.out,false);
    }

    private static void strToByte(String str,OutputStream out) throws IOException {
        out.write(str.getBytes());
    }
}
