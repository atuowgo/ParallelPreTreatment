package main.core.ModelDumper;

import main.core.Model.Clean.DefHtmlModel;
import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Created by adam on 14-3-12.
 */
public class DefHtmlModelDumper {

    private DefHtmlModelDumper(){}

    public static void dumpModelFromPath(Path srcPath,Configuration conf) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(srcPath.getFileSystem(conf),
                srcPath,HadoopConfUtil.getNewConfiguration());
        IntWritable fileName = new IntWritable();
        DefHtmlModel model = new DefHtmlModel();

        while (reader.next(fileName,model)){
            System.out.println("类别标号 : "+fileName);
            System.out.println("model : "+model);
        }
    }
}
