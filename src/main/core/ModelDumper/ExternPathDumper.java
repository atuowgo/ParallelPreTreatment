package main.core.ModelDumper;

import main.core.Util.LabelProUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

/**
 * Created by adam on 14-4-16.
 */
public class ExternPathDumper {
    private ExternPathDumper(){}

    public static Properties getPropertiesInstaceByPath(Path proPath,FileSystem fs) throws IOException {
        Properties properties = new Properties();
        properties.load(fs.open(proPath));

        return properties;
    }

    public static void dumpTestNumPro(Properties properties,OutputStream out,int clazzNum,boolean closeOut) throws IOException {
        strToBytes("测试集数目如下\n",out);
        strToBytes("[",out);
        for(int i=0;i<clazzNum;i++){
                strToBytes(LabelProUtil.parseLabelStr(i)+" ",out);
           }
        strToBytes("]",out);

        if (closeOut)
            out.close();
    }

    private static void strToBytes(String str,OutputStream out) throws IOException {
        out.write(str.getBytes());
    }
}
