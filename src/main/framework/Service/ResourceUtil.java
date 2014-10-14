package main.framework.Service;

import main.core.Util.HadoopConfUtil;
import org.apache.hadoop.conf.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-5-1.
 */
public class ResourceUtil {
    private static Configuration conf = HadoopConfUtil.getNewConfiguration();
    private static Properties packPro = new Properties();

    static {
        try {
            packPro.load(new FileInputStream(ResourceUtil.class.getResource("pack.properties").getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private ResourceUtil(){}

    public static Configuration getConfInstance(){
        return conf;
    }

    public static Properties getPackProInstance(){
        return packPro;
    }

    public static void delConfInstance(){
        conf = null;
    }

    public static void delPackProInstance(){
        packPro = null;
    }
}
