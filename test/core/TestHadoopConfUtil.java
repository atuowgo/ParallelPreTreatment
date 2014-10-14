package core;

import main.core.Util.HadoopConfUtil;
import org.junit.Test;

/**
 * Created by adam on 14-3-10.
 */
public class TestHadoopConfUtil {

    @Test
    public void test01(){
        System.out.println(HadoopConfUtil.getHadoopHdfsUrl());
        System.out.println(HadoopConfUtil.getHadoopHomePath());
    }
}
