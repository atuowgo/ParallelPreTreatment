package main.framework.Util;

/**
 * Created by adam on 14-3-10.
 */
public class LogMsgUtil {

    public static String startJobMsg(String jobName){
        return "Job : "+jobName+" 开始";
    }

    public static String endJobMsg(String jobName){
        return "JOb : "+jobName+" 结束";
    }
}
