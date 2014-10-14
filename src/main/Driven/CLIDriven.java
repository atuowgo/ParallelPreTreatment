package main.Driven;

import main.framework.Drive.JobXMlDrive;
import main.framework.JobRepo.JobRepo;
import main.framework.Resource.ResourceInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-4-19.
 */
public class CLIDriven {

    public static void main(String[] args)
            throws IllegalAccessException,
            InstantiationException, ClassNotFoundException, IOException {
        int argsLen = args.length;
        String jobConfPath=null;
        if (argsLen == 0) {
            Properties pathPro = new Properties();
            pathPro.load(new FileInputStream(ResourceInfo.class.getResource("path.properties").getPath()));
            String defConfPath = JobRepo.class.getResource("job-conf.xml").getPath();
            jobConfPath = pathPro.getProperty("job-conf.path",defConfPath);
        }else if (argsLen == 1){
            jobConfPath = args[0];
        }else{
            System.out.println("请输入正确的配置路径");
            return;
        }

        JobXMlDrive drive = new JobXMlDrive(jobConfPath);
        drive.buildTaskTupe();
        drive.startJobTupe();
    }
}
