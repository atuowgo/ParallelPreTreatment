package core;

import main.framework.Drive.JobXMlDrive;
import main.framework.Resource.ResourceInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-3-13.
 */
public class HoMain {
    public static void main(String[] args) throws InterruptedException,
            IOException, ClassNotFoundException, InstantiationException,
            IllegalAccessException {

        Properties pathPro = new Properties();
        pathPro.load(new FileInputStream(ResourceInfo.class.getResource("path.properties").getPath()));
        String jobConfPath = pathPro.getProperty("job-conf.path");
        JobXMlDrive drive = new JobXMlDrive(jobConfPath);
//        JobXMlDrive drive = new JobXMlDrive();
        drive.buildTaskTupe();
        drive.startJobTupe();
    }


}
