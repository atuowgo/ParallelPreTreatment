package framework.driven;

import main.framework.Drive.JobXMlDrive;
import org.junit.Test;

/**
 * Created by adam on 14-2-19.
 */
public class JobXmlDriveTest {

    @Test
    public void testFrame() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        JobXMlDrive drive = new JobXMlDrive("job-conf.xml");
        drive.buildTaskTupe();
        drive.startJobTupe();

    }
}
