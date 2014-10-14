package main.core.Job;

import java.io.IOException;

/**
 * Created by adam on 14-3-10.
 */
public interface TaskJob {

    public boolean startJob(String optionsValue) throws InterruptedException, IOException, ClassNotFoundException;
}
