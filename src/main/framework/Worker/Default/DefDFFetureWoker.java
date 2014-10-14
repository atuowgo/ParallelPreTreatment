package main.framework.Worker.Default;

import main.core.Job.Default.Feature.DefDFSingleJob;
import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.DefDFFetureOptions;
import main.framework.Options.Options;
import main.framework.Util.LogMsgUtil;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by adam on 14-3-16.
 */
public class DefDFFetureWoker extends AbstractWorker {
    private DefDFFetureOptions fetureOptions ;
    private final String JobName = "defDFFeture";
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public void buildTask(Options options) {
        fetureOptions = (DefDFFetureOptions) options;
        fetureOptions.setFetureNum(TaskRepository.getNodeAtrr(taskNode,"fetureNum"));
    }

    @Override
    public void startJob() {
        consoleLog.info(LogMsgUtil.startJobMsg(JobName));

//        DefDFFetureJob fetureJob = new DefDFFetureJob();
        DefDFSingleJob fetureJob = new DefDFSingleJob();
        boolean res = true;
        try {
             res = fetureJob.startJob(fetureOptions.optionsValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
            errorLog.error(e);
            res = false;
        } catch (IOException e) {
            e.printStackTrace();
            errorLog.error(e);
            res = false;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            errorLog.error(e);
            res = false;
        }

        if (!res)
            consoleLog.info(JobName+" : 失败");
        else
            consoleLog.info(LogMsgUtil.endJobMsg(JobName));
    }
}
