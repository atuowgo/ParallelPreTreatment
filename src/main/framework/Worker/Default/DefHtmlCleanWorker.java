package main.framework.Worker.Default;

import main.core.Job.Default.Clean.DefHtmlCleanJob;
import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.DefHtmlCleanOptions;
import main.framework.Options.Options;
import main.framework.Util.LogMsgUtil;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

/**
 * Created by adam on 14-3-9.
 */
public class DefHtmlCleanWorker extends AbstractWorker {
    private DefHtmlCleanOptions cleanOptions;
    private static Log consoleLog = LogUtil.getConsoleLog();
    private final String JobName = "DefHtmlClean";
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public void buildTask(Options options) {
        cleanOptions = (DefHtmlCleanOptions) options;
        cleanOptions.setHtmlTreater(TaskRepository.getNodeAtrr(taskNode, "htmlThreater"));
        cleanOptions.setTrainPecentage(TaskRepository.getNodeAtrr(taskNode,"trainPecentage"));
        consoleLog.debug("threater : "+cleanOptions.getHtmlTreater());
    }

    @Override
    public void startJob() {
        consoleLog.info(LogMsgUtil.startJobMsg(JobName));
        DefHtmlCleanJob job = new DefHtmlCleanJob();
        boolean res = true;
        try {
             res = job.startJob(cleanOptions.optionsValue());
        } catch (InterruptedException e) {
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
