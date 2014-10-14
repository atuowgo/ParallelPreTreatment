package main.framework.Worker.Default;

import main.core.Job.Default.Feature.DefECEFeatureJob;
import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.DefECEFeatureOptions;
import main.framework.Options.Options;
import main.framework.Util.LogMsgUtil;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by adam on 14-4-16.
 */
public class DefECEFeatureWoker extends AbstractWorker {
    private DefECEFeatureOptions options;
    private final String JobName = "defECEFeature";
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public void buildTask(Options options) {
        this.options = (DefECEFeatureOptions) options;
        this.options.setFeatureNum(TaskRepository.getNodeAtrr(taskNode,"featureNum"));
    }

    @Override
    public void startJob() {
        consoleLog.info(LogMsgUtil.startJobMsg(JobName));
        DefECEFeatureJob featureJob = new DefECEFeatureJob();
        boolean res = true;
        try {
            res = featureJob.startJob(options.optionsValue());
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
