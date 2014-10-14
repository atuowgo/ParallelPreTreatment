package main.framework.Worker.Default;

import main.core.Job.Default.Token.DefHtmlTokenJob;
import main.core.Job.TaskJob;
import main.core.Util.LogUtil;
import main.framework.Options.Default.DefHtmlTokenOptions;
import main.framework.Options.Options;
import main.framework.Util.LogMsgUtil;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by adam on 14-3-12.
 */
public class DefHtmlTokenWorker extends AbstractWorker
{
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();
    private DefHtmlTokenOptions tokenOptions;
    private static final String taskName = "defToken";
    @Override
    public void buildTask(Options options) {
        tokenOptions = (DefHtmlTokenOptions) options;
    }

    @Override
    public void startJob() {
        consoleLog.info(LogMsgUtil.startJobMsg(taskName));
        TaskJob job = new DefHtmlTokenJob();
        boolean res = true;
        try {
            res = job.startJob(tokenOptions.optionsValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        consoleLog.info(LogMsgUtil.endJobMsg(taskName));
    }
}
