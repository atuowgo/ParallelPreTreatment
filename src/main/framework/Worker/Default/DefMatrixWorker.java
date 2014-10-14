package main.framework.Worker.Default;

import main.core.Job.Default.Matrix.DefMatrixJob;
import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.DefMatrixOptions;
import main.framework.Options.Options;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by adam on 14-3-19.
 */
public class DefMatrixWorker extends AbstractWorker {
    private DefMatrixOptions matrixOptions;
    private static Log consoleLog = LogUtil.getConsoleLog();
    private static Log errorLog = LogUtil.getErrorLog();
    @Override
    public void buildTask(Options options) {
        matrixOptions = (DefMatrixOptions)options;
        matrixOptions.setFeaturePath(TaskRepository.getNodeAtrr(taskNode,"featurePath"));
    }

    @Override
    public void startJob() {
        DefMatrixJob job = new DefMatrixJob();

        boolean res = true;
        try {
            res = job.startJob(matrixOptions.optionsValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
            errorLog.error(e);
        } catch (IOException e) {
            e.printStackTrace();
            errorLog.error(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            errorLog.error(e);
        }

        if (!res){
            consoleLog.error("作业存在错误 。。。。");
            return;
        }
    }
}
