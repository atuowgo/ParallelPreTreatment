package main.framework.Worker.Default;

import main.core.Job.Default.NB.DefNBJob;
import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.DefNBOptinos;
import main.framework.Options.Options;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by adam on 14-3-22.
 */
public class DefNBWorker extends AbstractWorker {
    private DefNBOptinos nbOptinos;
    private static Log consoleLog = LogUtil.getConsoleLog();

    @Override
    public void buildTask(Options options) {
        nbOptinos = (DefNBOptinos) options;

        nbOptinos.setTestTermPath(TaskRepository.getNodeAtrr(taskNode,"testTermPath"));
        nbOptinos.setLocalOutputPath(TaskRepository.getNodeAtrr(taskNode,"localOutputPath"));
        nbOptinos.setMarkStr(TaskRepository.getNodeAtrr(taskNode,"markStr"));
    }

    @Override
    public void startJob() {
        DefNBJob nbJob = new DefNBJob();
        boolean res = true;
        try {
            res= nbJob.startJob(nbOptinos.optionsValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        if (!res){
            consoleLog.error("NB分类出错 。。。。 ");
            return;
        }

        consoleLog.info("NB分类完成 。。。。 ");
    }
}
