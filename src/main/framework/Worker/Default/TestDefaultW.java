package main.framework.Worker.Default;

import main.core.Util.LogUtil;
import main.framework.Drive.TaskRepository;
import main.framework.Options.Default.TestDefaultOp;
import main.framework.Options.Options;
import main.framework.Worker.AbstractWorker;
import org.apache.commons.logging.Log;

/**
 * Created by adam on 14-2-19.
 */
public class TestDefaultW extends AbstractWorker {
    private TestDefaultOp testDefaultW;
    private static Log consoleLog = LogUtil.getConsoleLog();
    @Override
    public void buildTask(Options options) {
        consoleLog.debug("test build task method ... ");
        testDefaultW = (TestDefaultOp) options;
        consoleLog.debug("test : "+options.getInputPath());
        testDefaultW.setAge(TaskRepository.getNodeAtrr(taskNode,"age"));

    }

    @Override
    public void startJob() {
        consoleLog.debug("test start job method .. ");
        consoleLog.debug("test : "+testDefaultW.getAge());
        consoleLog.debug("test : job end ... ");
    }
}
