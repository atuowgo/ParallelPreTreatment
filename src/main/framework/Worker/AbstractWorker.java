package main.framework.Worker;

import main.framework.Drive.TaskRepository;
import main.framework.Options.Options;
import org.dom4j.Node;

/**
 * Created by adam on 14-2-18.
 */
public abstract class AbstractWorker implements Worker{
    private Options options;
    protected Node taskNode;

    @Override
    public void work() {
        wrapPreOptions();
        buildTask(options);
        startJob();
    }

    public void wrapPreOptions(){
        options.setTaskName(TaskRepository.getNodeAtrr(taskNode,"name"));
        options.setInputPath(TaskRepository.getNodeAtrr(taskNode,"inputPath"));
        options.setTmpPath(TaskRepository.getNodeAtrr(taskNode,"tmpPath"));
        options.setOutputPath(TaskRepository.getNodeAtrr(taskNode,"outputPath"));
    }

    public abstract void buildTask(Options options);

    public abstract void startJob();

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public Node getTaskNode() {
        return taskNode;
    }

    public void setTaskNode(Node taskNode) {
        this.taskNode = taskNode;
    }
}
