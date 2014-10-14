package main.framework.Task;

import main.framework.Worker.Worker;

/**
 * Created by adam on 14-2-18.
 */
public abstract class AbstractTask implements Task{
    private String taskName;

    @Override
    public void execute(Worker worker) {
        worker.work();
    }

}
