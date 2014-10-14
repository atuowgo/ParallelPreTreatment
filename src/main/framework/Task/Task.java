package main.framework.Task;

import main.framework.Worker.Worker;

/**
 * Created by adam on 14-2-18.
 */
public interface Task {
    public void execute(Worker worker);
}
