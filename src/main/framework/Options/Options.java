package main.framework.Options;

/**
 * Created by adam on 14-2-18.
 */
public abstract class Options {
    private String taskName;
    private String inputPath;
    private String tmpPath;
    private String outputPath;

    public String frontValue(){
        String out = "";
        out += "taskName="+this.taskName+";"
              +"inputPath="+this.inputPath+";"
              +"tmpPath="+this.tmpPath+";"
              +"outputPath="+this.outputPath;
        return out;
    }
    public abstract String optionsValue();

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getTmpPath() {
        return tmpPath;
    }

    public void setTmpPath(String tmpPath) {
        this.tmpPath = tmpPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
}
