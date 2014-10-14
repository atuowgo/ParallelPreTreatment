package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-3-22.
 */
public class DefNBOptinos extends Options {
    private String testTermPath;
    private String localOutputPath;
    private String markStr;

    public String getTestTermPath() {
        return testTermPath;
    }

    public void setTestTermPath(String testTermPath) {
        this.testTermPath = testTermPath;
    }

    public String getLocalOutputPath() {
        return localOutputPath;
    }

    public void setLocalOutputPath(String localOutputPath) {
        this.localOutputPath = localOutputPath;
    }

    public String getMarkStr() {
        return markStr;
    }

    public void setMarkStr(String markStr) {
        this.markStr = markStr;
    }

    @Override
    public String optionsValue() {
        String out = frontValue();

        out += ";testTermPath="+this.testTermPath+
               ";localOutputPath="+this.localOutputPath+
               ";markStr="+this.markStr;

        return out;
    }
}
