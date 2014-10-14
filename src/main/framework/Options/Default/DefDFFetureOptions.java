package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-3-16.
 */
public class DefDFFetureOptions extends Options {
    private String fetureNum;


    @Override
    public String optionsValue() {
        String out = frontValue();

        out += ";fetureNum="+this.fetureNum;

        return out;
    }

    public String getFetureNum() {
        return fetureNum;
    }

    public void setFetureNum(String fetureNum) {
        this.fetureNum = fetureNum;
    }
}
