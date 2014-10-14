package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-4-16.
 */
public class DefECEFeatureOptions extends Options {
    private String featureNum;

    @Override
    public String optionsValue() {
        String out = frontValue();

        out += ";featureNum="+this.featureNum;

        return out;
    }

    public String getFeatureNum() {
        return featureNum;
    }

    public void setFeatureNum(String featureNum) {
        this.featureNum = featureNum;
    }
}
