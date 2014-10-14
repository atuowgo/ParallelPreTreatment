package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-3-19.
 */
public class DefMatrixOptions extends Options {
    private String featurePath;

    @Override
    public String optionsValue() {
        String out = frontValue();

        out += ";featurePath="+this.featurePath;

        return out;
    }

    public String getFeaturePath() {
        return featurePath;
    }

    public void setFeaturePath(String featurePath) {
        this.featurePath = featurePath;
    }
}
