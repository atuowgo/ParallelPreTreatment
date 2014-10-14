package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-3-9.
 */
public class DefHtmlCleanOptions extends Options {
    private String htmlTreater;
    private String trainPecentage;

    public String getHtmlTreater() {
        return htmlTreater;
    }

    public void setHtmlTreater(String htmlTreater) {
        this.htmlTreater = htmlTreater;
    }

    public String getTrainPecentage() {
        return trainPecentage;
    }

    public void setTrainPecentage(String trainPecentage) {
        this.trainPecentage = trainPecentage;
    }

    @Override
    public String optionsValue() {
        String out = frontValue();
        out += ";htmlTreater="+this.htmlTreater+
               ";trainPecentage="+this.trainPecentage;

        return out;
    }
}
