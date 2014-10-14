package main.framework.Options.Default;

import main.framework.Options.Options;

/**
 * Created by adam on 14-2-19.
 */
public class TestDefaultOp extends Options {
    private String age;

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String optionsValue() {
        return null;
    }
}
