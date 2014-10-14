package main.core.Util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by adam on 14-3-11.
 */
public class OptionsRepo {
    public static Map<String,String> parseOptionsValue(String optionsValue){
        Map<String,String> optionsMap = new HashMap<String, String>();
        String[] values = optionsValue.split(";");
        int ch;
        for(int i=0;i<values.length;i++){
            ch = values[i].indexOf("=");
            optionsMap.put(values[i].substring(0,ch),values[i].substring(ch+1));
        }

        return optionsMap;
    }

    public static String createOptionsFromPro(Properties pro){
        String out = "";
        for(String keyStr : pro.stringPropertyNames()){
            if (!"".equals(out))
                out += ";";
            out += keyStr+"="+pro.getProperty(keyStr);
        }

        return out;
    }
}
