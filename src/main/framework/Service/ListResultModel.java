package main.framework.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-5-1.
 */
public class ListResultModel {

    public static String getListRootPath() throws IOException {
        Properties packProperties = new Properties();
        packProperties.load(new FileInputStream(listService.class.getResource("pack.properties").getPath()));

        return packProperties.getProperty("list.root.path");
    }


}
