package main.core.Util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;

import java.io.IOException;

/**
 * Created by adam on 14-3-10.
 */
public class LogUtil {
    private static Log consoleLog = LogFactory.getLog("consoleLog");
    private static Log errorLog = LogFactory.getLog("errorLog");

    private LogUtil(){
        Logger logger = Logger.getLogger("");
        PatternLayout layout = new PatternLayout();
        try {
            Appender appender = new FileAppender(layout,"aa")
                    ;
            logger.addAppender(appender);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Log getConsoleLog(){
        return consoleLog;
    }

    public static Log getErrorLog(){
        return errorLog;
    }

}
