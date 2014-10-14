package main.core.Treatment.Default;

import main.core.Model.Clean.DefHtmlModel;
import main.core.Treatment.HtmlTreater;
import main.core.Util.LogUtil;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;

/**
 * Created by adam on 14-3-9.
 */
public class DefHtmlTreater implements HtmlTreater {
    private static DefHtmlTreater treater = new DefHtmlTreater();
    private DefHtmlTreater(){}
    private static Log errorLog = LogUtil.getErrorLog();

    @Override
    public Document canParse(File file) {
        Document doc = null;
        try {
            doc = Jsoup.parse(file,"UTF-8");
        } catch (IOException e) {
            errorLog.error(file.getName()+" 出错",e);
            e.printStackTrace();
        }

        return doc;
    }

    @Override
    public boolean parseHtml(String path, String clazz, String fileName, DefHtmlModel out) throws IOException {
        File file = new File(path);
        Document doc = canParse(file);
        if (doc == null) {
            return false;
        }

        if (doc.body()==null){
            errorLog.error("文件 "+path+" 错误");
            return false;
        }
        String modelContent = doc.body().text();
        out.setClazz(new Text(clazz));
        out.setFileName(new Text(fileName));
        out.setContent(new Text(modelContent));

        return true;
    }

    public static HtmlTreater getInstance() {
        return treater;
    }


}
