package main.core.Treatment;

import main.core.Model.Clean.DefHtmlModel;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;

/**
 * Created by adam on 14-3-9.
 */
public interface HtmlTreater {
    public Document canParse(File file);
    public boolean parseHtml(String path,String clazz,String fileName,DefHtmlModel out) throws IOException;

}
