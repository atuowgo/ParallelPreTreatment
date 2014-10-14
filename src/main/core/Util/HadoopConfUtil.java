package main.core.Util;

import main.core.Config.ConfInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import java.io.IOException;

/**
 * Created by adam on 14-2-10.
 */
public final class HadoopConfUtil {
    private static final String HDFSPREX = "hdfs://";
    private static Document document;
    static {
        readDoc();
    }

    private static void readDoc(){
        SAXReader reader = new SAXReader();
        try {
            document = reader.read(ConfInfo.class.getResource("hadoop-conf.xml").getPath());
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    public static String getHadoopHomePath(){
        return document.selectSingleNode("//hadoop-home").getText().trim();
    }

    public static String getHadoopHdfsUrl(){
        String url = document.selectSingleNode("//hadoop-hdfs").getText().trim();

        return HDFSPREX+url;
    }

    public static Configuration getNewConfiguration(){
        Configuration conf = new Configuration();
        conf.addResource(new Path(getHadoopHomePath()+"/Config/core-site.xml"));
        return conf;
    }

    public static String createHdfsUrl(String url){
        return getHadoopHdfsUrl()+"/"+url;
    }

    public static FileSystem getNewFileSystem(){
        try {
            return FileSystem.get(getNewConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void moveDir(String oldHdfsPath,String newHdfsPath) throws IOException {
        Path oldPath = new Path(createHdfsUrl(oldHdfsPath));
        Path newPath = new Path(createHdfsUrl(newHdfsPath));

        FileSystem fileSystem = getNewFileSystem();


        FileStatus[] statuses = fileSystem.listStatus(oldPath,new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if (path.getName().startsWith("part-"))
                    return true;
                else
                    return false;
            }
        });

        FSDataInputStream inputStream;
        FSDataOutputStream outputStream = fileSystem.create(newPath);
        byte[] bytes = new byte[1024];
        int ch = 0;

        for(FileStatus file : statuses){
            inputStream = fileSystem.open(file.getPath());

            while( (ch = inputStream.read(bytes))!= -1 ){
                outputStream.write(bytes,0,ch);
            }

            inputStream.close();
        }

        outputStream.close();

        fileSystem.deleteOnExit(oldPath);
    }
}
