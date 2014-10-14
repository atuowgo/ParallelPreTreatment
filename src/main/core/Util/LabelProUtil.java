package main.core.Util;

/**
 * Created by adam on 14-3-13.
 */
public class LabelProUtil {

    public static String parseLabelStr(int num){
        String out = String.valueOf(num);

        int step = 5 - out.length();

        for(int i=0;i<step;i++)
            out = "0"+out;

        return out;
    }

    public static int reParseLableInt(String labelStr){
        int index = 0;
        char ch = 'A';
        int clazzNum = 0;

        while(ch=='0' && index<5){
            ch = labelStr.charAt(index);
            index++;
        }

        if (index == 5)
            return 0;
        else{
            labelStr = labelStr.substring(index--);
            return Integer.parseInt(labelStr);
        }

    }

    public static String parseDFLable(String fileName,String tag){
        int index = fileName.indexOf(tag);
        fileName = fileName.substring(++index);

        return fileName;
    }

    public static int parseDFPartitioner(String fileName,String tag){
        int index = fileName.indexOf(tag);
        fileName = fileName.substring(0,index);

        return reParseLableInt(fileName);
    }

    public static int parseMatrixPartitioner(String fileName,String tag){
        int index = fileName.indexOf(tag);
        fileName = fileName.substring(0,index);

        return Integer.parseInt(fileName);
    }

    public static String parseMatrixFileName(String fileName,String tag){
        int index = fileName.indexOf(tag);
        fileName = fileName.substring(++index);

        return fileName;
    }

    public static String parseStrR(String fileName){
        int index = fileName.lastIndexOf("-");
        fileName = fileName.substring(++index);

        return  fileName;
    }
}
