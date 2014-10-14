package main.core.ModelDumper;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Created by adam on 14-4-13.
 */
public class MapWritableDumper {

    public static void mapDumper(MapWritable map){
        Writable key,value;
        for(Map.Entry<Writable,Writable> entry : map.entrySet()){
            key = entry.getKey();
            value = entry.getValue();
            System.out.println("key="+key.toString()+";value="+value.toString());
        }
    }

    public static void mapDumper(MapWritable map,OutputStream out,boolean closeOut) throws IOException {
        Writable key,value;
//        int i=0;
        for(Map.Entry<Writable,Writable> entry : map.entrySet()){
            key = entry.getKey();
            value = entry.getValue();
            strToByte("key="+key.toString()+";value="+value.toString()+"\n",out);
//            i++;
//            System.out.println("key="+key.toString()+";value="+value.toString());
        }
//        System.out.println("size : "+i);

        if (closeOut)
            out.close();
    }

    private static void strToByte(String str,OutputStream out) throws IOException {
        out.write(str.getBytes());
    }
}
