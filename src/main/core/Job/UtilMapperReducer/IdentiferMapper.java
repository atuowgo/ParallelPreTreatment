package main.core.Job.UtilMapperReducer;


import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by adam on 14-3-25.
 */
public class IdentiferMapper<K,V> extends Mapper<K,V,K,V> {
    @Override
    protected void map(K key, V value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
