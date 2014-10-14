package main.core.Treatment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by adam on 14-3-19.
 */
public class CleanPathFilter implements PathFilter{

    @Override
    public boolean accept(Path path) {
        String fileName = path.getName();

        if (fileName.startsWith("_"))
            return true;
        else
            return false;
    }
}
