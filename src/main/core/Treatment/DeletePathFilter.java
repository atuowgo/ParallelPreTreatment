package main.core.Treatment;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by adam on 14-3-25.
 */
public class DeletePathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        return true;
    }
}
