package main.framework.Service;

import java.io.IOException;

/**
 * Created by adam on 14-5-1.
 */
public interface delService {

    public void delPath(String pathToDel) throws IOException;
    public void delPaths(String[] pathsToDel) throws IOException;
}
