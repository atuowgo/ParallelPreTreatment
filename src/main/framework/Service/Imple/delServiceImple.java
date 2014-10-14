package main.framework.Service.Imple;

import main.core.Treatment.DeletePathFilter;
import main.core.Util.HadoopConfUtil;
import main.core.Util.JobUtil;
import main.framework.Service.ResourceUtil;
import main.framework.Service.delService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * Created by adam on 14-5-1.
 */
public class delServiceImple implements delService {

    @Override
    public void delPath(String pathToDel) throws IOException {
        Path hdfsPathToDel = new Path(HadoopConfUtil.createHdfsUrl(pathToDel));
        PathFilter pathFilter = new DeletePathFilter();
        FileSystem fs = hdfsPathToDel.getFileSystem(ResourceUtil.getConfInstance());
        JobUtil.cleanPath(hdfsPathToDel, fs,pathFilter);
    }

    @Override
    public void delPaths(String[] pathsToDel) throws IOException {
        for(String pathToDel : pathsToDel)
            delPath(pathToDel);
    }


}
