package main.core.Util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * Created by adam on 14-3-18.
 */
public class JobUtil {

    //    pathToClean为输出目录
    public static void cleanPath(Path pathToClean, FileSystem fileSystem, PathFilter filter) throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(pathToClean);

        for (FileStatus f : fileStatus) {
            if (filter.accept(f.getPath())) {
                if (f.isDir()) {
                    cleanPath(f.getPath(), fileSystem, filter);
                }

                fileSystem.delete(f.getPath(), true);
            }
        }
    }

    public static int getClazzNum(Path path,FileSystem fileSystem,PathFilter filter) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(path);

        int num = 0;

        for(FileStatus f : fileStatuses){
            if (!filter.accept(f.getPath()))
                ++num;
        }

        return num;
    }



}
