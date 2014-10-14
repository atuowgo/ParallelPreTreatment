package main.framework.Service.Imple;

import main.framework.Service.ResourceUtil;
import main.framework.Service.listService;
import org.apache.hadoop.util.Shell;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by adam on 14-5-1.
 */
public class listServiceImple implements listService {

    @Override
    public String listPath() throws IOException {
        Properties packPro = ResourceUtil.getPackProInstance();
        String listCLI = packPro.getProperty("list.cli");
        String[] envs = listCLI.split(" ");
        Shell.ShellCommandExecutor executor = new Shell.ShellCommandExecutor(envs);

        executor.execute();

        return executor.getOutput();
    }
}
