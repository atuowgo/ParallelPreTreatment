package main.view.CLI;

import main.framework.Drive.JobXMlDrive;
import main.framework.JobRepo.JobRepo;
import main.framework.Resource.ResourceInfo;
import main.framework.Service.Imple.delServiceImple;
import main.framework.Service.Imple.listServiceImple;
import main.framework.Service.delService;
import main.framework.Service.listService;
import org.apache.commons.cli.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by adam on 14-4-29.
 */
public class defCLI implements CLIView {
    private delService delServicer = null;
    private listService listServicer = null;

    @Override
    public void view(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "显示帮助文本");
        options.addOption("l", "list", false, "显示中间结果路径");

        Option runOption = OptionBuilder.withArgName("confPath")
                .withDescription("以xml文件运行作业")
                .withLongOpt("run").hasArg().create('r');
        Option dumOption = OptionBuilder.withArgName("dumpPaths")
                .withDescription("输入路径文件的内容")
                .withLongOpt("dump").hasArgs().create('d');
        Option delOption = OptionBuilder.withArgName("deletePaths")
                .withDescription("删除路径")
                .withLongOpt("del").hasArgs().create();

        options.addOption(runOption)
                .addOption(dumOption)
                .addOption(delOption);

        Parser parser = new BasicParser();
        CommandLine cli = null;
        HelpFormatter formatter = new HelpFormatter();

        try {
            cli = parser.parse(options, args);
            Option[] ops = cli.getOptions();

            if (ops.length > 0) {
                if (cli.hasOption('h')) {
                    formatter.printHelp("选项", options);
                } else if (cli.hasOption('r') || cli.hasOption("run")) {
                    String jobPath = cli.getOptionValue("run");
                    runOp(jobPath);
                } else if (cli.hasOption('d')) {
                    String[] pathsToDump = cli.getOptionValues("dump");
                    dumpOp(pathsToDump);
                } else if (cli.hasOption("del")) {
                    String[] pathsToDel = cli.getOptionValues("del");
                    delOp(pathsToDel);
                } else if (cli.hasOption('l')) {
                    listOp();
                } else {
                    formatter.printHelp("选项", options);
                }
            } else {
                formatter.printHelp("选项", options);
            }
        } catch (ParseException e) {
            formatter.printHelp("选项", options);
        } catch (IOException e){
            System.err.println("执行错误");
        } catch (Exception e){

        }
    }

    private void runOp(String jobPath) throws IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
//        System.out.println("run : " + jobPath);

        String jobConfPath = null ;
        if ("def".equals(jobPath)){
            Properties pathPro = new Properties();
            pathPro.load(new FileInputStream(ResourceInfo.class.getResource("path.properties").getPath()));
            String defConfPath = JobRepo.class.getResource("job-conf.xml").getPath();
            jobConfPath = pathPro.getProperty("job-conf.path",defConfPath);
        }else {
            jobConfPath = jobPath;
        }

        JobXMlDrive drive = new JobXMlDrive(jobConfPath);
        drive.buildTaskTupe();
        drive.startJobTupe();
    }

    private void dumpOp(String[] dumpPaths) {
        System.out.println("dump : " + Arrays.toString(dumpPaths));
        System.err.println("未实现");
    }

    private void delOp(String[] delPaths) {
//        System.out.println("delete : " + Arrays.toString(delPaths));
        if (delServicer == null){
            delServicer = new delServiceImple();
        }

        try {
            delServicer.delPaths(delPaths);
        } catch (IOException e) {
            System.err.println("删除路径出错，请检查仔细检查");
        }

    }

    private void listOp(){
//        System.out.println("list paths");
        if (listServicer == null){
            listServicer = new listServiceImple();
        }

        try {
            System.out.println(listServicer.listPath());
        } catch (IOException e) {
            System.err.println("执行错误");
        }
    }
}
