package view;

import org.apache.commons.cli.*;

import java.util.Arrays;

/**
 * Created by adam on 14-4-29.
 */
public class CLI {

    public static void test01(String[] args){
        Options options = new Options();
        options.addOption("h","help",false,"显示帮助文本");

        Option runOption = OptionBuilder.withArgName("path")
                .withDescription("运行作业")
                .withLongOpt("run").hasArg().create('r');
        Option dumOption = OptionBuilder.withArgName("paths")
                .withDescription("显示中间结果")
                .withLongOpt("dump").hasArgs().create('d');
        Option delOption = OptionBuilder.withArgName("paths")
                .withDescription("删除路径")
                .withLongOpt("del").hasArgs().create();

        options.addOption(runOption)
                .addOption(dumOption)
                .addOption(delOption);

        Parser parser = new BasicParser();
        CommandLine cli = null;
        try {
            cli = parser.parse(options,args);
            Option[] ops = cli.getOptions();

            if (ops.length>0){
                if (cli.hasOption('h')||cli.hasOption("help")){
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("选项",options);
                }else if (cli.hasOption('r') || cli.hasOption("run")){
                    String jobPath = cli.getOptionValue('r');
                    System.out.println("输入路径为："+jobPath);
                }else if (cli.hasOption('d') || cli.hasOption("dump")){
                    String[] paths = cli.getOptionValues('d');
                    System.out.println(Arrays.toString(paths));
                }else {
                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp("选项",options);
                }
            }else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("选项", options);
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("选项", options);
        }


    }

    public static void main(String[] args) throws ParseException {
        test01(args);
    }
}
