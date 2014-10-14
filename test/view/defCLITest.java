package view;

import main.view.CLI.CLIView;
import main.view.CLI.defCLI;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

/**
 * Created by adam on 14-4-29.
 */
public class defCLITest {

    @Test
    public void test01(){
        String[] args = {"-l"};

        CLIView view = new defCLI();
        view.view(args);
    }

    @Test
    public void test02() throws IOException, InterruptedException {
        Process process = new ProcessBuilder("hadoop","fs","-lsr /").start();

    }

    @Test
    public void test03(){
        Runtime runtime = Runtime.getRuntime();
        System.out.println(runtime.maxMemory());
        System.out.println(runtime.availableProcessors());
        System.out.println(runtime.freeMemory());
        System.out.println(runtime.totalMemory());
    }

    @Test
    public void test04(){
        ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        System.out.println(classLoadingMXBean.getLoadedClassCount());
        System.out.println(classLoadingMXBean.getTotalLoadedClassCount());
        System.out.println(classLoadingMXBean.getUnloadedClassCount());

//        Arrays.toString(ManagementFactory.getGarbageCollectorMXBeans());
    }
}
