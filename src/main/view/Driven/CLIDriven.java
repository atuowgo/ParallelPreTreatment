package main.view.Driven;

import main.view.CLI.CLIView;
import main.view.CLI.defCLI;

/**
 * Created by adam on 14-5-1.
 */
public class CLIDriven {

    public static void main(String[] args){
        CLIView view = new defCLI();
//        System.out.println("args : "+Arrays.toString(args));
        view.view(args);
    }
}
