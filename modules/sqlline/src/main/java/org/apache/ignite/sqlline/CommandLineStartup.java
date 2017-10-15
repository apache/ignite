package org.apache.ignite.sqlline;


public class CommandLineStartup {

    public static void main(String[] args) {

        //String example = "127.0.0.1 --schema pppp --enforceJoinOrder --collocated --socketrECEivebuffer 10 19";

        //String[] arr = example.split(" ");

        new Runner().run(args);

        //new Runner().run(new String[]{});



    }
}
