package org.apache.ignite.sqlline;


public class CommandLineStartup {

    public static void main(String[] args) {

        //String example = "127.0.0.1 --schema --ttt --collocated --lazy --replicatedOnly --socketSendBuffer --lazy";

        //String[] arr = example.split(" ");

        new Runner().run(args);

        //new Runner().run(new String[]{});



    }
}
