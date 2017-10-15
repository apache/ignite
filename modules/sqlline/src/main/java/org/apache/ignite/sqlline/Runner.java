package org.apache.ignite.sqlline;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by oostanin on 15.10.17.
 */
class Runner {

    private final String defaultDriver = "org.apache.ignite.IgniteJdbcThinDriver";
    private String schema = "PUBLIC";
    private String color="true";
    private String scriptName = "ignitedb.sh";

    public int run(String[] args) {
        if (System.getProperty("os.name").toLowerCase().startsWith("windows"))
            scriptName = "ignitedb.bat";

        if (args.length == 0) {
            System.out.println("Error. You need to define host.");
            printHelp();
            System.exit(1);
        }

        String host = args[0];

        List<String> connStrParams = new ArrayList<>();

        for (int i = 1; i < args.length; i++) {
            String valOrig = args[i];
            String val = args[i].toLowerCase();

            switch (val) {
                case "--schema":
                    check(args, i, val);
                    schema = args[++i];
                    break;
                case "--nocolor":
                    color="false";
                    break;
                case "--distributedjoins":
                    connStrParams.add("distributedJoins=true");
                    break;
                case "--lazy":
                    connStrParams.add("lazy=true");
                    break;
                case "--collocated":
                    connStrParams.add("collocated=true");
                    break;
                case "--replicatedonly":
                    connStrParams.add("replicatedOnly=true");
                    break;
                case "--enforcejoinorder":
                    connStrParams.add("enforceJoinOrder=true");
                    break;
                case "--socketsendbuffer":
                    check(args, i, val);
                    checkNum(valOrig, args[++i]);
                    connStrParams.add("socketSendBuffer=" + args[i]);
                    break;
                case "--socketreceivebuffer":
                    check(args, i, val);
                    checkNum(valOrig, args[++i]);
                    connStrParams.add("socketReceiveBuffer=" + args[i]);
                    break;
                default:
                    System.out.println("Error: Invalid argument " + val);
                    printHelp();
                    System.exit(1);
            }
        }

        String[] args0 = new String[]{
            "-d", defaultDriver,
            "--color=" + color,
            "--verbose=true",
            "--showWarnings=true",
            "--showNestedErrs=true",
            "-u=" + getConnStr(connStrParams, host)
        };

        for (String arg : args0)
            System.out.print(arg + " ");

        System.out.println();

        try {
            sqlline.SqlLine.main(args0);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return 0;
    }

    private void printHelp() {

        System.out.println();
        System.out.println("Usage: " + scriptName + " host[:port] [options]");
        System.out.println();
        System.out.println("If port is omitted default port 10800 will be used.");
        System.out.println();
        System.out.println("Options:");
        System.out.println("    -h  |  --help                       Help.");
        System.out.println("    --schema <schema>                   Schema name; defaults to PUBLIC.");
        System.out.println("    --distributedJoins                  Enable distributed joins.");
        System.out.println("    --lazy                              Execute queries in lazy mode.");
        System.out.println("    --collocated                        Collocated flag.");
        System.out.println("    --replicatedOnly                    Replicated only flag");
        System.out.println("    --enforceJoinOrder                  Enforce join order.");
        System.out.println("    --socketSendBuffer <buf_size>       Socket send buffer size in bytes.");
        System.out.println("    --socketReceiveBuffer <buf_size>    Socket receive buffer size in bytes.");
        System.out.println();
        System.out.println("Examples: " + scriptName + " myHost --schema mySchema --distributedJoins");
        System.out.println("          " + scriptName + " localhost --schema mySchema --collocated");
        System.out.println("          " + scriptName + " 127.0.0.1:10800 --schema mySchema --replicatedOnly");
        System.out.println();
        System.out.println("For more information see https://apacheignite-sql.readme.io/docs/jdbc-driver");
    }

    private void check(String[] args, int i, String val) {
        if (args.length < i && args[i+1].startsWith("-")){
            System.out.println("Error. You did not define value for " + val);
            printHelp();
            System.exit(1);
        }
    }

    private void checkNum(String param, String val){
        try {
            Integer.parseInt(val);
        }
        catch (NumberFormatException ignored) {
            System.out.println("Error: Invalid value " + val + " for " + param + ". Should be an integer.");
            printHelp();
            System.exit(1);
        }
    }

    private String getConnStr(List<String> params, String host){
        StringBuilder connStr = new StringBuilder("jdbc:ignite:thin://" + host + "/" + schema);

        String dlmtr = "?";

        for (String param : params){
            connStr.append(dlmtr);
            connStr.append(param);

            dlmtr = "&";
        }
        return connStr.toString();
    }
}
