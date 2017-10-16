/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sqlline;

import sqlline.SqlLine;

import java.util.ArrayList;
import java.util.List;

/**
 * Executable class for sqlline wrapper.
 */
public class CommandLineStartup {
    /** Schema. */
    private String schema;

    /** Whether color is enabled. */
    private boolean color = true;

    /** Script name. */
    private final String scriptName;

    /**
     * Entry point.
     * @param args Arguments.
     */
    public static void main(String[] args) {
        new CommandLineStartup().run(args);
    }

    /**
     * Constructor.
     */
    private CommandLineStartup() {
        if (System.getProperty("os.name").toLowerCase().startsWith("win"))
            scriptName = "ignitesql.bat";
        else
            scriptName = "ignitesql.sh";
    }

    /**
     * Execute.
     *
     * @param args Arguments.
     */
    public void run(String[] args) {
        if (args.length == 0) {
            System.out.println("Host is not defined.");

            printHelp();

            System.exit(1);
        }

        String hostPort = args[0];

        List<String> connStrParams = new ArrayList<>();

        for (int i = 1; i < args.length; i++) {
            String valOriginal = args[i];
            String val = args[i].toLowerCase();

            switch (val) {
                case "--schema":
                    check(args, i, valOriginal);

                    schema = args[i+1];

                    i++;

                    break;

                case "--nocolor":
                    color = false;

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
                    check(args, i, valOriginal, true);

                    connStrParams.add("socketSendBuffer=" + args[i+1]);

                    i++;
                    break;

                case "--socketreceivebuffer":
                    check(args, i, valOriginal, true);

                    connStrParams.add("socketReceiveBuffer=" + args[i+1]);

                    i++;

                    break;

                default:
                    System.out.println("Unrecognized parameter: " + val);

                    printHelp();

                    System.exit(1);
            }
        }

        String[] args0 = new String[] {
            "-d", "org.apache.ignite.IgniteJdbcThinDriver",
            "--color=" + color,
            "--verbose=true",
            "--showWarnings=true",
            "--showNestedErrs=true",
            "-u", createConnectionString(connStrParams, hostPort)
        };

        try {
            SqlLine.main(args0);
        }
        catch (Exception e) {
            e.printStackTrace();

            System.exit(1);
        }
    }

    /**
     * Print help message.
     */
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
        System.out.println("    --nocolor                           Use it in Windows cmd mode to avoid charset issues.");
        System.out.println();
        System.out.println("Examples: " + scriptName + " myHost --schema mySchema --distributedJoins");
        System.out.println("          " + scriptName + " localhost --schema mySchema --collocated");
        System.out.println("          " + scriptName + " 127.0.0.1:10800 --schema mySchema --replicatedOnly");
        System.out.println();
        System.out.println("For more information see https://apacheignite-sql.readme.io/docs/jdbc-driver");
    }

    /**
     * Check argument.
     *
     * @param args Arguments.
     * @param i Index.
     * @param name Parameter name.
     */
    private void check(String[] args, int i, String name) {
        check(args, i, name, false);
    }

    /**
     * Check argument.
     *
     * @param args All arguments.
     * @param i Index.
     * @param name Parameter name.
     * @param integer Whether it should be valid integer.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void check(String[] args, int i, String name, boolean integer) {
        boolean res = true;

        if (args.length <= i + 1) {
            System.out.println("Value is not provided for parameter " + name + ".");

            res = false;
        }

        if (integer) {
            String val = args[i];

            try {
                Integer.parseInt(val);
            }
            catch (NumberFormatException ignored) {
                System.out.println("Invalid value for parameter " + name + " (should be an integer): " + val);

                res = false;
            }
        }

        if (!res) {
            printHelp();

            System.exit(1);
        }
    }

    /**
     * Create connection string.
     *
     * @param params Parameters.
     * @param hostPort Host and port.
     * @return Connection string.
     */
    private String createConnectionString(List<String> params, String hostPort) {
        StringBuilder connStr = new StringBuilder("jdbc:ignite:thin://" + hostPort);

        if (schema != null)
            connStr.append("/").append(schema);

        String delim = "?";

        for (String param : params) {
            connStr.append(delim);
            connStr.append(param);

            delim = "&";
        }

        return connStr.toString();
    }
}
