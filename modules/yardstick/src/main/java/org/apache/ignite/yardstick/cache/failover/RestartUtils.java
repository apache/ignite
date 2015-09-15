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

package org.apache.ignite.yardstick.cache.failover;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Restart Utils.
 */
public final class RestartUtils {
    /**
     * Private default constructor.
     */
    private RestartUtils() {
        // No-op.
    }

    /**
     * Kills 'Dyardstick.server${ID}' process with -9 option on remote host.
     *
     * @param remoteUser Remote user.
     * @param hostName Host name.
     * @param id Server id.
     * @param isDebug Is debug enabled flag.
     * @return Result of process execution.
     */
    public static Result kill9(final String remoteUser, String hostName, final int id, boolean isDebug) {
        return executeUnderSshConnect(remoteUser, hostName, isDebug, "pkill -9 -f 'Dyardstick.server" + id + "'");
    }

    private static Result executeUnderSshConnect(String remoteUser, String hostName,
        boolean isDebug, String cmd) {
        if (U.isWindows())
            throw new UnsupportedOperationException("Unsupported operation for windows.");

        Tuple<Thread, StringBuffer> t1 = null;
        Tuple<Thread, StringBuffer> t2 = null;

        try {
            Process p = Runtime.getRuntime().exec("ssh -o PasswordAuthentication=no " + remoteUser + "@" + hostName);

            try(PrintStream out = new PrintStream(p.getOutputStream(), true)) {
                out.println(cmd);

                if (isDebug) {
                    t1 = monitorInputeStream(p.getInputStream(), "OUT");
                    t2 = monitorInputeStream(p.getErrorStream(), "ERR");
                }

                out.println("exit");

                p.waitFor();
            }

            return new Result(p.exitValue(), t1 == null ? "" : t1.val2.toString(), t2 == null ? "" : t2.val2.toString());
        }
        catch (Exception err) {
            return new Result(err);
        }
        finally {
            if (isDebug) {
                if (t1 != null && t1.val1 != null)
                    t1.val1.interrupt();

                if (t2 != null && t2.val1 != null)
                    t2.val1.interrupt();
            }
        }
    }

    /**
     * Monitors input stream at separate thread.
     * @param in Input stream.
     * @param name Name.
     * @return Tuple of started thread and output string.
     */
    private static Tuple<Thread, StringBuffer> monitorInputeStream(final InputStream in, final String name) {
        final StringBuffer sb = new StringBuffer();

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try (BufferedReader input = new BufferedReader(new InputStreamReader(in))) {
                    String l;

                    while (!Thread.currentThread().isInterrupted() && ((l = input.readLine()) != null))
                        sb.append(l).append('\n');
                }
                catch (IOException e) {
                    sb.append(e).append('\n');
                }
            }
        }, name);

        thread.setDaemon(true);

        thread.start();

        return new Tuple<>(thread, sb);
    }


    public static Result start(BenchmarkConfiguration cfg, final String remoteUser, String hostName, final int id, boolean isDebug) {
//        ssh -o PasswordAuthentication=no ${REMOTE_USER}"@"${HOST_NAME} \
//        "MAIN_CLASS='org.yardstickframework.BenchmarkServerStartUp'" \
//        "JVM_OPTS='${JVM_OPTS} -Dyardstick.server${ID}-${cntr}'" "CP='${CP}'" "CUR_DIR='${CUR_DIR}'" "PROPS_ENV0='${PROPS_ENV}'" \
//        "nohup ${SCRIPT_DIR}/benchmark-bootstrap.sh ${CONFIG} "--config" ${CONFIG_INCLUDE} > ${server_file_log} 2>& 1 &"

        String jvmOpts = "JVM_OPTS";
        String cp = "${CP}";
        String propsEnv = "${PROPS_ENV}";

        String curDir = "${CUR_DIR}";
        String scriptDir = "${SCRIPT_DIR}";
        String serversLogsDir = "SERVERS_LOGS_DIR";

        String descriptrion = ""; // TODO extract description from cmdArgs
        String now = "1111111"; // TODO extract
        String logFile = serversLogsDir +"/"+ now + "_id" + cfg.memberId() + "_" + hostName + descriptrion + ".log";

        String cmdArgs = Arrays.toString(cfg.commandLineArguments());

        System.out.println("cmdArgs=" + cmdArgs);

        String cmd = "MAIN_CLASS='org.yardstickframework.BenchmarkServerStartUp' " +
            "JVM_OPTS='" + jvmOpts + "' " +
            "CP='" + cp + "' " +
            "CUR_DIR='" + curDir + "' " +
            "PROPS_ENV0='" + propsEnv + "' " +
            "nohup " + scriptDir + "/benchmark-bootstrap.sh " + cmdArgs + " > " + logFile + " 2>& 1 &";

        return executeUnderSshConnect(remoteUser, hostName, isDebug, cmd);
    }

    /**
     * Result of executed command.
     */
    public static class Result {
        private final int exitCode;
        private final String out;
        private final String err;
        private final Exception e;

        /**
         * @param exitCode Exit code.
         * @param out Out.
         * @param err Err.
         */
        public Result(int exitCode, String out, String err) {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;

            e = null;
        }

        /**
         * @param e Exception.
         */
        public Result(Exception e) {
            exitCode = -1;
            out = null;
            err = null;

            this.e = e;
        }

        /**
         * @return Exit code.
         */
        public int getExitCode() {
            return exitCode;
        }

        /**
         * @return Output.
         */
        public String getOutput() {
            return out;
        }

        /**
         * @return Error output.
         */
        public String getErrorOutput() {
            return err;
        }

        /**
         * @return Exception.
         */
        public Exception getException() {
            return e;
        }
    }

    /**
     * Tuple.
     *
     * @param <T> Type of first element.
     * @param <K> Type of second element.
     */
    private static class Tuple<T, K> {
        /** */
        T val1;

        /** */
        K val2;

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        Tuple(T val1, K val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }
}
