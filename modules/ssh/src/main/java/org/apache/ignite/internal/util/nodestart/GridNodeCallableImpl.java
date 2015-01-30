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

package org.apache.ignite.internal.util.nodestart;

import com.jcraft.jsch.*;
import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.text.*;
import java.util.*;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 * SSH-based node starter.
 */
public class GridNodeCallableImpl implements GridNodeCallable {
    /** Default GridGain home path for Windows (taken from environment variable). */
    private static final String DFLT_IGNITE_HOME_WIN = "%IGNITE_HOME%";

    /** Default GridGain home path for Linux (taken from environment variable). */
    private static final String DFLT_IGNITE_HOME_LINUX = "$IGNITE_HOME";

    /** Default start script path for Windows. */
    private static final String DFLT_SCRIPT_WIN = "bin\\ignite.bat -v -np";

    /** Default start script path for Linux. */
    private static final String DFLT_SCRIPT_LINUX = "bin/ignite.sh -v";

    /**
     * Logs folder for Windows.
     * Folder for linux is configured in {@code ignite-log4j.xml}.
     */
    private static final String LOG_DIR_WIN = "work\\log";

    /** Windows service executable. */
    private static final String SVC_EXE = "bin\\include\\ggservice.exe";

    /** Date format for log file name. */
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("MM-dd-yyyy--HH-mm-ss");

    /** Specification. */
    private final GridRemoteStartSpecification spec;

    /** Connection timeout. */
    private final int timeout;

    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * Required by Externalizable.
     */
    public GridNodeCallableImpl() {
        spec = null;
        timeout = 0;

        assert false;
    }

    /**
     * Constructor.
     *
     * @param spec Specification.
     * @param timeout Connection timeout.
     */
    public GridNodeCallableImpl(GridRemoteStartSpecification spec, int timeout) {
        assert spec != null;

        this.spec = spec;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public GridTuple3<String, Boolean, String> call() {
        JSch ssh = new JSch();

        Session ses = null;

        try {
            if (spec.key() != null)
                ssh.addIdentity(spec.key().getAbsolutePath());

            ses = ssh.getSession(spec.username(), spec.host(), spec.port());

            if (spec.password() != null)
                ses.setPassword(spec.password());

            ses.setConfig("StrictHostKeyChecking", "no");

            ses.connect(timeout);

            boolean win = isWindows(ses);

            char separator = win ? '\\' : '/';

            spec.fixPaths(separator);

            String ggHome = spec.ggHome();

            if (ggHome == null)
                ggHome = win ? DFLT_IGNITE_HOME_WIN : DFLT_IGNITE_HOME_LINUX;

            String script = spec.script();

            if (script == null)
                script = win ? DFLT_SCRIPT_WIN : DFLT_SCRIPT_LINUX;

            String cfg = spec.configuration();

            if (cfg == null)
                cfg = "";

            String startNodeCmd;
            String scriptOutputFileName = FILE_NAME_DATE_FORMAT.format(new Date()) + '-'
                + UUID.randomUUID().toString().substring(0, 8) + ".log";

            if (win) {
                String logDir = ggHome + '\\' + LOG_DIR_WIN;
                String tmpDir = env(ses, "%TMP%", logDir);
                String scriptOutputDir = tmpDir + "\\gridgain-startNodes";

                shell(ses, "mkdir " + logDir);
                shell(ses, "mkdir " + scriptOutputDir);

                UUID id = UUID.randomUUID();

                String svcName = "GridGain-" + id;
                String svcPath = ggHome + '\\' + SVC_EXE;

                startNodeCmd = new SB().
                    a("cmd /c if exist \"").a(svcPath).a("\"").
                    a(" sc create ").a(svcName).
                    a(" binPath= \"").a(svcPath).a("\"").
                    a(" && ").
                    a("sc start ").a(svcName).
                    a(" ").a(svcName).
                    a(" \"").a(ggHome).a('\\').a(script).
                    a(" ").a(cfg).a("\"").
                    a(" \"").a(logDir).a("\\gridgain.").a(id).
                    a(".log\" > ").a(scriptOutputDir).a("\\").a(scriptOutputFileName).
                    toString();
            }
            else { // Assume Unix.
                int spaceIdx = script.indexOf(' ');

                String scriptPath = spaceIdx > -1 ? script.substring(0, spaceIdx) : script;
                String scriptArgs = spaceIdx > -1 ? script.substring(spaceIdx + 1) : "";
                String rmtLogArgs = buildRemoteLogArguments(spec.username(), spec.host());
                String tmpDir = env(ses, "$TMPDIR", "/tmp/");
                String scriptOutputDir = tmpDir + "gridgain-startNodes";

                shell(ses, "mkdir " + scriptOutputDir);

                // Mac os don't support ~ in double quotes. Trying get home path from remote system.
                if (ggHome.startsWith("~")) {
                    String homeDir = env(ses, "$HOME", "~");

                    ggHome = ggHome.replaceFirst("~", homeDir);
                }

                startNodeCmd = new SB().
                    // Console output is consumed, started nodes must use Grid file appenders for log.
                        a("nohup ").
                    a("\"").a(ggHome).a('/').a(scriptPath).a("\"").
                    a(" ").a(scriptArgs).
                    a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "").
                    a(rmtLogArgs).
                    a(" > ").a(scriptOutputDir).a("/").a(scriptOutputFileName).a(" 2>& 1 &").
                    toString();
            }

            info("Starting remote node with SSH command: " + startNodeCmd, spec.logger(), log);

            shell(ses, startNodeCmd);

            return new GridTuple3<>(spec.host(), true, null);
        }
        catch (IgniteInterruptedException e) {
            return new GridTuple3<>(spec.host(), false, e.getMessage());
        }
        catch (Exception e) {
            return new GridTuple3<>(spec.host(), false, X.getFullStackTrace(e));
        }
        finally {
            if (ses != null && ses.isConnected())
                ses.disconnect();
        }
    }

    /**
     * Executes command using {@code shell} channel.
     *
     * @param ses SSH session.
     * @param cmd Command.
     * @throws JSchException In case of SSH error.
     * @throws IOException If IO error occurs.
     * @throws org.apache.ignite.IgniteInterruptedException If thread was interrupted while waiting.
     */
    private void shell(Session ses, String cmd) throws JSchException, IOException, IgniteInterruptedException {
        ChannelShell ch = null;

        try {
            ch = (ChannelShell)ses.openChannel("shell");

            ch.connect();

            try (PrintStream out = new PrintStream(ch.getOutputStream(), true)) {
                out.println(cmd);

                U.sleep(1000);
            }
        }
        finally {
            if (ch != null && ch.isConnected())
                ch.disconnect();
        }
    }

    /**
     * Checks whether host is running Windows OS.
     *
     * @param ses SSH session.
     * @return Whether host is running Windows OS.
     * @throws JSchException In case of SSH error.
     */
    private boolean isWindows(Session ses) throws JSchException {
        try {
            return exec(ses, "cmd.exe") != null;
        }
        catch (IOException ignored) {
            return false;
        }
    }

    /**
     * Gets the value of the specified environment variable.
     *
     * @param ses SSH session.
     * @param name environment variable name.
     * @param dflt default value.
     * @return environment variable value.
     * @throws JSchException In case of SSH error.
     */
    private String env(Session ses, String name, String dflt) throws JSchException {
        try {
            return exec(ses, "echo " + name);
        }
        catch (IOException ignored) {
            return dflt;
        }
    }

    /**
     * Gets the value of the specified environment variable.
     *
     * @param ses SSH session.
     * @param cmd environment variable name.
     * @return environment variable value.
     * @throws JSchException In case of SSH error.
     * @throws IOException If failed.
     */
    private String exec(Session ses, String cmd) throws JSchException, IOException {
        ChannelExec ch = null;

        try {
            ch = (ChannelExec)ses.openChannel("exec");

            ch.setCommand(cmd);

            ch.connect();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(ch.getInputStream()))) {
                return reader.readLine();
            }
        }
        finally {
            if (ch != null && ch.isConnected())
                ch.disconnect();
        }
    }

    /**
     * Builds ignite.sh attributes to set up SSH username and password and log directory for started node.
     *
     * @param username SSH user name.
     * @param host Host.
     * @return {@code ignite.sh} script arguments.
     */
    private String buildRemoteLogArguments(String username, String host) {
        assert username != null;
        assert host != null;

        SB sb = new SB();

        sb.a(" -J-D").a(IGNITE_SSH_HOST).a("=\"").a(host).a("\"").
            a(" -J-D").a(IGNITE_SSH_USER_NAME).a("=\"").a(username).a("\"");

        return sb.toString();
    }

    /**
     * @param log Logger.
     * @return This callable for chaining method calls.
     */
    public GridNodeCallable setLogger(IgniteLogger log) {
        this.log = log;

        return this;
    }

    /**
     * Log info message to loggers.
     *
     * @param msg Message text.
     * @param loggers Loggers.
     */
    private void info(String msg, IgniteLogger... loggers) {
        for (IgniteLogger logger : loggers)
            if (logger != null && logger.isInfoEnabled())
                logger.info(msg);
    }
}
