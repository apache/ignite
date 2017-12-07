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

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterStartNodeResultImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_HOME;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_SSH_USER_NAME;

/**
 * SSH-based node starter.
 */
public class StartNodeCallableImpl implements StartNodeCallable {
    /** Default Ignite home path for Windows (taken from environment variable). */
    private static final String DFLT_IGNITE_HOME_WIN = "%IGNITE_HOME%";

    /** Default Ignite home path for Linux (taken from environment variable). */
    private static final String DFLT_IGNITE_HOME_LINUX = "$IGNITE_HOME";

    /** Windows console encoding */
    private static final String WINDOWS_ENCODING = "IBM866";

    /** Default start script path for Windows. */
    private static final String DFLT_SCRIPT_WIN = "bin\\ignite.bat -v";

    /** Default start script path for Linux. */
    private static final String DFLT_SCRIPT_LINUX = "bin/ignite.sh -v";

    /** Date format for log file name. */
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("MM-dd-yyyy--HH-mm-ss");

    /** Specification. */
    private final IgniteRemoteStartSpecification spec;

    /** Connection timeout. */
    private final int timeout;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /**
     * Required by Externalizable.
     */
    public StartNodeCallableImpl() {
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
    public StartNodeCallableImpl(IgniteRemoteStartSpecification spec, int timeout) {
        assert spec != null;

        this.spec = spec;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override public ClusterStartNodeResult call() {
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

            String igniteHome = spec.igniteHome();

            if (igniteHome == null)
                igniteHome = win ? DFLT_IGNITE_HOME_WIN : DFLT_IGNITE_HOME_LINUX;

            String script = spec.script();

            if (script == null)
                script = win ? DFLT_SCRIPT_WIN : DFLT_SCRIPT_LINUX;

            String cfg = spec.configuration();

            if (cfg == null)
                cfg = "";

            String id = UUID.randomUUID().toString();

            String scriptOutputFileName = FILE_NAME_DATE_FORMAT.format(new Date()) + '-' + id.substring(0, 8) + ".log";

            if (win) {
                int spaceIdx = script.indexOf(' ');

                String scriptPath = spaceIdx > -1 ? script.substring(0, spaceIdx) : script;
                String scriptArgs = spaceIdx > -1 ? script.substring(spaceIdx + 1) : "";
                String rmtLogArgs = buildRemoteLogArguments(spec.username(), spec.host());
                String tmpDir = env(ses, "%TEMP%", "C:\\Windows\\Temp", WINDOWS_ENCODING);
                String scriptOutputDir = tmpDir + "\\ignite-startNodes";

                shell(ses, "mkdir " + scriptOutputDir);

                String scriptFileName = scriptOutputDir + "\\" + id + ".bat";

                String createScript = new SB()
                    .a("echo \"").a(igniteHome).a('\\').a(scriptPath).a("\"")
                    .a(" ").a(scriptArgs)
                    .a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "")
                    .a(rmtLogArgs)
                    .a(" ^> ").a(scriptOutputDir).a("\\").a(scriptOutputFileName).a(" ^2^>^&^1")
                    .a(" > ").a(scriptFileName)
                    .toString();

                info("Create script with command: " + createScript, spec.logger(), log);

                shell(ses, createScript);

                String createTask = new SB()
                    .a("schtasks /create /f /sc onstart")
                    .a(" /ru ").a(spec.username())
                    .a(" /rp ").a(spec.password())
                    .a(" /tn ").a(id)
                    .a(" /np /tr \"").a(scriptFileName).a("\"")
                    .toString();

                info("Create task with command: " + createTask, spec.logger(), log);

                shell(ses, createTask);

                String runTask = new SB()
                    .a("schtasks /run /i")
                    .a(" /tn ").a(id)
                    .toString();

                info("Run task with command: " + runTask, spec.logger(), log);

                shell(ses, runTask);

                String deleteTask = new SB()
                    .a("schtasks /delete /f")
                    .a(" /tn ").a(id)
                    .toString();

                info("Delete task with command: " + deleteTask, spec.logger(), log);

                shell(ses, deleteTask);
            }
            else { // Assume Unix.
                int spaceIdx = script.indexOf(' ');

                String scriptPath = spaceIdx > -1 ? script.substring(0, spaceIdx) : script;
                String scriptArgs = spaceIdx > -1 ? script.substring(spaceIdx + 1) : "";
                String rmtLogArgs = buildRemoteLogArguments(spec.username(), spec.host());
                String tmpDir = env(ses, "$TMPDIR", "/tmp/");
                String scriptOutputDir = tmpDir + "ignite-startNodes";

                shell(ses, "mkdir " + scriptOutputDir);

                // Mac os don't support ~ in double quotes. Trying get home path from remote system.
                if (igniteHome.startsWith("~")) {
                    String homeDir = env(ses, "$HOME", "~");

                    igniteHome = igniteHome.replaceFirst("~", homeDir);
                }

                String startNodeCmd = new SB()
                    // Console output is consumed, started nodes must use Ignite file appenders for log.
                    .a("nohup ")
                    .a("\"").a(igniteHome).a('/').a(scriptPath).a("\"")
                    .a(" ").a(scriptArgs)
                    .a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "")
                    .a(rmtLogArgs)
                    .a(" > ").a(scriptOutputDir).a("/").a(scriptOutputFileName).a(" 2>& 1 &")
                    .toString();

                    info("Starting remote node with SSH command: " + startNodeCmd, spec.logger(), log);

                    shell(ses, startNodeCmd);
            }

            return new ClusterStartNodeResultImpl(spec.host(), true, null);
        }
        catch (IgniteInterruptedCheckedException e) {
            return new ClusterStartNodeResultImpl(spec.host(), false, e.getMessage());
        }
        catch (Exception e) {
            return new ClusterStartNodeResultImpl(spec.host(), false, X.getFullStackTrace(e));
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
     * @throws IgniteInterruptedCheckedException If thread was interrupted while waiting.
     */
    private void shell(Session ses, String cmd) throws JSchException, IOException, IgniteInterruptedCheckedException {
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
        return env(ses, name, dflt, Charset.defaultCharset().name());
    }

    /**
     * Gets the value of the specified environment variable.
     *
     * @param ses SSH session.
     * @param name environment variable name.
     * @param dflt default value.
     * @param encoding Process output encoding.
     * @return environment variable value.
     * @throws JSchException In case of SSH error.
     */
    private String env(Session ses, String name, String dflt, String encoding) throws JSchException {
        try {
            return exec(ses, "echo " + name, encoding);
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
        return exec(ses, cmd, Charset.defaultCharset().name());
    }

    /**
     * Gets the value of the specified environment variable.
     *
     * @param ses SSH session.
     * @param cmd environment variable name.
     * @param encoding Process output encoding.
     * @return environment variable value.
     * @throws JSchException In case of SSH error.
     * @throws IOException If failed.
     */
    private String exec(Session ses, String cmd, String encoding) throws JSchException, IOException {
        ChannelExec ch = null;

        try {
            ch = (ChannelExec)ses.openChannel("exec");

            ch.setCommand(cmd);

            ch.connect();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(ch.getInputStream(), encoding))) {
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
    public StartNodeCallable setLogger(IgniteLogger log) {
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