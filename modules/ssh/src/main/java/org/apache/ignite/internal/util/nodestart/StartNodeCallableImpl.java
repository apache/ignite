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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Pattern;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelShell;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterStartNodeResultImpl;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

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
    private static final String DFLT_SCRIPT_WIN = "bin\\ignite.bat -v -np";

    /** Default start script path for Linux. */
    private static final String DFLT_SCRIPT_LINUX = "bin/ignite.sh -v";

    /** Date format for log file name. */
    private static final SimpleDateFormat FILE_NAME_DATE_FORMAT = new SimpleDateFormat("MM-dd-yyyy--HH-mm-ss");

    /** Used to register successful node start in log */
    private static final String SUCCESSFUL_START_MSG = "Successfully bound to TCP port";

    /**  */
    private static final long EXECUTE_WAIT_TIME = 1000;

    /**  */
    private static final long NODE_START_CHECK_PERIOD = 2000;

    /**  */
    private static final long NODE_START_CHECK_LIMIT = 15;

    /** Specification. */
    private final IgniteRemoteStartSpecification spec;

    /** Connection timeout. */
    private final int timeout;

    /** Timeout processor. */
    private GridTimeoutProcessor proc;

    /** Logger. */
    @LoggerResource
    private transient IgniteLogger log;

    /** Ignite. */
    @IgniteInstanceResource
    private transient Ignite ignite;

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
            proc = ((IgniteEx)ignite).context().timeout();

            if (spec.key() != null)
                ssh.addIdentity(spec.key().getAbsolutePath());

            ses = ssh.getSession(spec.username(), spec.host(), spec.port());

            if (spec.password() != null)
                ses.setPassword(spec.password());

            ses.setConfig("StrictHostKeyChecking", "no");

            ses.connect(timeout);

            boolean win = isWindows(ses);

            info("Windows mode: " + win, spec.logger(), log);

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

            String id = FILE_NAME_DATE_FORMAT.format(new Date()) + '-' + UUID.randomUUID().toString().substring(0, 8);

            String scriptOutputFileName = id + ".log";

            int spaceIdx = script.indexOf(' ');

            String scriptPath = spaceIdx > -1 ? script.substring(0, spaceIdx) : script;

            String scriptArgs = spaceIdx > -1 ? script.substring(spaceIdx + 1) : "";

            String rmtLogArgs = buildRemoteLogArguments(spec.username(), spec.host());

            String scriptOutputDir;

            String dfltTmpDir = igniteHome + separator + "work" + separator + "log";

            if (win) {
                String tmpDir = env(ses, "%TMPDIR%", dfltTmpDir, WINDOWS_ENCODING);

                if ("%TMPDIR%".equals(tmpDir))
                    tmpDir = dfltTmpDir;

                scriptOutputDir = tmpDir + "\\ignite-startNodes";
            }
            else { // Assume Unix.
                String logDir = env(ses, "$TMPDIR", dfltTmpDir);

                scriptOutputDir = logDir + "/ignite-startNodes";
            }

            shell(ses, "mkdir " + scriptOutputDir);

            String scriptOutputPath = scriptOutputDir + separator + scriptOutputFileName;

            String findSuccess;

            if (win) {
                String scriptFileName = scriptOutputDir + '\\' + id + ".bat";

                String createScript = new SB()
                    .a("echo \"").a(igniteHome).a('\\').a(scriptPath).a("\" ")
                    .a(scriptArgs)
                    .a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "")
                    .a(rmtLogArgs)
                    .a(" ^> ").a(scriptOutputPath).a(" ^2^>^&^1")
                    .a(" > ").a(scriptFileName)
                    .toString();

                info("Create script with command: " + createScript, spec.logger(), log);

                shell(ses, createScript);

                try {
                    String createTask = new SB()
                        .a("schtasks /create /f /sc onstart")
                        .a(" /ru ").a(spec.username())
                        .a(" /rp ").a(spec.password())
                        .a(" /tn ").a(id)
                        .a(" /np /tr \"").a(scriptFileName).a('\"')
                        .toString();

                    info("Create task with command: " + createTask, spec.logger(), log);

                    shell(ses, createTask);

                    String runTask = "schtasks /run /i /tn " + id;

                    info("Run task with command: " + runTask, spec.logger(), log);

                    shell(ses, runTask);
                }
                finally {
                    String deleteTask = "schtasks /delete /f /tn " + id;

                    info("Delete task with command: " + deleteTask, spec.logger(), log);

                    shell(ses, deleteTask);
                }

                findSuccess = "find \"" + SUCCESSFUL_START_MSG + "\" " + scriptOutputPath;
            }
            else { // Assume Unix.
                // Mac os don't support ~ in double quotes. Trying get home path from remote system.
                if (igniteHome.startsWith("~")) {
                    String homeDir = env(ses, "$HOME", "~");

                    igniteHome = igniteHome.replaceFirst("~", homeDir);
                }

                String prepareStartCmd = new SB()
                    // Ensure diagnostics in the log even in case if start node breaks silently.
                    .a("nohup echo \"Preparing to start remote node...\" > ")
                    .a(scriptOutputDir).a('/').a(scriptOutputFileName).a(" 2>& 1 &")
                    .toString();

                shell(ses, prepareStartCmd);

                String startNodeCmd = new SB()
                    // Console output is consumed, started nodes must use Ignite file appenders for log.
                    .a("nohup ")
                    .a("\"").a(igniteHome).a('/').a(scriptPath).a("\"")
                    .a(" ").a(scriptArgs)
                    .a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "")
                    .a(rmtLogArgs)
                    .a(" > ").a(scriptOutputDir).a('/').a(scriptOutputFileName).a(" 2>& 1 &")
                    .toString();

                info("Starting remote node with SSH command: " + startNodeCmd, spec.logger(), log);

                // Execute command via ssh and wait until id of new process will be found in the output.
                shell(ses, startNodeCmd, "\\[(\\d)\\] (\\d)+");

                findSuccess = "grep \"" + SUCCESSFUL_START_MSG + "\" " + scriptOutputPath;
            }

            for (int i = 0; i < NODE_START_CHECK_LIMIT; ++i) {
                Thread.sleep(NODE_START_CHECK_PERIOD);

                String res = exec(ses, findSuccess, win ? WINDOWS_ENCODING : null);

                info("Find result: " + res, spec.logger(), log);

                if (res != null && res.contains(SUCCESSFUL_START_MSG))
                    return new ClusterStartNodeResultImpl(spec.host(), true, null);
            }

            return new ClusterStartNodeResultImpl(spec.host(), false, "Remote node could not start. " +
                "See log for details: " + scriptOutputPath);
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
        shell(ses, cmd, null);
    }

    /**
     * Executes command using {@code shell} channel.
     *
     * @param ses SSH session.
     * @param cmd Command.
     * @param regexp Regular expression to wait until it will be found in stream from node.
     * @throws JSchException In case of SSH error.
     * @throws IOException If IO error occurs.
     * @throws IgniteInterruptedCheckedException If thread was interrupted while waiting.
     */
    private void shell(Session ses, String cmd, String regexp)
        throws JSchException, IOException, IgniteInterruptedCheckedException {
        ChannelShell ch = null;

        GridTimeoutObject to = null;

        try {
            ch = (ChannelShell)ses.openChannel("shell");

            ch.connect();

            try (PrintStream out = new PrintStream(ch.getOutputStream(), true)) {
                out.println(cmd);
            }

            if (regexp != null) {
                Pattern ptrn = Pattern.compile(regexp);

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(ch.getInputStream()))) {
                    String line;

                    boolean first = true;

                    while ((line = reader.readLine()) != null) {
                        if (ptrn.matcher(line).find()) {
                            // Wait for a while until process from regexp really will be started.
                            U.sleep(50);

                            break;
                        }
                        else if (first) {
                            to = initTimer(cmd);

                            first = false;
                        }
                    }
                }
                catch (InterruptedIOException ignore) {
                    // No-op.
                }
                finally {
                    if (to != null) {
                        boolean r = proc.removeTimeoutObject(to);

                        assert r || to.endTime() <= U.currentTimeMillis() : "Timeout object was not removed: " + to;
                    }
                }

            }
            else
                U.sleep(EXECUTE_WAIT_TIME);
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
        return env(ses, name, dflt, null);
    }

    /**
     * Gets the value of the specified environment variable.
     *
     * @param ses SSH session.
     * @param name environment variable name.
     * @param dflt default value.
     * @param encoding Process output encoding, {@code null} for default charset encoding.
     * @return environment variable value.
     * @throws JSchException In case of SSH error.
     */
    private String env(Session ses, String name, String dflt, String encoding) throws JSchException {
        try {
            String res = exec(ses, "echo " + name, encoding);

            if (res == null)
                return dflt;

            res = res.trim();

            return res.isEmpty() ? dflt : res;
        }
        catch (IOException ignored) {
            return dflt;
        }
    }

    /**
     * Executes command using {@code exec} channel.
     *
     * @param ses SSH session.
     * @param cmd Command.
     * @return Output result.
     * @throws JSchException In case of SSH error.
     * @throws IOException If failed.
     */
    private String exec(Session ses, String cmd) throws JSchException, IOException {
        return exec(ses, cmd, null);
    }

    /**
     * Executes command using {@code exec} channel with setting encoding.
     *
     * @param ses SSH session.
     * @param cmd Command.
     * @param encoding Process output encoding, {@code null} for default charset encoding.
     * @return Output result.
     * @throws JSchException In case of SSH error.
     * @throws IOException If failed.
     */
    private String exec(Session ses, final String cmd, String encoding) throws JSchException, IOException {
        ChannelExec ch = null;

        try {
            ch = (ChannelExec)ses.openChannel("exec");

            ch.setCommand(cmd);

            ch.connect();

            if (encoding == null)
                encoding = Charset.defaultCharset().name();

            GridTimeoutObject to = null;

            SB out = null;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(ch.getInputStream(), encoding))) {
                String line;

                boolean first = true;

                while ((line = reader.readLine()) != null) {
                    if (first)
                        out = new SB();
                    else
                        out.a('\n');

                    out.a(line);

                    if (first) {
                        to = initTimer(cmd);

                        first = false;
                    }
                }
            }
            catch (InterruptedIOException ignore) {
                // No-op.
            }
            finally {
                if (to != null) {
                    boolean r = proc.removeTimeoutObject(to);

                    assert r || to.endTime() <= U.currentTimeMillis() : "Timeout object was not removed: " + to;
                }
            }

            return out == null ? null : out.toString();
        }
        finally {
            if (ch != null && ch.isConnected())
                ch.disconnect();
        }
    }

    /**
     * Initialize timer to wait for command execution.
     *
     * @param cmd Command to log.
     */
    private GridTimeoutObject initTimer(String cmd) {
        GridTimeoutObject to = new GridTimeoutObjectAdapter(EXECUTE_WAIT_TIME) {
            private final Thread thread = Thread.currentThread();

            @Override public void onTimeout() {
                thread.interrupt();
            }

            @Override public String toString() {
                return S.toString("GridTimeoutObject", "cmd", cmd, "thread", thread);
            }
        };

        boolean wasAdded = proc.addTimeoutObject(to);

        assert wasAdded : "Timeout object was not added: " + to;

        return to;
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
