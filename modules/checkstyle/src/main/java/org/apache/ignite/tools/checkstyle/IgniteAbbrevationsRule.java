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

package org.apache.ignite.tools.checkstyle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

/**
 * Rule that check Ignite abbervations used through project source code.
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/Abbreviation+Rules#AbbreviationRules-VariableAbbreviation">
 *     Ignite abbrevation rules</a>
 */
public class IgniteAbbrevationsRule extends AbstractCheck {
    /** File with abbrevations. */
    public static final String ABBREVS_FILE = "/abbrevations.csv";

    /** File with exclusions. */
    public static final String ABBREVS_EXCL_FILE = "/abbrevations-exclude.txt";

    /** */
    public static final char DELIM = ',';

    /** Include only Java processes in ProcessHandle dump. */
    private static final String JAVA_ONLY_PROP = "ignite.dump.system.diagnostics.java.only";

    /** Command timeout (seconds). */
    private static final String COMMAND_TIMEOUT_SEC_PROP = "ignite.dump.system.diagnostics.command.timeout.sec";

    /** Default command timeout in seconds. */
    private static final long DFLT_COMMAND_TIMEOUT_SEC = 30L;

    /** */
    private static final int[] TOKENS = new int[] {
        TokenTypes.VARIABLE_DEF, TokenTypes.PATTERN_VARIABLE_DEF
    };

    /**
     * Key is wrong term that should be replaced with abbrevations.
     * Value possible substitions to generate self-explained error message.
     */
    private static final Map<String, String> ABBREVS = new HashMap<>();

    /** Exclusions. */
    private static final Set<String> EXCL = new HashSet<>();

    static {
        forEachLine(ABBREVS_FILE, line -> {
            line = line.toLowerCase();

            int firstDelim = line.indexOf(DELIM);

            assert firstDelim > 0;

            String term = line.substring(0, firstDelim);
            String[] substitutions = line.substring(firstDelim + 1).split("" + DELIM);

            assert substitutions.length > 0;

            ABBREVS.put(term, String.join(", ", substitutions));
        });

        forEachLine(ABBREVS_EXCL_FILE, EXCL::add);
    }

    public IgniteAbbrevationsRule() {
        System.err.println("HERE!!!!!!!");
        dumpDiagnosticsIfEnabled();
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST parent = ast.getParent();

        if (parent.getType() == TokenTypes.OBJBLOCK)
            return;

        DetailAST token = ast.findFirstToken(TokenTypes.IDENT);

        String varName = token.getText();

        if (EXCL.contains(varName))
            return;

        List<String> words = words(varName);

        for (String word : words) {
            if (ABBREVS.containsKey(word.toLowerCase())) {
                log(
                    token.getLineNo(),
                    "Abbrevation should be used for {0}! Please, use {1}, instead of {2}",
                    varName,
                    ABBREVS.get(word.toLowerCase()),
                    word
                );
            }
        }
    }

    /**
     * @param varName Variable name.
     * @return Words list.
     */
    public static List<String> words(String varName) {
        if (varName.indexOf('_') != -1)
            return Arrays.asList(varName.split("_"));

        List<String> words = new ArrayList<>();

        int start = 0;
        boolean allUpper = Character.isUpperCase(varName.charAt(0));

        for (int i = 1; i < varName.length(); i++) {
            if (Character.isUpperCase(varName.charAt(i))) {
                if (allUpper)
                    continue;

                words.add(varName.substring(start, i));
                start = i;
                allUpper = true;
            }
            else
                allUpper = false;
        }

        words.add(varName.substring(start));

        return words;
    }

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return TOKENS.clone();
    }

    /** */
    private static void dumpDiagnosticsIfEnabled() {
        String os = System.getProperty("os.name", "unknown");

        System.err.println("=== Ignite system diagnostics dump start ===");
        System.err.println("OS=" + os + " java.version=" + System.getProperty("java.version", "N/A"));
        System.err.println("java.only=" + Boolean.getBoolean(JAVA_ONLY_PROP));

        dumpProcesses();
        dumpPortState(os);

        System.err.println("=== Ignite system diagnostics dump end ===");
    }

    /** */
    private static void dumpProcesses() {
        boolean javaOnly = Boolean.getBoolean(JAVA_ONLY_PROP);

        System.err.println("=== ProcessHandle dump ===");

        ProcessHandle.allProcesses()
            .sorted(Comparator.comparingLong(ProcessHandle::pid))
            .filter(proc -> !javaOnly || isJavaProcess(proc.info()))
            .forEach(IgniteAbbrevationsRule::printProcess);
    }

    /** */
    private static boolean isJavaProcess(ProcessHandle.Info info) {
        String cmd = info.command().orElse("").toLowerCase(Locale.ROOT);
        String cmdLine = info.commandLine().orElse("").toLowerCase(Locale.ROOT);

        return cmd.contains("java") || cmdLine.contains("java");
    }

    /** */
    private static void printProcess(ProcessHandle proc) {
        ProcessHandle.Info info = proc.info();

        long ppid = proc.parent().map(ProcessHandle::pid).orElse(-1L);

        String cmd = info.command().orElse("N/A");
        String cmdLine = info.commandLine().orElse("N/A");
        String user = info.user().orElse("N/A");
        String start = info.startInstant().map(Object::toString).orElse("N/A");
        String cpu = info.totalCpuDuration().map(Duration::toString).orElse("N/A");
        String args = info.arguments().map(arr -> String.join(" ", arr)).orElse("N/A");

        System.out.println(
            "PID=" + proc.pid() +
                " PPID=" + ppid +
                " ALIVE=" + proc.isAlive() +
                " USER=" + user +
                " START=" + start +
                " CPU=" + cpu +
                " CMD=" + cmd +
                " CMD_LINE=" + cmdLine +
                " ARGS=" + args
        );
    }

    /** */
    private static void dumpPortState(String osName) {
        System.out.println("=== Port/socket dump ===");

        for (List<String> cmd : diagnosticCommands(osName))
            runAndPrint(cmd);
    }

    /** */
    private static List<List<String>> diagnosticCommands(String osName) {
        String os = osName == null ? "" : osName.toLowerCase(Locale.ROOT);

        List<List<String>> cmds = new ArrayList<>();

        if (os.contains("win")) {
            cmds.add(asCmd("cmd", "/c", "netstat -ano"));
            cmds.add(asCmd("cmd", "/c", "netstat -anob"));

            return cmds;
        }

        cmds.add(asCmd("ps", "-axo", "pid,ppid,user,stat,etime,command"));
        cmds.add(asCmd("lsof", "-nP", "-iTCP", "-sTCP:LISTEN"));
        cmds.add(asCmd("lsof", "-nP", "-iUDP"));
        cmds.add(asCmd("lsof", "-nP", "-i"));

        if (os.contains("linux")) {
            cmds.add(asCmd("ss", "-lntup"));
            cmds.add(asCmd("ss", "-ntup"));
            cmds.add(asCmd("netstat", "-lntup"));
        }
        else if (os.contains("mac") || os.contains("darwin")) {
            cmds.add(asCmd("netstat", "-anv", "-p", "tcp"));
            cmds.add(asCmd("netstat", "-anv", "-p", "udp"));
        }

        return cmds;
    }

    /** */
    private static List<String> asCmd(String... args) {
        List<String> cmd = new ArrayList<>(args.length);

        for (String arg : args)
            cmd.add(arg);

        return cmd;
    }

    /** */
    private static void runAndPrint(List<String> cmd) {
        String cmdStr = String.join(" ", cmd);

        System.out.println(">>> " + cmdStr);

        Process proc;

        try {
            proc = new ProcessBuilder(cmd)
                .redirectErrorStream(true)
                .start();
        }
        catch (IOException e) {
            System.out.println("Command not available: " + cmdStr + " [" + e.getMessage() + "]");

            return;
        }

        try (BufferedReader rdr = new BufferedReader(
            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)
        )) {
            String line;

            while ((line = rdr.readLine()) != null)
                System.out.println(line);

            if (!proc.waitFor(commandTimeoutSec(), TimeUnit.SECONDS)) {
                proc.destroyForcibly();

                System.out.println("Command timed out: " + cmdStr);

                return;
            }

            System.out.println("[exit=" + proc.exitValue() + "]");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            System.out.println("Interrupted while running command: " + cmdStr);
        }
        catch (IOException e) {
            System.out.println("Failed to read command output: " + cmdStr + " [" + e.getMessage() + "]");
        }
    }

    /** */
    private static long commandTimeoutSec() {
        long timeout = Long.getLong(COMMAND_TIMEOUT_SEC_PROP, DFLT_COMMAND_TIMEOUT_SEC);

        return Math.max(timeout, 1L);
    }

    /** */
    private static void forEachLine(String file, Consumer<String> lineProc) {
        InputStream stream = IgniteAbbrevationsRule.class.getResourceAsStream(file);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            String line;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;

                lineProc.accept(line);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
