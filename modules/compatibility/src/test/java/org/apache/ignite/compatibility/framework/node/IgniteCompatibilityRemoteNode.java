/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.framework.node;

import com.thoughtworks.xstream.XStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp;
import org.apache.ignite.compatibility.framework.IgniteCompatibilityTestConfig;
import org.apache.ignite.compatibility.start.IgniteCompatibilityStartNodeClosure;
import org.apache.ignite.compatibility.utils.GridCompatibilityCheckTopology;
import org.apache.ignite.compatibility.utils.IgniteCompatibilityActivate;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteProductVersion;

import static java.util.logging.Level.SEVERE;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.FAILED_CLOSURE_MSG;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.FAILED_COMMAND_MSG;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.FAILED_MSG;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.FINISHED_CLOSURE_MSG;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.FINISHED_COMMAND_MSG;
import static org.apache.ignite.compatibility.IgniteCompatibilityRemoteNodeStartApp.START_NODE_PREFIX;

/**
 *
 */
public class IgniteCompatibilityRemoteNode {
    /** */
    private static Logger log = Logger.getLogger(IgniteCompatibilityRemoteNode.class.getName());

    /** */
    public static final String TEST_ALL_VERSIONS_PROP = "ignite.compatibility.all";

    /** */
    public static final long DFLT_CLOSURE_TIMEOUT = 30_000;

    /** */
    private static final String BASE_PATH;

    /** */
    private static final int VERSIONS_PER_RELEASE = 2;

    /** */
    private static final List<String> VERSIONS;

    /** */
    public static final String CURRENT_VER = IgniteCompatibilityTestConfig.get().currentVersion();

    /** Prefix of work directory of ignite node. Suffix is node name. */
    public static final String WORK_DIR_PREFIX = "workDir_";

    /** */
    private static final Comparator<String> VERSION_COMPARATOR = new Comparator<String>() {
        @Override public int compare(String o1, String o2) {
            return IgniteProductVersion.fromString(o1).compareTo(IgniteProductVersion.fromString(o2));
        }
    };

    /*
     *
     */
    static {
        String baseDir = IgniteCompatibilityTestConfig.get().versionsDirectory();

        BASE_PATH = baseDir == null ? IgniteCompatibilityTestConfig.get().defaultBasePath() : baseDir;

        log.info("Base directory: " + BASE_PATH);

        List<String> verList = new ArrayList<>();

        File[] files = new File(BASE_PATH).listFiles();

        if (files != null) {
            for (File versionDir : files) {
                if (versionDir.isDirectory()
                    && !versionDir.getName().endsWith("_") && !versionDir.getName().startsWith("_") && !versionDir.getName().equals("base")) {
                    assert versionDir.getName().matches("\\d+\\.\\d+\\.\\d+(-p\\d+)?") : versionDir.getName();
                    assert new File(versionDir, "pom.xml").exists() : versionDir.getAbsolutePath();

                    verList.add(versionDir.getName());
                }
            }
        }

        verList.sort(VERSION_COMPARATOR);

        VERSIONS = Collections.unmodifiableList(verList);

        System.out.println("All available previous versions: " + VERSIONS);
    }

    /**
     * @return List of all versions from 'ignite-versions' directory.
     */
    private static List<String> allPreviousVersions() {
        return VERSIONS;
    }

    /**
     * @return List of all versions to use in test.
     */
    public static List<String> previousVersions() {
        boolean testAllVers = Boolean.getBoolean(TEST_ALL_VERSIONS_PROP);

        if (testAllVers)
            return allPreviousVersions();

        Map<String, List<String>> byRelease = new HashMap<>();

        for (String ver : allPreviousVersions()) {
            IgniteProductVersion ver0 = IgniteProductVersion.fromString(ver);

            String release = ver0.major() + "." + ver0.minor();

            List<String> releaseVers = byRelease.computeIfAbsent(release, k -> new ArrayList<>());

            releaseVers.add(ver);
        }

        List<String> verList = new ArrayList<>();

        for (List<String> vers : byRelease.values()) {
            vers.sort(VERSION_COMPARATOR);

            int idx = Math.max(0, vers.size() - VERSIONS_PER_RELEASE);

            for (int i = 0; i < VERSIONS_PER_RELEASE && idx < vers.size(); i++)
                verList.add(vers.get(idx++));
        }

        verList.sort(VERSION_COMPARATOR);

        return verList;
    }

    /** */
    private static boolean testStarted;

    /** */
    private static final Map<String, Integer> startedNodeVersions = new HashMap<>();

    /** */
    private static final List<IgniteCompatibilityRemoteNode> startedNodes = new ArrayList<>();

    /**
     *
     */
    public static synchronized void startTest() {
        if (testStarted)
            throw new IllegalStateException("Test already started");

        assert F.isEmpty(startedNodes);

        testStarted = true;
    }

    /**
     *
     */
    public static synchronized void dumpDebugInfoForStarted() {
        for (IgniteCompatibilityRemoteNode node : startedNodes)
            node.dumpDebugInfo();
    }

    /**
     *
     */
    public static synchronized void stopTest() {
        testStarted = false;

        if (!startedNodes.isEmpty()) {
            ListIterator<IgniteCompatibilityRemoteNode> it = startedNodes.listIterator(startedNodes.size() - 1);

            while (it.hasPrevious())
                it.previous().startStop();

            while (it.hasPrevious())
                it.previous().waitStop();
        }

        startedNodes.clear();
        startedNodeVersions.clear();
    }

    /**
     * @param ver Node version.
     * @return Node index.
     */
    public static synchronized int nextNodeIndex(String ver) {
        Integer cnt = startedNodeVersions.get(ver);

        if (cnt == null) {
            cnt = -1;
        }

        int next = cnt + 1;

        startedNodeVersions.put(ver, next);

        return next;
    }

    /** */
    private static final String TMP_PATH = System.getProperty("java.io.tmpdir") +
        File.separator + "ign_compatibility.tmp_";

    /** */
    private boolean started;

    /** */
    private String name;

    /** */
    private GridJavaProcess proc;

    /** */
    private volatile CountDownLatch latch;

    /** */
    private volatile CountDownLatch closureLatch;

    /** */
    private volatile boolean closureFailed;

    /** */
    private final IgniteInClosure<String> outC = new IgniteInClosure<String>() {
        @Override public void apply(String s) {
            System.out.println(name + ">>>" + s);

            if (!started) {
                if (s.contains(START_NODE_PREFIX)) {
                    started = true;

                    assert latch != null;

                    latch.countDown();
                }
                else if (s.contains(FAILED_MSG)) {
                    latch.countDown();
                }
            }
            else {
                CountDownLatch latch0 = latch;

                if (latch0 != null) {
                    if (s.contains(FINISHED_COMMAND_MSG))
                        latch0.countDown();
                    else if (s.contains(FAILED_COMMAND_MSG)) {
                        latch0.countDown();
                    }
                }

                latch0 = closureLatch;

                if (latch0 != null) {
                    if (s.contains(FINISHED_CLOSURE_MSG))
                        latch0.countDown();
                    else if (s.contains(FAILED_CLOSURE_MSG)) {
                        closureFailed = true;

                        latch0.countDown();
                    }
                }
            }
        }
    };

    /**
     * @param ver Version.
     * @return Classpath.
     */
    public static String classpathForVersion(String ver) {
        assert !ver.equals(CURRENT_VER) : ver;
        String ggPath = BASE_PATH + File.separator + ver;

        File folder = new File(ggPath + "/target");

        File[] files = folder.listFiles();

        if (files == null)
            throw new IllegalStateException("The folder does not contain files: " + folder.getAbsolutePath());

        StringBuilder cp = new StringBuilder();

        cp.append(ggPath).append("/target/classes");

        boolean foundJar = false;

        for (File file : files) {
            if (file.isFile() && file.getName().endsWith(".jar")) {
                cp.append(File.pathSeparatorChar).append(file.getAbsolutePath());

                foundJar = true;
            }
        }

        if (!foundJar)
            throw new IllegalStateException("Can not find jar files in the folder: " + folder.getAbsolutePath());

        return cp.toString();
    }

    /**
     * @param ver Ignite version string (e.g. "2.7.0").
     * @param c Node start closure.
     * @return Node.
     * @throws Exception If failed.
     */
    public static synchronized IgniteCompatibilityRemoteNode startVersion(
        String ver,
        IgniteOutClosure<Ignite> c) throws Exception {
        if (ver.equals(CURRENT_VER))
            return startCurrentVersion(c);

        return new IgniteCompatibilityRemoteNode(ver, classpathForVersion(ver), c);
    }

    /**
     * @param c Configuration closure.
     * @return Node.
     * @throws Exception If failed.
     */
    public static synchronized IgniteCompatibilityRemoteNode startCurrentVersion(
        IgniteOutClosure<Ignite> c
    ) throws Exception {
        return new IgniteCompatibilityRemoteNode(
            CURRENT_VER,
            null,
            c);
    }

    /**
     * @param ver Node version string.
     * @param classpath Node classpath.
     * @param c Node start closure.
     * @throws Exception If failed.
     */
    private IgniteCompatibilityRemoteNode(
        String ver,
        String classpath,
        IgniteOutClosure<Ignite> c)
        throws Exception {
        if (!testStarted)
            throw new IllegalStateException("Test is not started");

        String suffix = null;

        if (c instanceof IgniteCompatibilityStartNodeClosure) {
            suffix = ((IgniteCompatibilityStartNodeClosure)c).clientMode() ? "client" : "server";
        }

        String name = ver + "-n" + nextNodeIndex(ver);

        if (suffix != null)
            name += "-" + suffix;

        this.name = name;

        if (c instanceof IgniteCompatibilityStartNodeClosure) {
            IgniteCompatibilityStartNodeClosure c0 = (IgniteCompatibilityStartNodeClosure)c;

            String nodeName = c0.getNodeName();

            if (nodeName == null)
                c0.nodeName(name);

            if (c0.workDirectory() == null) {
                String igniteHome = IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_HOME);

                File workDir = new File(new File(igniteHome), WORK_DIR_PREFIX + c0.getNodeName());

                c0.workDirectory(workDir.getAbsolutePath());
            }
        }

        start(ver, classpath, c);

        startedNodes.add(this);
    }

    /**
     * @return Node name.
     */
    public String name() {
        return name;
    }

    /**
     * @param ver Node version.
     * @param classpath Remove JVM classpath.
     * @param c Configuration closure.
     * @throws Exception If failed.
     */
    private void start(String ver, String classpath, IgniteOutClosure<Ignite> c) throws Exception {
        log.info("Try start node: " + name);

        latch = new CountDownLatch(1);

        String params = storeToFile(c);

        List<String> args = new ArrayList<>();

        args.add("-Xms64m");
        args.add("-Xmx512m");

        if (classpath != null) {
            args.add("-cp");
            args.add(classpath);

            log.warning("Classpath -> " + classpath);
        }

        if(!IgniteCompatibilityTestConfig.get().assertionExcludes().contains(ver))
            args.add("-ea");

        args.add("-DIGNITE_QUIET=false");
        args.add("-DIGNITE_UPDATE_NOTIFIER=false");
        args.add("-DIGNITE_NO_DISCO_ORDER=true");
        args.add("-DIGNITE_PERFORMANCE_SUGGESTIONS_DISABLED=true");
        args.add("-DIGNITE_NO_SHUTDOWN_HOOK=true");

        // TODO GG-14585 Remove or refactor this property usage when the ticket is resolved.
        args.add("-DIGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED=false");

        Set<Object> props = System.getProperties().keySet();

        for (String prop : IgniteCompatibilityStartNodeClosure.TEST_PROPS) {
            if (props.contains(prop))
                args.add("-D" + prop + "=" + System.getProperty(prop));
        }

        proc = GridJavaProcess.exec(IgniteCompatibilityRemoteNodeStartApp.class.getName(),
            params,
            null,
            outC,
            null,
            null,
            args,
            null);

        long end = System.currentTimeMillis() + 3 * 60_000;

        while (System.currentTimeMillis() < end) {
            latch.await(5_000, TimeUnit.MILLISECONDS);

            if (started)
                break;

            try {
                int exitCode = proc.getProcess().exitValue();

                log.warning("Failed to wait for correct process start, " +
                    "process finished with code: " + exitCode);

                // log.warning("Remote jvm classpath: " + classpath);

                break;
            }
            catch (IllegalThreadStateException ignore) {
                // No-op.
            }
        }

        if (!started)
            throw new Exception("Failed to start remote node.");

        log.info("Started node: " + name);
    }

    /**
     * @param nodes Nodes to stop.
     */
    public static void stopAll(Iterable<IgniteCompatibilityRemoteNode> nodes) {
        for (IgniteCompatibilityRemoteNode node : nodes)
            node.startStop();

        for (IgniteCompatibilityRemoteNode node : nodes)
            node.waitStop();
    }

    /**
     *
     */
    public void startStop() {
        try {
            if (!started)
                return;

            log.info("Start stop node: " + name);

            sendCommand("stop");
        }
        catch (Throwable e) {
            log.log(SEVERE, "Failed to start node stop: " + name, e);
        }
    }

    /**
     *
     */
    public void waitStop() {
        try {
            Process process = proc.getProcess();

            if (!process.waitFor(1, TimeUnit.MINUTES)) {
                U.error(null, "Failed to wait for a graceful remote JVM termination (will kill): " + process);

                process.destroy();

                process.waitFor();
            }

            started = false;
        }
        catch (Throwable e) {
            log.log(SEVERE, "Failed to stop node " + name, e);
        }

    }

    /**
     *
     */
    public void stop() {
        startStop();

        waitStop();
    }

    /**
     *
     */
    public void dumpDebugInfo() {
        try {
            latch = new CountDownLatch(1);

            sendCommand("dumpDebugInfo");

            if (!latch.await(10, TimeUnit.SECONDS))
                log.warning("Failed to wait for dumpDebugInfo");
        }
        catch (Throwable e) {
            log.log(SEVERE, "Failed to execute dumpDebugInfo command for " + name, e);
        }
    }

    /**
     * @param expSrvs Expected servers number.
     * @param expClients Expected clients number.
     * @throws Exception If failed.
     */
    public void checkTopology(int expSrvs, int expClients) throws Exception {
        run(new GridCompatibilityCheckTopology(expSrvs, expClients), 15_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void activate() throws Exception {
        run(new IgniteCompatibilityActivate(), 3 * 60_000);
    }

    /**
     * @param c Closure.
     * @throws Exception If failed.
     */
    public void run(IgniteInClosure<Ignite> c) throws Exception {
        run(c, DFLT_CLOSURE_TIMEOUT);
    }

    /**
     * @param c Closure.
     * @param timeoutMs Closure timeout in milliseconds.
     * @throws Exception If failed.
     */
    public void run(IgniteInClosure<Ignite> c, long timeoutMs) throws Exception {
        boolean done;

        try {
            if (!started)
                throw new IllegalStateException("Node is not started.");

            String path = storeToFile(c);

            closureLatch = new CountDownLatch(1);

            sendCommand("runClosure");
            sendCommand(path);

            done = closureLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (Throwable t) {
            log.log(SEVERE, "Failed to run closure [node=" + name + ", c=" + c.getClass().getSimpleName() + "]", t);

            throw new Exception("Failed to run closure [node=" + name +
                ", c=" + c.getClass().getSimpleName() + "]", t);
        }

        if (!done) {
            log.warning("Failed to wait for command execution [c=" + c.getClass().getSimpleName() +
                ", timeout=" + timeoutMs +
                ", node=" + name + ']');

            throw new TimeoutException("Failed to wait for command execution [c=" + c.getClass().getSimpleName() +
                ", timeout=" + timeoutMs +
                ", node=" + name + ']');
        }

        if (closureFailed)
            throw new Exception("Failed to execute command [c=" + c.getClass().getSimpleName() + ", node=" + name + ']');
    }

    /**
     * @param cmd Command.
     * @throws Exception if failed.
     */
    private void sendCommand(String cmd) throws Exception {
        if (proc == null)
            throw new IllegalStateException();

        proc.getProcess().getOutputStream().write((cmd + "\n").getBytes(StandardCharsets.UTF_8));
        proc.getProcess().getOutputStream().flush();
    }

    /**
     * @param obj Object to store.
     * @return Path to file.
     * @throws IOException If failed.
     */
    private static String storeToFile(Object obj) throws IOException {
        assert obj != null;

        String fileName = TMP_PATH + obj.getClass().getSimpleName();

        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName))) {
            new XStream().toXML(obj, out);
        }

        return fileName;
    }
}
