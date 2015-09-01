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

package org.apache.ignite.loadtest;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.varia.LevelRangeFilter;
import org.apache.log4j.varia.NullAppender;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Single execution test.
 */
public final class GridSingleExecutionTest {
    /** */
    public static final int JOB_COUNT = 50;

    /**
     * Private constructor because class has only static
     * methods and was considered as utility one by StyleChecker.
     */
    private GridSingleExecutionTest() {
        // No-op.
    }

    /**
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"CallToSystemExit"})
    public static void main(String[] args) throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        System.out.println("Starting master node [params=" + Arrays.toString(args) + ']');

        if (args.length < 2) {
            System.out.println("Log file name must be provided as first argument.");

            System.exit(1);
        }
        else if (args.length >= 2) {
            for (IgniteConfiguration cfg: getConfigurations(args[1], args[0])) {
                G.start(cfg);
            }
        }

        boolean useSes = false;

        if (args.length == 3) {
            if ("-session".equals(args[2].trim()))
                useSes = true;
        }

        try {
            Ignite ignite = G.ignite();

            IgniteCompute comp = ignite.compute().withAsync();

            // Execute Hello World task.
            comp.execute(!useSes ? TestTask.class : TestSessionTask.class, null);

            ComputeTaskFuture<Object> fut = comp.future();

            if (useSes) {
                fut.getTaskSession().setAttribute("attr1", 1);
                fut.getTaskSession().setAttribute("attr2", 2);
            }

            // Wait for task completion.
            fut.get();

            System.out.println("Task executed.");
        }
        finally {
            G.stop(true);

            System.out.println("Master node stopped.");
        }
    }

    /**
     * Initializes logger.
     *
     * @param log Log file name.
     * @return Logger.
     * @throws IgniteCheckedException If file initialization failed.
     */
    private static IgniteLogger initLogger(String log) throws IgniteCheckedException {

        Logger impl = Logger.getRootLogger();

        impl.removeAllAppenders();

        String fileName =  U.getIgniteHome() + "/work/log/" + log;

        // Configure output that should go to System.out
        RollingFileAppender fileApp;

        String fmt = "[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n";

        try {
            fileApp = new RollingFileAppender(new PatternLayout(fmt), fileName);

            fileApp.setMaxBackupIndex(0);

            fileApp.rollOver();
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to initialize file appender.", e);
        }

        LevelRangeFilter lvlFilter = new LevelRangeFilter();

        lvlFilter.setLevelMin(Level.DEBUG);

        fileApp.addFilter(lvlFilter);

        impl.addAppender(fileApp);

        // Configure output that should go to System.out
        ConsoleAppender conApp = new ConsoleAppender(new PatternLayout(fmt), ConsoleAppender.SYSTEM_OUT);

        lvlFilter = new LevelRangeFilter();

        lvlFilter.setLevelMin(Level.INFO);
        lvlFilter.setLevelMax(Level.INFO);

        conApp.addFilter(lvlFilter);

        impl.addAppender(conApp);

        // Configure output that should go to System.err
        conApp = new ConsoleAppender(new PatternLayout(fmt), ConsoleAppender.SYSTEM_ERR);

        conApp.setThreshold(Level.WARN);

        impl.addAppender(conApp);

        impl.setLevel(Level.INFO);

        Logger.getLogger("org.apache.ignite").setLevel(Level.DEBUG);

        return new GridTestLog4jLogger(false);
    }

    /**
     * Initializes configurations.
     *
     * @param springCfgPath Configuration file path.
     * @param log Log file name.
     * @return List of configurations.
     * @throws IgniteCheckedException If failed..
     */
    @SuppressWarnings("unchecked")
    private static Iterable<IgniteConfiguration> getConfigurations(String springCfgPath, String log) throws IgniteCheckedException {
        File path = GridTestUtils.resolveIgnitePath(springCfgPath);

        if (path == null) {
            throw new IgniteCheckedException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to IGNITE_HOME.");
        }

        if (!path.isFile())
            throw new IgniteCheckedException("Provided file path is not a file: " + path);

        // Add no-op logger to remove no-appender warning.
        Appender app = new NullAppender();

        Logger.getRootLogger().addAppender(app);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(path.toURI().toURL().toString());
        }
        catch (BeansException | MalformedURLException e) {
            throw new IgniteCheckedException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(IgniteConfiguration.class);
        }
        catch (BeansException e) {
            throw new IgniteCheckedException("Failed to instantiate bean [type=" + IgniteConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null)
            throw new IgniteCheckedException("Failed to find a single grid factory configuration in: " + path);

        // Remove previously added no-op logger.
        Logger.getRootLogger().removeAppender(app);

        if (cfgMap.isEmpty())
            throw new IgniteCheckedException("Can't find grid factory configuration in: " + path);

        Collection<IgniteConfiguration> res = new ArrayList<>();

        for (IgniteConfiguration cfg : (Collection<IgniteConfiguration>)cfgMap.values()) {
            UUID nodeId = UUID.randomUUID();

            cfg.setNodeId(nodeId);

            cfg.setGridLogger(initLogger(log));

            res.add(cfg);
        }

        return res;
    }

    /** */
    public static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> jobs = new ArrayList<>(JOB_COUNT);

            for (int i = 0; i < JOB_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @LoggerResource
                    private IgniteLogger log;

                    @Override public Serializable execute() {
                        if (log.isInfoEnabled())
                            log.info("Executing job [index=" + argument(0) + ']');

                        return argument(0);
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results != null : "Unexpected result [results=" + results + ']';
            assert results.size() == JOB_COUNT : "Unexpected result [results=" + results + ']';

            return null;
        }
    }

    /** */
    public static class TestSessionTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> jobs = new ArrayList<>(JOB_COUNT);

            for (int i = 0; i < JOB_COUNT; i++) {
                jobs.add(new ComputeJobAdapter(i) {
                    @LoggerResource
                    private IgniteLogger log;

                    @Override public Serializable execute() {
                        if (log.isInfoEnabled())
                            log.info("Executing job [index=" + argument(0) + ']');

                        ses.setAttribute("attr3", 3);
                        ses.setAttribute("attr4", 4);

                        return argument(0);
                    }
                });
            }

            ses.setAttribute("attr5", 5);
            ses.setAttribute("attr6", 6);

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) {
            ses.setAttribute("attr7", 7);
            ses.setAttribute("attr8", 8);

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results != null : "Unexpected result [results=" + results + ']';
            assert results.size() == JOB_COUNT : "Unexpected result [results=" + results + ']';

            return null;
        }
    }
}