/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.loadtest;

import org.apache.log4j.*;
import org.apache.log4j.varia.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.logger.*;
import org.springframework.beans.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

import java.io.*;
import java.net.*;
import java.util.*;

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
        System.setProperty(GridSystemProperties.GG_UPDATE_NOTIFIER, "false");

        System.out.println("Starting master node [params=" + Arrays.toString(args) + ']');

        if (args.length < 2) {
            System.out.println("Log file name must be provided as first argument.");

            System.exit(1);
        }
        else if (args.length >= 2) {
            for (GridConfiguration cfg: getConfigurations(args[1], args[0])) {
                G.start(cfg);
            }
        }

        boolean useSession = false;

        if (args.length == 3) {
            if ("-session".equals(args[2].trim())) {
                useSession = true;
            }
        }

        try {
            Grid grid = G.grid();

            // Execute Hello World task.
            GridComputeTaskFuture<Object> fut = grid.compute().execute(!useSession ? TestTask.class : TestSessionTask.class,
                null);

            if (useSession) {
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
     * @throws GridException If file initialization failed.
     */
    private static GridLogger initLogger(String log) throws GridException {

        Logger impl = Logger.getRootLogger();

        impl.removeAllAppenders();

        String fileName =  U.getGridGainHome() + "/work/log/" + log;

        // Configure output that should go to System.out
        RollingFileAppender fileApp;

        String fmt = "[%d{ABSOLUTE}][%-5p][%t][%c{1}] %m%n";

        try {
            fileApp = new RollingFileAppender(new PatternLayout(fmt), fileName);

            fileApp.setMaxBackupIndex(0);

            fileApp.rollOver();
        }
        catch (IOException e) {
            throw new GridException("Unable to initialize file appender.", e);
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

        Logger.getLogger("org.gridgain").setLevel(Level.DEBUG);

        return new GridTestLog4jLogger(false);
    }

    /**
     * Initializes configurations.
     *
     * @param springCfgPath Configuration file path.
     * @param log Log file name.
     * @return List of configurations.
     * @throws GridException If failed..
     */
    @SuppressWarnings("unchecked")
    private static Iterable<GridConfiguration> getConfigurations(String springCfgPath, String log) throws GridException {
        File path = GridTestUtils.resolveGridGainPath(springCfgPath);

        if (path == null) {
            throw new GridException("Spring XML configuration file path is invalid: " + new File(springCfgPath) +
                ". Note that this path should be either absolute path or a relative path to GRIDGAIN_HOME.");
        }

        if (!path.isFile()) {
            throw new GridException("Provided file path is not a file: " + path);
        }

        // Add no-op logger to remove no-appender warning.
        Appender app = new NullAppender();

        Logger.getRootLogger().addAppender(app);

        ApplicationContext springCtx;

        try {
            springCtx = new FileSystemXmlApplicationContext(path.toURI().toURL().toString());
        }
        catch (BeansException | MalformedURLException e) {
            throw new GridException("Failed to instantiate Spring XML application context: " + e.getMessage(), e);
        }

        Map cfgMap;

        try {
            // Note: Spring is not generics-friendly.
            cfgMap = springCtx.getBeansOfType(GridConfiguration.class);
        }
        catch (BeansException e) {
            throw new GridException("Failed to instantiate bean [type=" + GridConfiguration.class + ", err=" +
                e.getMessage() + ']', e);
        }

        if (cfgMap == null) {
            throw new GridException("Failed to find a single grid factory configuration in: " + path);
        }

        // Remove previously added no-op logger.
        Logger.getRootLogger().removeAppender(app);

        if (cfgMap.isEmpty()) {
            throw new GridException("Can't find grid factory configuration in: " + path);
        }

        Collection<GridConfiguration> res = new ArrayList<>();

        for (GridConfiguration cfg : (Collection<GridConfiguration>)cfgMap.values()) {
            UUID nodeId = UUID.randomUUID();

            cfg.setNodeId(nodeId);

            cfg.setGridLogger(initLogger(log));

            res.add(cfg);
        }

        return res;
    }

    /** */
    public static class TestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            Collection<GridComputeJob> jobs = new ArrayList<>(JOB_COUNT);

            for (int i = 0; i < JOB_COUNT; i++) {
                jobs.add(new GridComputeJobAdapter(i) {
                    @GridLoggerResource private GridLogger log;

                    @Override public Serializable execute() throws GridException {
                        if (log.isInfoEnabled()) {
                            log.info("Executing job [index=" + argument(0) + ']');
                        }

                        return argument(0);
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results != null : "Unexpected result [results=" + results + ']';
            assert results.size() == JOB_COUNT : "Unexpected result [results=" + results + ']';

            return null;
        }
    }

    /** */
    public static class TestSessionTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridTaskSessionResource private GridComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            Collection<GridComputeJob> jobs = new ArrayList<>(JOB_COUNT);

            for (int i = 0; i < JOB_COUNT; i++) {
                jobs.add(new GridComputeJobAdapter(i) {
                    @GridLoggerResource private GridLogger log;

                    @Override public Serializable execute() throws GridException {
                        if (log.isInfoEnabled()) {
                            log.info("Executing job [index=" + argument(0) + ']');
                        }

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
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult result, List<GridComputeJobResult> received) throws GridException {
            ses.setAttribute("attr7", 7);
            ses.setAttribute("attr8", 8);

            return super.result(result, received);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results != null : "Unexpected result [results=" + results + ']';
            assert results.size() == JOB_COUNT : "Unexpected result [results=" + results + ']';

            return null;
        }
    }
}
