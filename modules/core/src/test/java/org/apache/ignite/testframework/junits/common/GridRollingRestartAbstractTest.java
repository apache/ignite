package org.apache.ignite.testframework.junits.common;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.assertions.AlwaysAssertion;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Base class for tests which use a {@link RollingRestartThread} to stop and start
 * remote grid JVMs for failover testing.
 */
public abstract class GridRollingRestartAbstractTest extends GridCommonAbstractTest {

    /** Thread that shuts down and restarts Grid nodes for this test. */
    protected RollingRestartThread rollingRestartThread;

    /** Default predicate used to determine if a Grid node should be restarted. */
    protected final IgnitePredicate<Ignite> defaultRestartCheck = new IgnitePredicate<Ignite>() {
        @Override
        public boolean apply(Ignite ignite) {
            return serverCount() <= ignite.cluster().forServers().nodes().size();
        }
    };

    /** @return the predicate used to determine if a Grid node should be restarted. */
    public IgnitePredicate<Ignite> getRestartCheck() {
        return defaultRestartCheck;
    }

    /**
     * Return the {@link Assertion} used to assert some condition before a node is
     * stopped and started. If the assertion fails, the test will fail with that
     * assertion.
     *
     * @return assertion that will be tested before a node is restarted
     */
    public Assertion getRestartAssertion() {
        return AlwaysAssertion.INSTANCE;
    }

    /** @return the maximum number of times to perform a restart before exiting (&lt;= 0 implies no limit). */
    public int getMaxRestarts() {
        return 3;
    }

    /** @return the amount of time in milliseconds to wait between node restarts. */
    public int getRestartInterval() {
        return 5000;
    }

    /** @return the number of server nodes to start. */
    public abstract int serverCount();

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        if (isFirstGrid(gridName)) {
            cfg.setClientMode(true);
            assert cfg.getDiscoverySpi() instanceof TcpDiscoverySpi;
            ((TcpDiscoverySpi) cfg.getDiscoverySpi()).setForceServerMode(true);
        }
        cfg.setCacheConfiguration(getCacheConfiguration());
        return cfg;
    }

    /**
     * @return the cache configuration for the test cache
     */
    protected abstract CacheConfiguration<?, ?> getCacheConfiguration();

    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // the +1 includes this JVM (the client)
        startGrids(serverCount() + 1);
        this.rollingRestartThread = new RollingRestartThread();
        this.rollingRestartThread.start();
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
        this.rollingRestartThread.shutdown();
    }


    /** Thread that performs a "rolling restart" of a set of Ignite grid processes. */
    protected class RollingRestartThread extends Thread {

        /** Running flag. */
        private volatile boolean isRunning;

        /** The total number of restarts performed by this thread. */
        private volatile int restartTotal;

        /** Index of Ignite grid that was most recently restarted. */
        private int currentRestartGridId;

        /**
         * Create a new {@link RollingRestartThread} that will stop and start Ignite Grid
         * processes managed by the given test. The thread will check the given
         * {@link #getRestartCheck()} predicate every {@link #getRestartInterval()} milliseconds and
         * when it returns true, will start and then stop a Java process
         * via the test class.
         */
        public RollingRestartThread() {
            if (getRestartInterval() < 0) {
                throw new IllegalArgumentException("invalid restart interval: " + getRestartInterval());
            }
            setDaemon(true);
            setName(RollingRestartThread.class.getSimpleName());
        }

        /**
         * @return the total number of process restarts performed by this thread.
         */
        public int getRestartTotal() {
            return restartTotal;
        }

        /**
         * Stop the rolling restart thread and wait for it to fully exit.
         *
         * @throws InterruptedException if the calling thread was interrupted while waiting for the rolling restart thread
         *                              to exit.
         */
        public synchronized void shutdown() throws InterruptedException {
            isRunning = false;
            interrupt();
            join();
        }

        @Override public synchronized void start() {
            isRunning = true;
            super.start();
        }

        @Override public void run() {
            Ignite ignite = grid(0);
            ignite.log().info(getName() + ": started.");

            IgnitePredicate<Ignite> restartCheck = getRestartCheck();
            Assertion restartAssertion = getRestartAssertion();

            while (this.isRunning) {
                try {
                    if (getRestartInterval() > 0) {
                        Thread.sleep(getRestartInterval());
                    }
                    else {
                        Thread.yield();
                    }

                    if (restartCheck.apply(ignite)) {
                        restartAssertion.test();
                        int restartGrid = nextGridToRestart();
                        stopGrid(restartGrid);
                        ignite.log().info(getName() + ": stopped a process.");
                        startGrid(restartGrid);
                        ignite.log().info(getName() + ": started a process.");
                        int restartTotal = ++this.restartTotal;
                        if (getMaxRestarts() > 0 && restartTotal >= getMaxRestarts()) {
                            isRunning = false;
                        }
                    }
                }
                catch (RuntimeException e) {
                    if (isRunning) {
                        StringWriter sw = new StringWriter();
                        e.printStackTrace(new PrintWriter(sw));
                        ignite.log().info(getName() + ": caught exception: " + sw.toString());
                    }
                    else {
                        ignite.log().info(getName() + ": caught exception while exiting: " + e);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (isRunning) {
                        StringWriter sw = new StringWriter();
                        e.printStackTrace(new PrintWriter(sw));
                        ignite.log().info(getName() + ": was interrupted: " + sw.toString());
                    }
                    else {
                        ignite.log().info(getName() + ": was interrupted while exiting: " + e);
                    }
                    isRunning = false;
                }
                catch (AssertionError e) {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    ignite.log().info(getName() + ": assertion failed: " + sw.toString());
                    isRunning = false;
                }
            }
            ignite.log().info(getName() + ": exited.");
        }

        /**
         * Return the index of the next Grid to restart.
         *
         * @return index of the next grid to start
         * @see #currentRestartGridId
         * @see GridRollingRestartAbstractTest#grid(int)
         */
        protected int nextGridToRestart() {
            if (this.currentRestartGridId == serverCount()) {
                this.currentRestartGridId = 0;
            }
            // skip grid 0 because this is the "client" - the JVM that
            // is executing the test
            return ++this.currentRestartGridId;
        }

        /**
         * Start the Grid at the given index.
         *
         * @param index index of Grid to start
         * @see GridRollingRestartAbstractTest#grid(int)
         */
        protected void startGrid(int index) {
            try {
                GridRollingRestartAbstractTest.this.startGrid(index);
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Stop the process for the Grid at the given index.
         *
         * @param index index of Grid to stop
         * @see GridRollingRestartAbstractTest#grid(int)
         */
        protected void stopGrid(int index) {
            Ignite remote = grid(index);
            assert remote != null;
            assert remote instanceof IgniteProcessProxy;

            IgniteProcessProxy process = (IgniteProcessProxy) remote;
            int pid = process.getProcess().getPid();
            try {
                grid(0).log().info(String.format("Killing grid id %d with PID %d", index, pid));
                IgniteProcessProxy.kill(process.name());
                grid(0).log().info(String.format("Grid id %d with PID %d stopped", index, pid));
            }
            catch (RuntimeException e) {
                throw e;
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
