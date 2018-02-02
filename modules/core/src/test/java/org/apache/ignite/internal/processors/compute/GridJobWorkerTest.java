package org.apache.ignite.internal.processors.compute;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.job.GridJobWorker;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests grid job worker.
 *
 * https://issues.apache.org/jira/browse/IGNITE-7309
 */
public class GridJobWorkerTest extends GridCommonAbstractTest {
    /**
     * Custom class (used as a compute job result type).
     */
    public static class CustomInteger {
        /** Value. */
        private final int val;

        /**
         * @param val Value.
         */
        CustomInteger(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int getValue() {
            return val;
        }
    }

    /**
     * Custom marshaller implementation.
     */
    public static class CustomMarshaller extends BinaryMarshaller {
        /** Latch for stopping server node once client job run is started. */
        private final CountDownLatch allowServerToStopLatch;

        /** Latch for awaiting server node state is 'stopping'. */
        private final CountDownLatch serverIsStoppingLatch;

        /** */
        CustomMarshaller() {
            super();

            this.allowServerToStopLatch = null;

            this.serverIsStoppingLatch = null;
        }

        /**
         * @param allowServerToStopLatch Latch for stopping server node once client job run is started.
         * @param serverIsStoppingLatch Latch for awaiting server node state is 'stopping'.
         */
        CustomMarshaller(CountDownLatch allowServerToStopLatch, CountDownLatch serverIsStoppingLatch) {
            super();

            this.allowServerToStopLatch = allowServerToStopLatch;

            this.serverIsStoppingLatch = serverIsStoppingLatch;
        }

        /**
         * This method wraps marshal invocations to initiate node stopping and throw an error in process of CustomInteger handling.
         *
         * @param obj Object to marshal.
         * @return Byte array.
         * @throws IgniteCheckedException If marshalling failed.
         */
        @Override public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
            if ((allowServerToStopLatch != null) && (serverIsStoppingLatch != null) && (obj != null)) {
                String clsName = obj.getClass().getName();

                if (clsName.endsWith("$CustomInteger")) {
                    // Await for node stopping.
                    allowServerToStopLatch.countDown();

                    try {
                        serverIsStoppingLatch.await();
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // Simulate marshal exception.
                    throw new IgniteCheckedException(String.format("Marshalling %s object fails.", clsName));
                }
            }

            return super.marshal(obj);
        }
    }

    /**
     * Create server logger.
     *
     * @param errors Logged errors collection.
     */
    private IgniteLogger createServerLogger(final Collection<Throwable> errors) {
        return new JavaLogger() {
            @Override public boolean isDebugEnabled() {
                return true;
            }

            @Override public void error(String msg, @Nullable Throwable e) {
                super.error(msg, e);

                errors.add(e);
            }

            @Override public IgniteLogger getLogger(Object ctgr) {
                return this;
            }
        };
    }

    /**
     * Run server node.
     *
     * @param errors Logged errors collection.
     * @param allowServerToStopLatch Latch for stopping server node once client job run is started.
     * @param serverIsStoppingLatch Latch for awaiting server node state is 'stopping'.
     * @param nodesAreStoppedLatch Latch for checking that all nodes are stopped.
     */
    private void runServerNode(final Collection<Throwable> errors, final CountDownLatch allowServerToStopLatch,
        final CountDownLatch serverIsStoppingLatch, final CountDownLatch nodesAreStoppedLatch) {

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    IgniteConfiguration conf = getConfiguration("server-node");

                    conf.setGridLogger(createServerLogger(errors));

                    conf.setMarshaller(new CustomMarshaller(allowServerToStopLatch, serverIsStoppingLatch));

                    conf.setLifecycleBeans(new LifecycleBean() {
                        @Override public void onLifecycleEvent(LifecycleEventType evt) throws IgniteException {
                            if (evt.equals(LifecycleEventType.BEFORE_NODE_STOP))
                                serverIsStoppingLatch.countDown();
                        }
                    });

                    try (Ignite ignite = Ignition.start(conf)) {
                        log().info("Server node started: " + ignite.name());

                        allowServerToStopLatch.await();
                    }
                    catch (Throwable e) {
                        e.printStackTrace();

                        fail("Run server node fail.");
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();

                    fail("Prepare server node configuration fail.");
                }
                finally {
                    nodesAreStoppedLatch.countDown();
                }
            }
        }).start();
    }

    /**
     * Run client node.
     *
     * @param nodesAreStoppedLatch Latch for checking that all nodes are stopped.
     */
    private void runClientNode(final CountDownLatch nodesAreStoppedLatch) {
        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    IgniteConfiguration conf = getConfiguration("client-node");

                    conf.setClientMode(true);

                    conf.setMarshaller(new CustomMarshaller());

                    try (Ignite ignite = Ignition.start(conf)) {
                        log().info("Client node started: " + ignite.name());

                        IgniteCompute compute = ignite.compute();

                        compute.call(new IgniteCallable<CustomInteger>() {
                            @Override public CustomInteger call() {
                                return new CustomInteger(0);
                            }
                        });
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();
                }
                finally {
                    nodesAreStoppedLatch.countDown();
                }
            }
        }).start();
    }

    /**
     * The test verifies that server node throws NodeStoppingException if marshalling fails in the process of node stopping.
     */
    public void testNodeStoppingException() {
        // Sync latches of node states.
        CountDownLatch allowServerToStopLatch = new CountDownLatch(1);
        CountDownLatch serverIsStoppingLatch = new CountDownLatch(1);
        CountDownLatch nodesAreStoppedLatch = new CountDownLatch(2);

        Collection<Throwable> serverErrors = new ConcurrentLinkedQueue<>();

        try {
            GridJobWorker.useStaticLog = false;

            runServerNode(serverErrors, allowServerToStopLatch, serverIsStoppingLatch, nodesAreStoppedLatch);

            runClientNode(nodesAreStoppedLatch);

            assertTrue("Server or client node was not stopped.", nodesAreStoppedLatch.await(30000, TimeUnit.MILLISECONDS));

            // Check for NodeStoppingException
            boolean found = false;

            for (Throwable e : serverErrors) {
                if (X.hasCause(e, NodeStoppingException.class)) {
                    found = true;

                    break;
                }
            }

            assertTrue("Server exception should contain NodeStoppingException.", found);
        }
        catch (InterruptedException e) {
            e.printStackTrace();

            fail("Server or client node was interrupted.");
        }
        finally {
            GridJobWorker.useStaticLog = true;
        }
    }
}
