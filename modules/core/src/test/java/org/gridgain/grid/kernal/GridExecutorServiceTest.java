/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.kernal.executor.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Grid distributed executor test.
 */
@GridCommonTest(group = "Thread Tests")
public class GridExecutorServiceTest extends GridCommonAbstractTest {
    /** */
    public GridExecutorServiceTest() {
        super(true);
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    public void testExecute() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        srvc.execute(new Runnable() {
            @GridInstanceResource
            private Ignite ignite;

            @Override public void run() {
                System.out.println("Test message.");

                assert this.ignite != null;
            }
        });

        srvc.execute(new TestRunnable());

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    public void testSubmit() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Future<?> fut = srvc.submit(new TestRunnable());

        Object res = fut.get();

        info("Default Runnable result:" + res);

        assert res == null : "Failed to get valid default result for submitted Runnable: " + res;

        String val = "test-value";

        fut = srvc.submit(new TestRunnable(), val);

        res = fut.get();

        info("Defined Runnable result:" + res);

        assert val.equals(res) : "Failed to get valid predefined result for submitted Runnable: " + res;

        fut = srvc.submit(new TestCallable<>(val));

        res = fut.get();

        info("Callable result:" + res);

        assert val.equals(res) : "Failed to get valid result for submitted Callable: " + res;

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    public void testSubmitWithFutureTimeout() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Future<Integer> fut = srvc.submit(new TestCallable<>(3000)); // Just sleep for 3 seconds.

        boolean ok = true;

        try {
            fut.get(1, TimeUnit.SECONDS);

            ok = false;
        }
        catch (TimeoutException e) {
            info("Task timeout elapsed: " + e.getMessage());
        }

        assert ok : "Timeout must be thrown.";

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    @SuppressWarnings("TooBroadScope")
    public void testInvokeAll() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Collection<Callable<String>> cmds = new ArrayList<>(2);

        String val1 = "test-value-1";
        String val2 = "test-value-2";

        cmds.add(new TestCallable<>(val1));
        cmds.add(new TestCallable<>(val2));

        List<Future<String>> futs = srvc.invokeAll(cmds);

        assert futs != null;
        assert futs.size() == 2;

        String res1 = futs.get(0).get();
        String res2 = futs.get(1).get();

        assert val1.equals(res1) : "Failed to get valid result for first command: " + res1;
        assert val2.equals(res2) : "Failed to get valid result for second command: " + res2;

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    @SuppressWarnings("TooBroadScope")
    public void testInvokeAllWithTimeout() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Collection<Callable<Integer>> cmds = new ArrayList<>();

        cmds.add(new TestCallable<>(3000)); // Just sleeps for 3 seconds.
        cmds.add(new TestCallable<>(3000)); // Just sleeps for 3 seconds.

        List<Future<Integer>> fut = srvc.invokeAll(cmds, 1, TimeUnit.SECONDS);

        assert fut != null;
        assert fut.size() == 2;

        boolean ok = true;

        try {
            fut.get(0).get();

            ok = false;
        }
        catch (CancellationException e) {
            info("First timeout task is cancelled: " + e.getMessage());
        }

        assert ok : "First task must be cancelled.";

        try {
            fut.get(1).get();

            ok = false;
        }
        catch (CancellationException e) {
            info("Second timeout task is cancelled: " + e.getMessage());
        }

        assert ok : "Second task must be cancelled.";

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    @SuppressWarnings("TooBroadScope")
    public void testInvokeAny() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Collection<Callable<String>> cmds = new ArrayList<>(2);

        String val1 = "test-value-1";
        String val2 = "test-value-2";

        cmds.add(new TestCallable<>(val1));
        cmds.add(new TestCallable<>(val2));

        String res = srvc.invokeAny(cmds);

        info("Result: " + res);

        assert val1.equals(res) : "Failed to get valid result: " + res;

        srvc.shutdown();
    }

    /**
     * @throws Exception Thrown in case of test failure.
     */
    @SuppressWarnings("TooBroadScope")
    public void testInvokeAnyWithTimeout() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ExecutorService srvc = createExecutorService(ignite);

        Collection<Callable<Integer>> timeoutCmds = new ArrayList<>(2);

        timeoutCmds.add(new TestCallable<>(5000));
        timeoutCmds.add(new TestCallable<>(5000));

        boolean ok = true;

        try {
            srvc.invokeAny(timeoutCmds, 1, TimeUnit.SECONDS);

            ok = false;
        }
        catch (TimeoutException e) {
            info("Task timeout elapsed: " + e.getMessage());
        }

        assert ok : "Timeout must be thrown.";

        srvc.shutdown();
    }

    /**
     * @param ignite Grid instance.
     * @return Thrown in case of test failure.
     */
    private ExecutorService createExecutorService(Ignite ignite) {
        assert ignite != null;

        return new GridExecutorService((ClusterGroupAdapter) ignite, log());
    }

    /**
     * @param <T> Type of the {@link Callable} argument.
     */
    private static class TestCallable<T> implements Callable<T>, Serializable {
        /** */
        private T data;

        /** */
        @GridInstanceResource
        private Ignite ignite;

        /**
         * @param data Data.
         */
        TestCallable(T data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public T call() throws Exception {
            System.out.println("Test callable message.");

            assert ignite != null;

            if (data instanceof Integer)
                Thread.sleep((Integer)data);

            return data;
        }
    }

    /** */
    private static class TestRunnable implements Runnable, Serializable {
        /** */
        @GridInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println("Test Runnable message.");

            assert ignite != null;
        }
    }
}
