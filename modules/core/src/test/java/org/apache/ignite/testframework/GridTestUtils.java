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

package org.apache.ignite.testframework;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.file.attribute.PosixFilePermission;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.GridBusyLock;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for tests.
 */
@SuppressWarnings({"UnusedCatchParameter"})
public final class GridTestUtils {
    /** Default busy wait sleep interval in milliseconds.  */
    public static final long DFLT_BUSYWAIT_SLEEP_INTERVAL = 200;

    /** */
    private static final Map<Class<?>, String> addrs = new HashMap<>();

    /** */
    private static final Map<Class<? extends Test>, Integer> mcastPorts = new HashMap<>();

    /** */
    private static final Map<Class<? extends Test>, Integer> discoPorts = new HashMap<>();

    /** */
    private static final Map<Class<? extends Test>, Integer> commPorts = new HashMap<>();

    /** */
    private static int[] addr;

    /** */
    private static final int default_mcast_port = 50000;

    /** */
    private static final int max_mcast_port = 54999;

    /** */
    private static final int default_comm_port = 45000;

    /** */
    private static final int max_comm_port = 49999;

    /** */
    private static final int default_disco_port = 55000;

    /** */
    private static final int max_disco_port = 59999;

    /** */
    private static int mcastPort = default_mcast_port;

    /** */
    private static int discoPort = default_disco_port;

    /** */
    private static int commPort = default_comm_port;

    /** */
    private static final GridBusyLock busyLock = new GridBusyLock();

    /**
     * Ensure singleton.
     */
    private GridTestUtils() {
        // No-op.
    }

    /**
     * Checks whether callable throws expected exception or not.
     *
     * @param log Logger (optional).
     * @param call Callable.
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @return Thrown throwable.
     */
    public static Throwable assertThrows(@Nullable IgniteLogger log, Callable<?> call,
        Class<? extends Throwable> cls, @Nullable String msg) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (cls != e.getClass()) {
                if (e.getClass() == CacheException.class && e.getCause() != null && e.getCause().getClass() == cls)
                    e = e.getCause();
                else {
                    U.error(log, "Unexpected exception.", e);

                    fail("Exception class is not as expected [expected=" + cls + ", actual=" + e.getClass() + ']', e);
                }
            }

            if (msg != null && (e.getMessage() == null || !e.getMessage().contains(msg))) {
                U.error(log, "Unexpected exception message.", e);

                fail("Exception message is not as expected [expected=" + msg + ", actual=" + e.getMessage() + ']', e);
            }

            if (log != null) {
                if (log.isInfoEnabled())
                    log.info("Caught expected exception: " + e.getMessage());
            }
            else
                X.println("Caught expected exception: " + e.getMessage());

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether callable throws expected exception or its child or not.
     *
     * @param log Logger (optional).
     * @param call Callable.
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsInherited(@Nullable IgniteLogger log, Callable<?> call,
        Class<? extends Throwable> cls, @Nullable String msg) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (!cls.isAssignableFrom(e.getClass()))
                fail("Exception class is not as expected [expected=" + cls + ", actual=" + e.getClass() + ']', e);

            if (msg != null && (e.getMessage() == null || !e.getMessage().startsWith(msg)))
                fail("Exception message is not as expected [expected=" + msg + ", actual=" + e.getMessage() + ']', e);

            if (log != null) {
                if (log.isDebugEnabled())
                    log.debug("Caught expected exception: " + e.getMessage());
            }
            else
                X.println("Caught expected exception: " + e.getMessage());

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether callable throws exception, which is itself of a specified
     * class, or has a cause of the specified class.
     *
     * @param call Callable.
     * @param cls Expected class.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsWithCause(Callable<?> call, Class<? extends Throwable> cls) {
        assert call != null;
        assert cls != null;

        try {
            call.call();
        }
        catch (Throwable e) {
            if (!X.hasCause(e, cls))
                fail("Exception is neither of a specified class, nor has a cause of the specified class: " + cls, e);

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Throw assertion error with specified error message and initialized cause.
     *
     * @param msg Error message.
     * @param cause Error cause.
     * @return Assertion error.
     */
    private static AssertionError fail(String msg, @Nullable Throwable cause) {
        AssertionError e = new AssertionError(msg);

        if (cause != null)
            e.initCause(cause);

        throw e;
    }

    /**
     * Checks whether object's method call throws expected exception or not.
     *
     * @param log Logger (optional).
     * @param cls Exception class.
     * @param msg Exception message (optional). If provided exception message
     *      and this message should be equal.
     * @param obj Object to invoke method for.
     * @param mtd Object's method to invoke.
     * @param params Method parameters.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrows(@Nullable IgniteLogger log, Class<? extends Throwable> cls,
        @Nullable String msg, final Object obj, final String mtd, final Object... params) {
        return assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                return invoke(obj, mtd, params);
            }
        }, cls, msg);
    }

    /**
     * Asserts that each element in iterable has one-to-one correspondence with a
     * predicate from list.
     *
     * @param it Input iterable of elements.
     * @param ps Array of predicates (by number of elements in iterable).
     */
    @SuppressWarnings("ConstantConditions")
    public static <T> void assertOneToOne(Iterable<T> it, IgnitePredicate<T>... ps) {
        Collection<IgnitePredicate<T>> ps0 = new ArrayList<>(Arrays.asList(ps));
        Collection<T2<IgnitePredicate<T>, T>> passed = new ArrayList<>();

        for (T elem : it) {
            for (T2<IgnitePredicate<T>, T> p : passed) {
                if (p.get1().apply(elem))
                    throw new AssertionError("Two elements match one predicate [elem1=" + p.get2() +
                        ", elem2=" + elem + ", pred=" + p.get1() + ']');
            }

            IgnitePredicate<T> matched = null;

            for (IgnitePredicate<T> p : ps0) {
                if (p.apply(elem)) {
                    if (matched != null)
                        throw new AssertionError("Element matches more than one predicate [elem=" + elem +
                            ", pred1=" + p + ", pred2=" + matched + ']');

                    matched = p;
                }
            }

            if (matched == null) // None matched.
                throw new AssertionError("The element does not match [elem=" + elem +
                    ", numRemainingPreds=" + ps0.size() + ']');

            ps0.remove(matched);
            passed.add(new T2<>(matched, elem));
        }
    }

    /**
     * Every invocation of this method will never return a
     * repeating multicast port for a different test case.
     *
     * @param cls Class.
     * @return Next multicast port.
     */
    public static synchronized int getNextMulticastPort(Class<? extends Test> cls) {
        Integer portRet = mcastPorts.get(cls);

        if (portRet != null)
            return portRet;

        int startPort = mcastPort;

        while (true) {
            if (mcastPort >= max_mcast_port)
                mcastPort = default_mcast_port;
            else
                mcastPort++;

            if (startPort == mcastPort)
                break;

            portRet = mcastPort;

            MulticastSocket sock = null;

            try {
                sock = new MulticastSocket(portRet);

                break;
            }
            catch (IOException ignored) {
                // No-op.
            }
            finally {
                U.closeQuiet(sock);
            }
        }

        // Cache port to be reused by the same test.
        mcastPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating communication port for a different test case.
     *
     * @param cls Class.
     * @return Next communication port.
     */
    public static synchronized int getNextCommPort(Class<? extends Test> cls) {
        Integer portRet = commPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (commPort >= max_comm_port)
            commPort = default_comm_port;
        else
            // Reserve 10 ports per test.
            commPort += 10;

        portRet = commPort;

        // Cache port to be reused by the same test.
        commPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating discovery port for a different test case.
     *
     * @param cls Class.
     * @return Next discovery port.
     */
    public static synchronized int getNextDiscoPort(Class<? extends Test> cls) {
        Integer portRet = discoPorts.get(cls);

        if (portRet != null)
            return portRet;

        if (discoPort >= max_disco_port)
            discoPort = default_disco_port;
        else
            discoPort += 10;

        portRet = discoPort;

        // Cache port to be reused by the same test.
        discoPorts.put(cls, portRet);

        return portRet;
    }

    /**
     * Every invocation of this method will never return a
     * repeating multicast group for a different test case.
     *
     * @param cls Class.
     * @return Next multicast group.
     */
    public static synchronized String getNextMulticastGroup(Class<?> cls) {
        String addrStr = addrs.get(cls);

        if (addrStr != null)
            return addrStr;

        // Increment address.
        if (addr[3] == 255) {
            if (addr[2] == 255)
                assert false;
            else {
                addr[2] += 1;

                addr[3] = 1;
            }
        }
        else
            addr[3] += 1;

        // Convert address to string.
        StringBuilder b = new StringBuilder(15);

        for (int i = 0; i < addr.length; i++) {
            b.append(addr[i]);

            if (i < addr.length - 1)
                b.append('.');
        }

        addrStr = b.toString();

        // Cache address to be reused by the same test.
        addrs.put(cls, addrStr);

        return addrStr;
    }

    /**
     * Runs runnable object in specified number of threads.
     *
     * @param run Target runnable.
     * @param threadNum Number of threads.
     * @param threadName Thread name.
     * @return Execution time in milliseconds.
     * @throws Exception Thrown if at least one runnable execution failed.
     */
    public static long runMultiThreaded(Runnable run, int threadNum, String threadName) throws Exception {
        return runMultiThreaded(makeCallable(run, null), threadNum, threadName);
    }

    /**
     * Runs runnable object in specified number of threads.
     *
     * @param run Target runnable.
     * @param threadNum Number of threads.
     * @param threadName Thread name.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    public static IgniteInternalFuture<Long> runMultiThreadedAsync(Runnable run, int threadNum, String threadName) {
        return runMultiThreadedAsync(makeCallable(run, null), threadNum, threadName);
    }

    /**
     * Runs callable object in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Callable<?> call, int threadNum, String threadName) throws Exception {
        List<Callable<?>> calls = Collections.<Callable<?>>nCopies(threadNum, call);

        return runMultiThreaded(calls, threadName);
    }

    /**
     * Runs callable object in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    public static IgniteInternalFuture<Long> runMultiThreadedAsync(Callable<?> call, int threadNum, final String threadName) {
        final List<Callable<?>> calls = Collections.<Callable<?>>nCopies(threadNum, call);
        final GridTestSafeThreadFactory threadFactory = new GridTestSafeThreadFactory(threadName);

        // Future that supports cancel() operation.
        GridFutureAdapter<Long> cancelFut = new GridFutureAdapter<Long>() {
            @Override public boolean cancel() {
                if (onCancelled()) {
                    threadFactory.interruptAllThreads();

                    onCancelled();

                    return true;
                }

                return false;
            }
        };

        // Async execution future (doesn't support cancel()).
        IgniteInternalFuture<Long> runFut = runAsync(new Callable<Long>() {
            @Override public Long call() throws Exception {
                return runMultiThreaded(calls, threadFactory);
            }
        });

        // Compound future, that adds cancel() support to execution future.
        GridCompoundFuture<Long, Long> compFut = new GridCompoundFuture<>();

        compFut.addAll(cancelFut, runFut);
        compFut.reducer(F.sumLongReducer());
        compFut.markInitialized();

        cancelFut.onDone();

        return compFut;
    }

    /**
     * Runs callable tasks each in separate threads.
     *
     * @param calls Callable tasks.
     * @param threadName Thread name.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Iterable<Callable<?>> calls, String threadName) throws Exception {
        return runMultiThreaded(calls, new GridTestSafeThreadFactory(threadName));
    }

    /**
     * Runs callable tasks each in separate threads.
     *
     * @param calls Callable tasks.
     * @param threadFactory Thread factory.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Iterable<Callable<?>> calls, GridTestSafeThreadFactory threadFactory)
        throws Exception {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to start new threads (test is being stopped).");

        Collection<Thread> threads = new ArrayList<>();
        long time;

        try {
            for (Callable<?> call : calls)
                threads.add(threadFactory.newThread(call));

            time = System.currentTimeMillis();

            for (Thread t : threads)
                t.start();
        }
        finally {
            busyLock.leaveBusy();
        }

        // Wait threads finish their job.
        for (Thread t : threads)
            t.join();

        time = System.currentTimeMillis() - time;

        // Validate errors happens
        threadFactory.checkError();

        return time;
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @return Future with task result.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    public static <T> IgniteInternalFuture<T> runAsync(final Callable<T> task) {
        if (!busyLock.enterBusy())
            throw new IllegalStateException("Failed to start new threads (test is being stopped).");

        try {
            final GridTestSafeThreadFactory thrFactory = new GridTestSafeThreadFactory("async-runner");

            final GridFutureAdapter<T> fut = new GridFutureAdapter<T>() {
                @Override public boolean cancel() throws IgniteCheckedException {
                    super.cancel();

                    thrFactory.interruptAllThreads();

                    onCancelled();

                    return true;
                }
            };

            thrFactory.newThread(new Runnable() {
                @Override public void run() {
                    try {
                        // Execute task.
                        T res = task.call();

                        fut.onDone(res);
                    }
                    catch (Throwable e) {
                        fut.onDone(e);
                    }
                }
            }).start();

            return fut;
        }
        finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Interrupts and waits for termination of all the threads started
     * so far by current test.
     *
     * @param log Logger.
     */
    public static void stopThreads(IgniteLogger log) {
        busyLock.block();

        try {
            GridTestSafeThreadFactory.stopAllThreads(log);
        }
        finally {
            busyLock.unblock();
        }
    }

    /**
     * @return Ignite home.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ProhibitedExceptionThrown"})
    public static String getIgniteHome() throws Exception {
        String ggHome = System.getProperty("IGNITE_HOME");

        if (ggHome == null)
            ggHome = System.getenv("IGNITE_HOME");

        if (ggHome == null)
            throw new Exception("IGNITE_HOME parameter must be set either as system or environment variable.");

        File dir = new File(ggHome);

        if (!dir.exists())
            throw new Exception("Ignite home does not exist [ignite-home=" + dir.getAbsolutePath() + ']');

        if (!dir.isDirectory())
            throw new Exception("Ignite home is not a directory [ignite-home=" + dir.getAbsolutePath() + ']');

        return ggHome;
    }

    /**
     * @param <T> Type.
     * @param cls Class.
     * @param annCls Annotation class.
     * @return Annotation.
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        for (Class<?> cls0 = cls; cls0 != null; cls0 = cls0.getSuperclass()) {
            T ann = cls0.getAnnotation(annCls);

            if (ann != null)
                return ann;
        }

        return null;
    }

    /**
     * Initializes address.
     */
    static {
        InetAddress locHost = null;

        try {
            locHost = U.getLocalHost();
        }
        catch (IOException e) {
            assert false : "Unable to get local address. This leads to the same multicast addresses " +
                "in the local network.";
        }

        if (locHost != null) {
            int thirdByte = locHost.getAddress()[3];

            if (thirdByte < 0)
                thirdByte += 256;

            // To get different addresses for different machines.
            addr = new int[] {229, thirdByte, 1, 1};
        }
        else
            addr = new int[] {229, 1, 1, 1};
    }

    /**
     * @param path Path.
     * @param startFilter Start filter.
     * @param endFilter End filter.
     * @return List of JARs that corresponds to the filters.
     * @throws IOException If failed.
     */
    private static Collection<String> getFiles(String path, @Nullable final String startFilter,
        @Nullable final String endFilter) throws IOException {
        Collection<String> res = new ArrayList<>();

        File file = new File(path);

        assert file.isDirectory();

        File[] jars = file.listFiles(new FilenameFilter() {
            /**
             * @see FilenameFilter#accept(File, String)
             */
            @SuppressWarnings({"UnnecessaryJavaDocLink"})
            @Override public boolean accept(File dir, String name) {
                // Exclude spring.jar because it tries to load META-INF/spring-handlers.xml from
                // all available JARs and create instances of classes from there for example.
                // Exclude logging as it is used by spring and casted to Log interface.
                // Exclude log4j because of the design - 1 per VM.
                if (name.startsWith("spring") || name.startsWith("log4j") ||
                    name.startsWith("commons-logging") || name.startsWith("junit") ||
                    name.startsWith("ignite-tests"))
                    return false;

                boolean ret = true;

                if (startFilter != null)
                    ret = name.startsWith(startFilter);

                if (ret && endFilter != null)
                    ret = name.endsWith(endFilter);

                return ret;
            }
        });

        for (File jar : jars)
            res.add(jar.getCanonicalPath());

        return res;
    }

    /**
     * Silent stop grid.
     * Method doesn't throw any exception.
     *
     * @param ignite Grid to stop.
     * @param log Logger.
     */
    @SuppressWarnings({"CatchGenericClass"})
    public static void close(Ignite ignite, IgniteLogger log) {
        if (ignite != null)
            try {
                G.stop(ignite.name(), false);
            }
            catch (Throwable e) {
                U.error(log, "Failed to stop grid: " + ignite.name(), e);
            }
    }

    /**
     * Silent stop grid.
     * Method doesn't throw any exception.
     *
     * @param gridName Grid name.
     * @param log Logger.
     */
    @SuppressWarnings({"CatchGenericClass"})
    public static void stopGrid(String gridName, IgniteLogger log) {
        try {
            G.stop(gridName, false);
        }
        catch (Throwable e) {
            U.error(log, "Failed to stop grid: " + gridName, e);
        }
    }

    /**
     * Gets file representing the path passed in. First the check is made if path is absolute.
     * If not, then the check is made if path is relative to ${IGNITE_HOME}. If both checks fail,
     * then {@code null} is returned, otherwise file representing path is returned.
     * <p>
     * See {@link #getIgniteHome()} for information on how {@code IGNITE_HOME} is retrieved.
     *
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     * @see #getIgniteHome()
     */
    @Nullable public static File resolveIgnitePath(String path) {
        return resolvePath(null, path);
    }

    /**
     * @param igniteHome Optional ignite home path.
     * @param path Path to resolve.
     * @return Resolved path, or {@code null} if file cannot be resolved.
     */
    @Nullable private static File resolvePath(@Nullable String igniteHome, String path) {
        File file = new File(path).getAbsoluteFile();

        if (!file.exists()) {
            String home = igniteHome != null ? igniteHome : U.getIgniteHome();

            if (home == null)
                return null;

            file = new File(home, path);

            return file.exists() ? file : null;
        }

        return file;
    }

    /**
     * @param cache Cache.
     * @return Cache context.
     */
    public static <K, V> GridCacheContext<K, V> cacheContext(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache().context();
    }

    /**
     * @param cache Cache.
     * @return Near cache.
     */
    public static <K, V> GridNearCacheAdapter<K, V> near(IgniteCache<K, V> cache) {
        return cacheContext(cache).near();
    }

    /**
     * @param cache Cache.
     * @return DHT cache.
     */
    public static <K, V> GridDhtCacheAdapter<K, V> dht(IgniteCache<K, V> cache) {
        return near(cache).dht();
    }

    /**
     * @param cacheName Cache name.
     * @param backups Number of backups.
     * @param log Logger.
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public static <K, V> void waitTopologyUpdate(@Nullable String cacheName, int backups, IgniteLogger log)
        throws Exception {
        for (Ignite g : Ignition.allGrids()) {
            IgniteCache<K, V> cache = ((IgniteEx)g).cache(cacheName);

            GridDhtPartitionTopology top = dht(cache).topology();

            while (true) {
                boolean wait = false;

                for (int p = 0; p < g.affinity(cacheName).partitions(); p++) {
                    Collection<ClusterNode> nodes = top.nodes(p, AffinityTopologyVersion.NONE);

                    if (nodes.size() > backups + 1) {
                        LT.warn(log, null, "Partition map was not updated yet (will wait) [grid=" + g.name() +
                            ", p=" + p + ", nodes=" + F.nodeIds(nodes) + ']');

                        wait = true;

                        break;
                    }
                }

                if (wait)
                    Thread.sleep(20);
                else
                    break; // While.
            }
        }
    }

    /**
     * Convert runnable tasks with callable.
     *
     * @param run Runnable task to convert into callable one.
     * @param res Callable result.
     * @param <T> The result type of method <tt>call</tt>, always {@code null}.
     * @return Callable task around the specified runnable one.
     */
    public static <T> Callable<T> makeCallable(final Runnable run, @Nullable final T res) {
        return new Callable<T>() {
            @Override public T call() throws Exception {
                run.run();
                return res;
            }
        };
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param cls Class.
     * @param fieldName Field names to get value for.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public static <T> T getFieldValue(Object obj, Class cls, String fieldName) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            // Resolve inner field.
            Field field = cls.getDeclaredField(fieldName);

            synchronized (field) {
                // Backup accessible field state.
                boolean accessible = field.isAccessible();

                try {
                    if (!accessible)
                        field.setAccessible(true);

                    obj = field.get(obj);
                }
                finally {
                    // Recover accessible field state.
                    if (!accessible)
                        field.setAccessible(false);
                }
            }

            return (T)obj;
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldName=" + fieldName + ']', e);
        }
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteException In case of error.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public static <T> T getFieldValue(Object obj, String... fieldNames) throws IgniteException {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        try {
            for (String fieldName : fieldNames) {
                Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

                try {
                    // Resolve inner field.
                    Field field = cls.getDeclaredField(fieldName);

                    synchronized (field) {
                        // Backup accessible field state.
                        boolean accessible = field.isAccessible();

                        try {
                            if (!accessible)
                                field.setAccessible(true);

                            obj = field.get(obj);
                        }
                        finally {
                            // Recover accessible field state.
                            if (!accessible)
                                field.setAccessible(false);
                        }
                    }
                }
                catch (NoSuchFieldException e) {
                    // Resolve inner class, if not an inner field.
                    Class<?> innerCls = getInnerClass(cls, fieldName);

                    if (innerCls == null)
                        throw new IgniteException("Failed to get object field [obj=" + obj +
                            ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);

                    obj = innerCls;
                }
            }

            return (T)obj;
        }
        catch (IllegalAccessException e) {
            throw new IgniteException("Failed to get object field [obj=" + obj +
                ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
        }
    }

    /**
     * Get inner class by its name from the enclosing class.
     *
     * @param parentCls Parent class to resolve inner class for.
     * @param innerClsName Name of the inner class.
     * @return Inner class.
     */
    @Nullable public static <T> Class<T> getInnerClass(Class<?> parentCls, String innerClsName) {
        for (Class<?> cls : parentCls.getDeclaredClasses())
            if (innerClsName.equals(cls.getSimpleName()))
                return (Class<T>)cls;

        return null;
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            synchronized (field) {
                // Backup accessible field state.
                boolean accessible = field.isAccessible();

                try {
                    if (!accessible)
                        field.setAccessible(true);

                    field.set(obj, val);
                }
                finally {
                    // Recover accessible field state.
                    if (!accessible)
                        field.setAccessible(false);
                }
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param cls Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteException In case of error.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            synchronized (field) {
                // Backup accessible field state.
                boolean accessible = field.isAccessible();

                try {
                    if (!accessible)
                        field.setAccessible(true);

                    field.set(obj, val);
                }
                finally {
                    // Recover accessible field state.
                    if (!accessible)
                        field.setAccessible(false);
                }
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Invoke method on an object.
     *
     * @param obj Object to call method on.
     * @param mtd Method to invoke.
     * @param params Parameters of the method.
     * @return Method invocation result.
     * @throws Exception If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Nullable public static <T> T invoke(Object obj, String mtd, Object... params) throws Exception {
        // We cannot resolve method by parameter classes due to some of parameters can be null.
        // Search correct method among all methods collection.
        for (Method m : obj.getClass().getDeclaredMethods()) {
            // Filter methods by name.
            if (!m.getName().equals(mtd))
                continue;

            if (!areCompatible(params, m.getParameterTypes()))
                continue;

            try {
                synchronized (m) {
                    // Backup accessible field state.
                    boolean accessible = m.isAccessible();

                    try {
                        if (!accessible)
                            m.setAccessible(true);

                        return (T)m.invoke(obj, params);
                    }
                    finally {
                        // Recover accessible field state.
                        if (!accessible)
                            m.setAccessible(false);
                    }
                }
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access method" +
                    " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']', e);
            }
            catch (InvocationTargetException e) {
                Throwable cause = e.getCause();

                if (cause instanceof Error)
                    throw (Error) cause;

                if (cause instanceof Exception)
                    throw (Exception) cause;

                throw new RuntimeException("Failed to invoke method)" +
                    " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']', e);
            }
        }

        throw new RuntimeException("Failed to find method" +
            " [obj=" + obj + ", mtd=" + mtd + ", params=" + Arrays.toString(params) + ']');
    }

    /**
     * Check objects and corresponding types are compatible.
     *
     * @param objs Objects array.
     * @param types Classes array.
     * @return Objects in array can be casted to corresponding types.
     */
    private static boolean areCompatible(Object[] objs, Class[] types) {
        if (objs.length != types.length)
            return false;

        for (int i = 0, size = objs.length; i < size; i++) {
            Object o = objs[i];

            if (o != null && !types[i].isInstance(o))
                return false;
        }

        return true;
    }

    /**
     * Tries few times to perform some assertion. In the worst case
     * {@code assertion} closure will be executed {@code retries} + 1 times and
     * thread will spend approximately {@code retries} * {@code retryInterval} sleeping.
     *
     * @param log Log.
     * @param retries Number of retries.
     * @param retryInterval Interval between retries in milliseconds.
     * @param c Closure with assertion. All {@link AssertionError}s thrown
     *      from this closure will be ignored {@code retries} times.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public static void retryAssert(@Nullable IgniteLogger log, int retries, long retryInterval, GridAbsClosure c)
        throws IgniteInterruptedCheckedException {
        for (int i = 0; i < retries; i++) {
            try {
                c.apply();

                return;
            }
            catch (AssertionError e) {
                U.warn(log, "Check failed (will retry in " + retryInterval + "ms).", e);

                U.sleep(retryInterval);
            }
        }

        // Apply the last time without guarding try.
        c.apply();
    }

    /**
     * Reads entire file into byte array.
     *
     * @param file File to read.
     * @return Content of file in byte array.
     * @throws IOException If failed.
     */
    public static byte[] readFile(File file) throws IOException {
        assert file.exists();
        assert file.length() < Integer.MAX_VALUE;

        byte[] bytes = new byte[(int) file.length()];

        try (FileInputStream fis = new FileInputStream(file)) {
            int readBytesCnt = fis.read(bytes);
            assert readBytesCnt == bytes.length;
        }

        return bytes;
    }

    /**
     * Sleeps and increments an integer.
     * <p>
     * Allows for loops like the following:
     * <pre>{@code
     *     for (int i = 0; i < 20 && !condition; i = sleepAndIncrement(200, i)) {
     *         ...
     *     }
     * }</pre>
     * for busy-waiting limited number of iterations.
     *
     * @param sleepDur Sleep duration in milliseconds.
     * @param i Integer to increment.
     * @return Incremented value.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If sleep was interrupted.
     */
    public static int sleepAndIncrement(int sleepDur, int i) throws IgniteInterruptedCheckedException {
        U.sleep(sleepDur);

        return i + 1;
    }

    /**
     * Waits for condition, polling in busy wait loop.
     *
     * @param cond Condition to wait for.
     * @param timeout Max time to wait in milliseconds.
     * @return {@code true} if condition was achieved, {@code false} otherwise.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
     */
    public static boolean waitForCondition(GridAbsPredicate cond, long timeout) throws IgniteInterruptedCheckedException {
        long curTime = U.currentTimeMillis();
        long endTime = curTime + timeout;

        if (endTime < 0)
            endTime = Long.MAX_VALUE;

        while (curTime < endTime) {
            if (cond.apply())
                return true;

            U.sleep(DFLT_BUSYWAIT_SLEEP_INTERVAL);

            curTime = U.currentTimeMillis();
        }

        return false;
    }

    /**
     * Creates an SSL context from test key store with disabled trust manager.
     *
     * @return Initialized context.
     * @throws GeneralSecurityException In case if context could not be initialized.
     * @throws IOException If keystore cannot be accessed.
     */
    public static SSLContext sslContext() throws GeneralSecurityException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLS");

        char[] storePass = GridTestProperties.getProperty("ssl.keystore.password").toCharArray();

        KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance("SunX509");

        KeyStore keyStore = KeyStore.getInstance("JKS");

        keyStore.load(new FileInputStream(U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.path"))),
            storePass);

        keyMgrFactory.init(keyStore, storePass);

        ctx.init(keyMgrFactory.getKeyManagers(),
            new TrustManager[]{GridSslBasicContextFactory.getDisabledTrustManager()}, null);

        return ctx;
    }

    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static GridSslContextFactory sslContextFactory() {
        GridSslBasicContextFactory factory = new GridSslBasicContextFactory();

        factory.setKeyStoreFilePath(
            U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.path")).getAbsolutePath());
        factory.setKeyStorePassword(GridTestProperties.getProperty("ssl.keystore.password").toCharArray());

        factory.setTrustManagers(GridSslBasicContextFactory.getDisabledTrustManager());

        return factory;
    }


    /**
     * Creates test-purposed SSL context factory from test key store with disabled trust manager.
     *
     * @return SSL context factory used in test.
     */
    public static Factory<SSLContext> sslFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(
            U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.path")).getAbsolutePath());
        factory.setKeyStorePassword(GridTestProperties.getProperty("ssl.keystore.password").toCharArray());

        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param o1 Object 1.
     * @param o2 Object 2.
     * @return Equals or not.
     */
    public static boolean deepEquals(@Nullable Object o1, @Nullable Object o2) {
        if (o1 == o2)
            return true;
        else if (o1 == null || o2 == null)
            return false;
        else if (o1.getClass() != o2.getClass())
            return false;
        else {
            Class<?> cls = o1.getClass();

            assert o2.getClass() == cls;

            for (Field f : cls.getDeclaredFields()) {
                f.setAccessible(true);

                Object v1;
                Object v2;

                try {
                    v1 = f.get(o1);
                    v2 = f.get(o2);
                }
                catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }

                if (!Objects.deepEquals(v1, v2))
                    return false;
            }

            return true;
        }
    }

    /**
     * Converts integer permission mode into set of {@link PosixFilePermission}.
     *
     * @param mode File mode.
     * @return Set of {@link PosixFilePermission}.
     */
    public static Set<PosixFilePermission> modeToPermissionSet(int mode) {
        Set<PosixFilePermission> res = EnumSet.noneOf(PosixFilePermission.class);

        if ((mode & 0400) > 0)
            res.add(PosixFilePermission.OWNER_READ);

        if ((mode & 0200) > 0)
            res.add(PosixFilePermission.OWNER_WRITE);

        if ((mode & 0100) > 0)
            res.add(PosixFilePermission.OWNER_EXECUTE);

        if ((mode & 040) > 0)
            res.add(PosixFilePermission.GROUP_READ);

        if ((mode & 020) > 0)
            res.add(PosixFilePermission.GROUP_WRITE);

        if ((mode & 010) > 0)
            res.add(PosixFilePermission.GROUP_EXECUTE);

        if ((mode & 04) > 0)
            res.add(PosixFilePermission.OTHERS_READ);

        if ((mode & 02) > 0)
            res.add(PosixFilePermission.OTHERS_WRITE);

        if ((mode & 01) > 0)
            res.add(PosixFilePermission.OTHERS_EXECUTE);

        return res;
    }

    /**
     * @param name Name.
     * @param run Run.
     */
    public static void benchmark(@Nullable String name, @NotNull Runnable run) {
        benchmark(name, 8000, 10000, run);
    }

    /**
     * @param name Name.
     * @param warmup Warmup.
     * @param executionTime Time.
     * @param run Run.
     */
    public static void benchmark(@Nullable String name, long warmup, long executionTime, @NotNull Runnable run) {
        final AtomicBoolean stop = new AtomicBoolean();

        class Stopper extends TimerTask {
            @Override public void run() {
                stop.set(true);
            }
        }

        new Timer(true).schedule(new Stopper(), warmup);

        while (!stop.get())
            run.run();

        stop.set(false);

        new Timer(true).schedule(new Stopper(), executionTime);

        long startTime = System.currentTimeMillis();

        int cnt = 0;

        do {
            run.run();

            cnt++;
        }
        while (!stop.get());

        double dur = (System.currentTimeMillis() - startTime) / 1000d;

        System.out.printf("%s:\n operations:%d, duration=%fs, op/s=%d, latency=%fms\n", name, cnt, dur,
            (long)(cnt / dur), dur / cnt);
    }

    /**
     * Prompt to execute garbage collector.
     * {@code System.gc();} is not guaranteed to garbage collection, this method try to fill memory to crowd out dead
     * objects.
     */
    public static void runGC() {
        System.gc();

        ReferenceQueue<byte[]> queue = new ReferenceQueue<>();

        Collection<SoftReference<byte[]>> refs = new ArrayList<>();

        while (true) {
            byte[] bytes = new byte[128 * 1024];

            refs.add(new SoftReference<>(bytes, queue));

            if (queue.poll() != null)
                break;
        }

        System.gc();
    }

    /**
     * @return Path to apache ignite.
     */
    public static String apacheIgniteTestPath() {
        return System.getProperty("IGNITE_TEST_PATH", U.getIgniteHome() + "/target/ignite");
    }

    /**
     * {@link Class#getSimpleName()} does not return outer class name prefix for inner classes, for example,
     * getSimpleName() returns "RegularDiscovery" instead of "GridDiscoveryManagerSelfTest$RegularDiscovery"
     * This method return correct simple name for inner classes.
     *
     * @param cls Class
     * @return Simple name with outer class prefix.
     */
    public static String fullSimpleName(@NotNull Class cls) {
        if (cls.getEnclosingClass() != null)
            return cls.getEnclosingClass().getSimpleName() + "." + cls.getSimpleName();
        else
            return cls.getSimpleName();
    }

    /**
     * Adds test to the suite only if it's not in {@code ignoredTests} set.
     *
     * @param suite TestSuite where to place the test.
     * @param test Test.
     * @param ignoredTests Tests to ignore.
     */
    public static void addTestIfNeeded(TestSuite suite, Class test, Set<Class> ignoredTests) {
        if (ignoredTests != null && ignoredTests.contains(test))
            return;

        suite.addTestSuite(test);
    }

    /**
     * Sets cache configuration parameters according to test memory mode.
     *
     * @param cfg Ignite configuration.
     * @param ccfg Cache configuration.
     * @param testMode Test memory mode.
     * @param maxHeapCnt Maximum number of entries in heap (used if test mode involves eviction from heap).
     * @param maxOffheapSize Maximum offheap memory size (used if test mode involves eviction from offheap to swap).
     */
    public static void setMemoryMode(IgniteConfiguration cfg, CacheConfiguration ccfg,
        TestMemoryMode testMode,
        int maxHeapCnt,
        long maxOffheapSize) {
        assert testMode != null;
        assert ccfg != null;

        CacheMemoryMode memMode;
        boolean swap = false;
        boolean evictionPlc = false;
        long offheapMaxMem = -1L;

        switch (testMode) {
            case HEAP: {
                memMode = CacheMemoryMode.ONHEAP_TIERED;
                swap = false;

                break;
            }

            case SWAP: {
                memMode = CacheMemoryMode.ONHEAP_TIERED;
                evictionPlc = true;
                swap = true;

                break;
            }

            case OFFHEAP_TIERED: {
                memMode = CacheMemoryMode.OFFHEAP_TIERED;
                offheapMaxMem = 0;

                break;
            }

            case OFFHEAP_TIERED_SWAP: {
                assert maxOffheapSize > 0 : maxOffheapSize;

                memMode = CacheMemoryMode.OFFHEAP_TIERED;
                offheapMaxMem = maxOffheapSize;
                swap = true;

                break;
            }

            case OFFHEAP_EVICT: {
                memMode = CacheMemoryMode.ONHEAP_TIERED;
                evictionPlc = true;
                offheapMaxMem = 0;

                break;
            }

            case OFFHEAP_EVICT_SWAP: {
                assert maxOffheapSize > 0 : maxOffheapSize;

                memMode = CacheMemoryMode.ONHEAP_TIERED;
                swap = true;
                evictionPlc = true;
                offheapMaxMem = maxOffheapSize;

                break;
            }

            default:
                throw new IllegalArgumentException("Invalid mode: " + testMode);
        }

        ccfg.setMemoryMode(memMode);
        ccfg.setSwapEnabled(swap);

        if (swap && cfg != null)
            cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        if (evictionPlc) {
            LruEvictionPolicy plc = new LruEvictionPolicy();

            plc.setMaxSize(maxHeapCnt);

            ccfg.setEvictionPolicy(plc);
        }

        ccfg.setOffHeapMaxMemory(offheapMaxMem);
    }

    /**
     *
     */
    public enum TestMemoryMode {
        /** Heap only. */
        HEAP,
        /** Evict from heap to swap with eviction policy. */
        SWAP,
        /** Always evict to offheap, no swap. */
        OFFHEAP_TIERED,
        /** Always evict to offheap + evict from offheap to swap when max offheap memory limit is reached. */
        OFFHEAP_TIERED_SWAP,
        /** Evict to offheap with eviction policy, no swap. */
        OFFHEAP_EVICT,
        /** Evict to offheap with eviction policy + evict from offheap to swap when max offheap memory limit is reached. */
        OFFHEAP_EVICT_SWAP,
    }
}