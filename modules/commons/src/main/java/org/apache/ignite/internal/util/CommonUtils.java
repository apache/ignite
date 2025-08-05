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

package org.apache.ignite.internal.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used in 'ignite-commons' and throughout the system.
 */
public abstract class CommonUtils {
    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     * @see java.util.ArrayList#MAX_ARRAY_SIZE
     */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /** @see IgniteCommonsSystemProperties#IGNITE_MEMORY_PER_BYTE_COPY_THRESHOLD */
    public static final long DFLT_MEMORY_PER_BYTE_COPY_THRESHOLD = 0L;

    /** @see IgniteCommonsSystemProperties#IGNITE_MARSHAL_BUFFERS_RECHECK */
    public static final int DFLT_MARSHAL_BUFFERS_RECHECK = 10000;

    /** @see IgniteCommonsSystemProperties#IGNITE_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE */
    public static final int DFLT_MARSHAL_BUFFERS_PER_THREAD_POOL_SIZE = 32;

    /** Sun-specific JDK constructor factory for objects that don't have empty constructor. */
    private static final Method CTOR_FACTORY;

    /** Sun JDK reflection factory. */
    private static final Object SUN_REFLECT_FACTORY;

    /** Public {@code java.lang.Object} no-argument constructor. */
    private static final Constructor OBJECT_CTOR;

    /** System line separator. */
    static final String NL = System.getProperty("line.separator");

    /** Version of the JDK. */
    static String jdkVer;

    /** */
    static volatile long curTimeMillis = System.currentTimeMillis();

    /** Clock timer. */
    private static Thread timer;

    /** Grid counter. */
    static int gridCnt;

    /** Mutex. */
    static final Object mux = new Object();

    static {
        try {
            OBJECT_CTOR = Object.class.getConstructor();
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError("Object class does not have empty constructor (is JDK corrupted?).", e);
        }

        // Constructor factory.
        Method ctorFac = null;
        Object refFac = null;

        try {
            Class<?> refFactoryCls = Class.forName("sun.reflect.ReflectionFactory");

            refFac = refFactoryCls.getMethod("getReflectionFactory").invoke(null);

            ctorFac = refFac.getClass().getMethod("newConstructorForSerialization", Class.class,
                Constructor.class);
        }
        catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException ignored) {
            // No-op.
        }

        CTOR_FACTORY = ctorFac;
        SUN_REFLECT_FACTORY = refFac;

        CommonUtils.jdkVer = System.getProperty("java.specification.version");
    }

    /**
     * Creates new instance of a class even if it does not have public constructor.
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T forceNewInstance(Class<?> cls) throws IgniteCheckedException {
        Constructor ctor = forceEmptyConstructor(cls);

        if (ctor == null)
            return null;

        boolean set = false;

        try {

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return (T)ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Gets empty constructor for class even if the class does not have empty constructor
     * declared. This method is guaranteed to work with SUN JDK and other JDKs still need
     * to be tested.
     *
     * @param cls Class to get empty constructor for.
     * @return Empty constructor if one could be found or {@code null} otherwise.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static Constructor<?> forceEmptyConstructor(Class<?> cls) throws IgniteCheckedException {
        Constructor<?> ctor = null;

        try {
            return cls.getDeclaredConstructor();
        }
        catch (Exception ignore) {
            Method ctorFac = CTOR_FACTORY;
            Object sunRefFac = sunReflectionFactory();

            if (ctorFac != null && sunRefFac != null)
                try {
                    ctor = (Constructor)ctorFac.invoke(sunRefFac, cls, OBJECT_CTOR);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new IgniteCheckedException("Failed to get object constructor for class: " + cls, e);
                }
        }

        return ctor;
    }

    /**
     * SUN JDK specific reflection factory for objects without public constructor.
     *
     * @return Reflection factory for objects without public constructor.
     */
    @Nullable public static Object sunReflectionFactory() {
        return SUN_REFLECT_FACTORY;
    }

    /**
     * Returns a capacity that is sufficient to keep the map from being resized as
     * long as it grows no larger than expSize and the load factor is >= its
     * default (0.75).
     *
     * Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of created map.
     * @return Capacity.
     */
    public static int capacity(int expSize) {
        if (expSize < 3)
            return expSize + 1;

        if (expSize < (1 << 30))
            return expSize + expSize / 3;

        return Integer.MAX_VALUE; // any large value
    }

    /**
     * @return {@code line.separator} system property.
     */
    public static String nl() {
        return NL;
    }

    /**
     * Gets JDK version.
     *
     * @return JDK version.
     */
    public static String jdkVersion() {
        return jdkVer;
    }

    /**
     * Get major Java version from string.
     *
     * @param verStr Version string.
     * @return Major version or zero if failed to resolve.
     */
    public static int majorJavaVersion(String verStr) {
        if (F.isEmpty(verStr))
            return 0;

        try {
            String[] parts = verStr.split("\\.");

            int major = Integer.parseInt(parts[0]);

            if (parts.length == 1)
                return major;

            int minor = Integer.parseInt(parts[1]);

            return major == 1 ? minor : major;
        }
        catch (Exception e) {
            return 0;
        }
    }

    /**
     * @param x X.
     * @param less Less.
     */
    public static int nearestPow2(int x, boolean less) {
        assert x > 0 : "can not calculate for less zero";

        long y = 1;

        while (y < x) {
            if (y * 2 > Integer.MAX_VALUE)
                return (int)y;

            y *= 2;
        }

        if (less)
            y /= 2;

        return (int)y;
    }

    /**
     * @param svcCls Service class to load.
     * @param <S> Type of loaded interfaces.
     * @return Lazy iterable structure over loaded class implementations.
     */
    public static <S> Iterable<S> loadService(Class<S> svcCls) {
        return AccessController.doPrivileged(new PrivilegedAction<Iterable<S>>() {
            @Override public Iterable<S> run() {
                return ServiceLoader.load(svcCls);
            }
        });
    }

    /**
     * Convert milliseconds time interval to nanoseconds.
     *
     * @param millis Original time interval.
     * @return Calculated time interval.
     */
    public static long millisToNanos(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /**
     * Convert nanoseconds time interval to milliseconds.
     *
     * @param nanos Original time interval.
     * @return Calculated time interval.
     */
    public static long nanosToMillis(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }

    /**
     * Returns number of milliseconds passed after the given nanos timestamp.
     *
     * @param nanos Nanos timestamp.
     * @return Number of milliseconds passed after the given nanos timestamp.
     * @see System#nanoTime()
     */
    public static long millisSinceNanos(long nanos) {
        return nanosToMillis(System.nanoTime() - nanos);
    }

    /**
     * Gets nearest power of 2 larger or equal than v.
     *
     * @param v Value.
     * @return Nearest power of 2.
     */
    public static int ceilPow2(int v) {
        int i = v - 1;

        return Integer.highestOneBit(i) << 1 - (i >>> 30 ^ v >> 31);
    }

    /**
     * @param i Value.
     * @return {@code true} If the given value is power of 2 (0 is not power of 2).
     */
    public static boolean isPow2(int i) {
        return i > 0 && (i & (i - 1)) == 0;
    }

    /**
     * Round up the argument to the next highest power of 2;
     *
     * @param v Value to round up.
     * @return Next closest power of 2.
     */
    public static int nextPowerOf2(int v) {
        A.ensure(v >= 0, "v must not be negative");

        if (v == 0)
            return 1;

        return 1 << (32 - Integer.numberOfLeadingZeros(v - 1));
    }

    /**
     * Starts clock timer if grid is first.
     */
    public static void onGridStart() {
        synchronized (mux) {
            if (gridCnt == 0) {
                assert timer == null;

                timer = new Thread(new Runnable() {
                    @SuppressWarnings({"BusyWait"})
                    @Override public void run() {
                        while (true) {
                            curTimeMillis = System.currentTimeMillis();

                            try {
                                Thread.sleep(10);
                            }
                            catch (InterruptedException ignored) {
                                break;
                            }
                        }
                    }
                }, "ignite-clock");

                timer.setDaemon(true);

                timer.setPriority(10);

                timer.start();
            }

            ++gridCnt;
        }
    }

    /**
     * Stops clock timer if all nodes into JVM were stopped.
     * @throws InterruptedException If interrupted.
     */
    public static void onGridStop() throws InterruptedException {
        synchronized (mux) {
            // Grid start may fail and onGridStart() does not get called.
            if (gridCnt == 0)
                return;

            --gridCnt;

            Thread timer0 = timer;

            if (gridCnt == 0 && timer0 != null) {
                timer = null;

                timer0.interrupt();

                timer0.join();
            }
        }
    }

    /**
     * @return System time approximated by 10 ms.
     */
    public static long currentTimeMillis() {
        return curTimeMillis;
    }

    /**
     * Writes UUID to output stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param out Output stream.
     * @param uid UUID to write.
     * @throws IOException If write failed.
     */
    public static void writeUuid(DataOutput out, UUID uid) throws IOException {
        // Write null flag.
        out.writeBoolean(uid == null);

        if (uid != null) {
            out.writeLong(uid.getMostSignificantBits());
            out.writeLong(uid.getLeastSignificantBits());
        }
    }

    /**
     * Reads UUID from input stream. This method is meant to be used by
     * implementations of {@link Externalizable} interface.
     *
     * @param in Input stream.
     * @return Read UUID.
     * @throws IOException If read failed.
     */
    @Nullable public static UUID readUuid(DataInput in) throws IOException {
        // If UUID is not null.
        if (!in.readBoolean()) {
            long most = in.readLong();
            long least = in.readLong();

            return IgniteUuidCache.onIgniteUuidRead(new UUID(most, least));
        }

        return null;
    }
}
