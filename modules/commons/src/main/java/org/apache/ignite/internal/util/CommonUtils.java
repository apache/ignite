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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.CacheClassLoaderMarker;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.isNull;
import static org.apache.ignite.IgniteCommonsSystemProperties.IGNITE_HOME;

/**
 * Collection of utility methods used in 'ignite-commons' and throughout the system.
 */
public abstract class CommonUtils {
    /** */
    public static final long KB = 1024L;

    /** */
    public static final long MB = 1024L * 1024;

    /** */
    public static final long GB = 1024L * 1024 * 1024;

    /** Dev only logging disabled. */
    private static final boolean devOnlyLogDisabled =
        IgniteCommonsSystemProperties.getBoolean(IgniteCommonsSystemProperties.IGNITE_DEV_ONLY_LOGGING_DISABLED);

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

    /** Project home directory. */
    private static volatile GridTuple<String> ggHome;

    /** */
    static volatile long curTimeMillis = System.currentTimeMillis();

    /** Clock timer. */
    private static Thread timer;

    /** Grid counter. */
    static int gridCnt;

    /** Indicates whether current OS is some version of Windows. */
    private static boolean win;

    /** Indicates whether current OS is UNIX flavor. */
    private static boolean unix;

    /** Indicates whether current OS is Linux flavor. */
    private static boolean linux;

    /** Indicates whether current OS is Mac OS. */
    private static boolean mac;

    /** Empty URL array. */
    private static final URL[] EMPTY_URL_ARR = new URL[0];

    /** Builtin class loader class.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Class bltClsLdrCls = defaultClassLoaderClass();

    /** Url class loader field.
     *
     * Note: needs for compatibility with Java 9.
     */
    private static final Field urlClsLdrField = urlClassLoaderField();

    /** JDK9: jdk.internal.loader.URLClassPath. */
    private static Class clsURLClassPath;

    /** JDK9: URLClassPath#getURLs. */
    private static Method mthdURLClassPathGetUrls;

    /** Mutex. */
    static final Object mux = new Object();

    /** Empty local Ignite name. */
    public static final String LOC_IGNITE_NAME_EMPTY = new String();

    /** Local Ignite name thread local. */
    private static final ThreadLocal<String> LOC_IGNITE_NAME = new ThreadLocal<String>() {
        @Override protected String initialValue() {
            return LOC_IGNITE_NAME_EMPTY;
        }
    };

    /** Ignite package. */
    public static final String IGNITE_PKG = "org.apache.ignite.";

    /**
     * Short date format pattern for log messages in "quiet" mode.
     * Only time is included since we don't expect "quiet" mode to be used
     * for longer runs.
     */
    public static final DateTimeFormatter SHORT_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    /** Class loader used to load Ignite. */
    private static final ClassLoader gridClassLoader = CommonUtils.class.getClassLoader();

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = new HashMap<>(16, .5f);

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    /** Exception converters. */
    protected static final Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>>
        exceptionConverters;

    static {
        primitiveMap.put("byte", byte.class);
        primitiveMap.put("short", short.class);
        primitiveMap.put("int", int.class);
        primitiveMap.put("long", long.class);
        primitiveMap.put("float", float.class);
        primitiveMap.put("double", double.class);
        primitiveMap.put("char", char.class);
        primitiveMap.put("boolean", boolean.class);
        primitiveMap.put("void", void.class);

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

        jdkVer = System.getProperty("java.specification.version");

        String osName = System.getProperty("os.name");

        String osLow = osName.toLowerCase();

        // OS type detection.
        if (osLow.contains("win"))
            win = true;
        else if (osLow.contains("mac os"))
            mac = true;
        else {
            // UNIXs flavors tokens.
            for (CharSequence os : new String[] {"ix", "inux", "olaris", "un", "ux", "sco", "bsd", "att"})
                if (osLow.contains(os)) {
                    unix = true;

                    break;
                }

            if (osLow.contains("inux"))
                linux = true;
        }

        exceptionConverters = exceptionConverters();

        try {
            clsURLClassPath = Class.forName("jdk.internal.loader.URLClassPath");
            mthdURLClassPathGetUrls = clsURLClassPath.getMethod("getURLs");
        }
        catch (ReflectiveOperationException e) {
            clsURLClassPath = null;
            mthdURLClassPathGetUrls = null;
        }
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
     * @return System time approximated by 10 ms.
     */
    public static long currentTimeMillis() {
        return curTimeMillis;
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

    /**
     * Get current Ignite name.
     *
     * @return Current Ignite name.
     */
    @Nullable public static String getCurrentIgniteName() {
        return LOC_IGNITE_NAME.get();
    }

    /**
     * Check if current Ignite name is set.
     *
     * @param name Name to check.
     * @return {@code True} if set.
     */
    @SuppressWarnings("StringEquality")
    public static boolean isCurrentIgniteNameSet(@Nullable String name) {
        return name != LOC_IGNITE_NAME_EMPTY;
    }

    /**
     * Set current Ignite name.
     *
     * @param newName New name.
     * @return Old name.
     */
    @SuppressWarnings("StringEquality")
    @Nullable public static String setCurrentIgniteName(@Nullable String newName) {
        String oldName = LOC_IGNITE_NAME.get();

        if (oldName != newName)
            LOC_IGNITE_NAME.set(newName);

        return oldName;
    }

    /**
     * Restore old Ignite name.
     *
     * @param oldName Old name.
     * @param curName Current name.
     */
    @SuppressWarnings("StringEquality")
    public static void restoreOldIgniteName(@Nullable String oldName, @Nullable String curName) {
        if (oldName != curName)
            LOC_IGNITE_NAME.set(oldName);
    }

    /**
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader gridClassLoader() {
        return gridClassLoader;
    }

    /**
     * @param ldr Custom class loader.
     * @param cfgLdr Class loader from config.
     * @return ClassLoader passed as param in case it is not null or cfgLdr  in case it is not null or ClassLoader used to start Ignite.
     */
    public static ClassLoader resolveClassLoader(@Nullable ClassLoader ldr, @Nullable ClassLoader cfgLdr) {
        return (ldr != null && ldr != gridClassLoader)
            ? ldr
            : cfgLdr != null
                ? cfgLdr
                : gridClassLoader;
    }

    /**
     * Tests whether given class is loadable with provided class loader.
     *
     * @param clsName Class name to test.
     * @param ldr Class loader to test with. If {@code null} - we'll use system class loader instead.
     *      If System class loader is not set - this method will return {@code false}.
     * @return {@code True} if class is loadable, {@code false} otherwise.
     */
    public static boolean isLoadableBy(String clsName, @Nullable ClassLoader ldr) {
        assert clsName != null;

        if (ldr == null)
            ldr = gridClassLoader;

        String lambdaParent = lambdaEnclosingClassName(clsName);

        try {
            ldr.loadClass(lambdaParent == null ? clsName : lambdaParent);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(@Nullable String cls, @Nullable Class<?> dflt) {
        return classForName(cls, dflt, false);
    }

    /**
     * Gets class for the given name if it can be loaded or default given class.
     *
     * @param cls Class.
     * @param dflt Default class to return.
     * @param includePrimitiveTypes Whether class resolution should include primitive types
     *                              (i.e. "int" will resolve to int.class if flag is set)
     * @return Class or default given class if it can't be found.
     */
    @Nullable public static Class<?> classForName(
        @Nullable String cls,
        @Nullable Class<?> dflt,
        boolean includePrimitiveTypes
    ) {
        Class<?> clazz;
        if (cls == null)
            clazz = dflt;
        else if (!includePrimitiveTypes || cls.length() > 7 || (clazz = primitiveMap.get(cls)) == null) {
            try {
                clazz = Class.forName(cls);
            }
            catch (ClassNotFoundException ignore) {
                clazz = dflt;
            }
        }
        return clazz;
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param useCache If true class loader and result should be cached internally, false otherwise.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter,
        boolean useCache
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null)
            return cls;

        if (ldr != null) {
            if (ldr instanceof ClassCache)
                return ((ClassCache)ldr).getFromCache(clsName);
            else if (!useCache) {
                cls = Class.forName(clsName, true, ldr);

                return cls;
            }
        }
        else
            ldr = gridClassLoader;

        if (!useCache) {
            cls = Class.forName(clsName, true, ldr);

            return cls;
        }

        ConcurrentMap<String, Class> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.apply(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            // Avoid class caching inside Class.forName
            if (ldr instanceof CacheClassLoaderMarker)
                cls = ldr.loadClass(clsName);
            else
                cls = Class.forName(clsName, true, ldr);

            Class old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Clears class associated with provided class loader from class cache.
     *
     * @param ldr Class loader.
     * @param clsName Class name of clearing class.
     */
    public static void clearClassFromClassCache(ClassLoader ldr, String clsName) {
        ConcurrentMap<String, Class> map = classCache.get(ldr);

        if (map != null)
            map.remove(clsName);
    }

    /**
     * Clears class cache for provided loader.
     *
     * @param ldr Class loader.
     */
    public static void clearClassCache(ClassLoader ldr) {
        classCache.remove(ldr);
    }

    /**
     * Completely clears class cache.
     */
    public static void clearClassCache() {
        classCache.clear();
    }

    /**
     * Extracts full name of enclosing class from JDK8 lambda class name.
     *
     * @param clsName JDK8 lambda class name.
     * @return Full name of enclosing class for JDK8 lambda class name or
     *      {@code null} if passed in name is not related to lambda.
     */
    @Nullable public static String lambdaEnclosingClassName(String clsName) {
        int idx0 = clsName.indexOf("$$Lambda$"); // Java 8+
        int idx1 = clsName.indexOf("$$Lambda/"); // Java 21+

        if (idx0 == idx1)
            return null;

        return clsName.substring(0, idx0 >= 0 ? idx0 : idx1);
    }

    /**
     * Returns {@code true} if class is a lambda.
     *
     * @param objectClass Class.
     * @return {@code true} if class is a lambda, {@code false} otherwise.
     */
    public static boolean isLambda(Class<?> objectClass) {
        return !objectClass.isPrimitive() && !objectClass.isArray()
            // Order is crucial here, isAnonymousClass and isLocalClass may fail if
            // class' outer class was loaded with different classloader.
            && objectClass.isSynthetic()
            && !objectClass.isAnonymousClass() && !objectClass.isLocalClass()
            && classCannotBeLoadedByName(objectClass);
    }

    /**
     * Returns {@code true} if class can not be loaded by name.
     *
     * @param objectClass Class.
     * @return {@code true} if class can not be loaded by name, {@code false} otherwise.
     */
    public static boolean classCannotBeLoadedByName(Class<?> objectClass) {
        try {
            Class.forName(objectClass.getName());
            return false;
        }
        catch (ClassNotFoundException e) {
            return true;
        }
    }

    /**
     * Creates new {@link HashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of map keys.
     * @param <V> Type of map values.
     * @return New map.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int expSize) {
        return new LinkedHashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link HashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> HashSet<T> newHashSet(int expSize) {
        return new HashSet<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashSet} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <T> Type of elements.
     * @return New set.
     */
    public static <T> LinkedHashSet<T> newLinkedHashSet(int expSize) {
        return new LinkedHashSet<>(capacity(expSize));
    }

    /**
     * Creates new map that limited by size.
     *
     * @param limit Limit for size.
     */
    public static <K, V> Map<K, V> limitedMap(int limit) {
        if (limit == 0)
            return Collections.emptyMap();

        if (limit < 5)
            return new GridLeanMap<>(limit);

        return new HashMap<>(capacity(limit), 0.75f);
    }

    /**
     * @param col non-null collection with one element
     * @return a SingletonList containing the element in the original collection
     */
    public static <T> Collection<T> convertToSingletonList(Collection<T> col) {
        if (col.size() != 1) {
            throw new IllegalArgumentException("Unexpected collection size for singleton list, expecting 1 but was: " + col.size());
        }
        return Collections.singletonList(col.iterator().next());
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable AutoCloseable rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null) {
            try {
                rsrc.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Closes given socket logging possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Socket sock, @Nullable IgniteLogger log) {
        if (sock == null || sock.isClosed())
            return;

        try {
            // Closing output and input first to avoid tls 1.3 incompatibility
            // https://bugs.openjdk.java.net/browse/JDK-8208526
            if (!sock.isOutputShutdown())
                sock.shutdownOutput();
            if (!sock.isInputShutdown())
                sock.shutdownInput();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to shutdown socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to shutdown socket: " + e.getMessage(), e);
        }

        try {
            sock.close();
        }
        catch (ClosedChannelException | SocketException ex) {
            LT.warn(log, "Failed to close socket", ex);
        }
        catch (Exception e) {
            warn(log, "Failed to close socket: " + e.getMessage(), e);
        }
    }

    /**
     * Closes given resource logging possible checked exceptions.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable SelectionKey rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            // This apply will automatically deregister the selection key as well.
            close(rsrc.channel(), log);
    }

    /**
     * Closes given resource.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void close(@Nullable DatagramSocket rsrc) {
        if (rsrc != null)
            rsrc.close();
    }

    /**
     * Closes given resource logging possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable Selector rsrc, @Nullable IgniteLogger log) {
        if (rsrc != null)
            try {
                if (rsrc.isOpen())
                    rsrc.close();
            }
            catch (IOException e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
    }

    /**
     * Closes class loader logging possible checked exception.
     *
     * @param clsLdr Class loader. If it's {@code null} - it's no-op.
     * @param log Logger to log possible checked exception with (optional).
     */
    public static void close(@Nullable URLClassLoader clsLdr, @Nullable IgniteLogger log) {
        if (clsLdr != null) {
            try {
                clsLdr.close();
            }
            catch (Exception e) {
                warn(log, "Failed to close resource: " + e.getMessage());
            }
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        warn(log, s, null);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (wrn)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log using normal logger.
     * @param e Optional exception.
     */
    public static void warn(@Nullable IgniteLogger log, Object msg, @Nullable Throwable e) {
        assert msg != null;

        if (log != null)
            log.warning(compact(msg.toString()), e);
        else {
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                compact(msg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg) {
        quietAndWarn(log, msg, msg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param shortMsg Short message.
     * @param msg Message to log.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, Object shortMsg) {
        warn(log, msg);

        if (log.isQuiet())
            quiet(false, shortMsg);
    }

    /**
     * Logs warning message in both verbose and quiet modes.
     *
     * @param log Logger to use.
     * @param msg Message to log.
     * @param e Optional exception.
     */
    public static void quietAndWarn(IgniteLogger log, Object msg, @Nullable Throwable e) {
        warn(log, msg, e);

        if (log.isQuiet())
            quiet(false, msg);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void error(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        if (msg instanceof Throwable) {
            Throwable t = (Throwable)msg;

            error(log, t.getMessage(), t);
        }
        else {
            String s = msg.toString();

            error(log, s, s, null);
        }
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log WARN message with {@link IgniteLogger#DEV_ONLY DEV_ONLY} marker.
     * If {@code log} is {@code null} or in QUIET mode it will add {@code (wrn)} prefix to the message.
     * If property {@link IgniteCommonsSystemProperties#IGNITE_DEV_ONLY_LOGGING_DISABLED IGNITE_DEV_ONLY_LOGGING_DISABLED}
     * is set to true, the message will not be logged.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void warnDevOnly(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        // don't log message if DEV_ONLY messages are disabled
        if (devOnlyLogDisabled)
            return;

        if (log != null)
            log.warning(IgniteLogger.DEV_ONLY, compact(msg.toString()), null);
        else
            X.println("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (wrn) " +
                compact(msg.toString()));
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INFO message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     */
    public static void log(@Nullable IgniteLogger log, Object longMsg, Object shortMsg) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (log.isInfoEnabled())
                log.info(compact(longMsg.toString()));
        }
        else
            quiet(false, shortMsg);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log INF0 message.
     * <p>
     * <b>NOTE:</b> unlike the normal logging when INFO level may not be enabled and
     * therefore no logging will happen - using this method the log will be written
     * always either via INFO log or quiet mode.
     * <p>
     * <b>USE IT APPROPRIATELY.</b>
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param msg Message to log.
     */
    public static void log(@Nullable IgniteLogger log, Object msg) {
        assert msg != null;

        String s = msg.toString();

        log(log, s, s);
    }

    /**
     * Depending on whether or not log is provided and quiet mode is enabled logs given
     * messages as quiet message or normal log ERROR message. If {@code log} is {@code null}
     * or in QUIET mode it will add {@code (err)} prefix to the message.
     *
     * @param log Optional logger to use when QUIET mode is not enabled.
     * @param longMsg Message to log using normal logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object longMsg, Object shortMsg, @Nullable Throwable e) {
        assert longMsg != null;
        assert shortMsg != null;

        if (log != null) {
            if (e == null)
                log.error(compact(longMsg.toString()));
            else
                log.error(compact(longMsg.toString()), e);
        }
        else {
            X.printerr("[" + SHORT_DATE_FMT.format(Instant.now()) + "] (err) " +
                compact(shortMsg.toString()));

            if (e != null)
                e.printStackTrace(System.err);
            else
                X.printerrln();
        }
    }

    /**
     * Shortcut for {@link #error(org.apache.ignite.IgniteLogger, Object, Object, Throwable)}.
     *
     * @param log Optional logger.
     * @param shortMsg Message to log using quiet logger.
     * @param e Optional exception.
     */
    public static void error(@Nullable IgniteLogger log, Object shortMsg, @Nullable Throwable e) {
        assert shortMsg != null;

        String s = shortMsg.toString();

        error(log, s, s, e);
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param objs Objects to log in quiet mode.
     */
    public static void quiet(boolean err, Object... objs) {
        assert objs != null;

        String time = SHORT_DATE_FMT.format(Instant.now());

        SB sb = new SB();

        for (Object obj : objs)
            sb.a('[').a(time).a("] ").a(obj.toString()).a(NL);

        PrintStream ps = err ? System.err : System.out;

        ps.print(compact(sb.toString()));
    }

    /**
     *
     * @param err Whether to print to {@code System.err}.
     * @param multiline Multiple lines string to print.
     */
    public static void quietMultipleLines(boolean err, String multiline) {
        assert multiline != null;

        quiet(err, multiline.split(NL));
    }

    /**
     * Prints out the message in quiet and info modes.
     *
     * @param log Logger.
     * @param msg Message to print.
     */
    public static void quietAndInfo(IgniteLogger log, String msg) {
        if (log.isQuiet())
            quiet(false, msg);

        if (log.isInfoEnabled())
            log.info(msg);
    }

    /**
     * Replaces all occurrences of {@code org.apache.ignite.} with {@code o.a.i.},
     * {@code org.apache.ignite.internal.} with {@code o.a.i.i.},
     * {@code org.apache.ignite.internal.visor.} with {@code o.a.i.i.v.} and
     *
     * @param s String to replace in.
     * @return Replaces string.
     */
    public static String compact(String s) {
        return s.replace("org.apache.ignite.internal.visor.", "o.a.i.i.v.").
            replace("org.apache.ignite.internal.", "o.a.i.i.").
            replace(IGNITE_PKG, "o.a.i.");
    }

    /**
     * Check if given class represents a Enum.
     *
     * @param cls Class to check.
     * @return {@code True} if this is a Enum class.
     */
    public static boolean isEnum(Class cls) {
        if (cls.isEnum())
            return true;

        Class sCls = cls.getSuperclass();

        return sCls != null && sCls.isEnum();
    }

    /**
     * Returns a first non-null value in a given array, if such is present.
     *
     * @param vals Input array.
     * @return First non-null value, or {@code null}, if array is empty or contains
     *      only nulls.
     */
    @Nullable public static <T> T firstNotNull(@Nullable T... vals) {
        if (vals == null)
            return null;

        for (T val : vals) {
            if (val != null)
                return val;
        }

        return null;
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if class is final.
     */
    public static boolean isFinal(Class<?> cls) {
        return Modifier.isFinal(cls.getModifiers());
    }

    /**
     * Checks if given class is of {@code Ignite} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Ignite} type.
     */
    public static boolean isIgnite(Class<?> cls) {
        String name = cls.getName();

        return name.startsWith("org.apache.ignite") || name.startsWith("org.jsr166");
    }

    /**
     * Checks if given class is of {@code Grid} type.
     *
     * @param cls Class to check.
     * @return {@code True} if given class is of {@code Grid} type.
     */
    public static boolean isGrid(Class<?> cls) {
        return cls.getName().startsWith("org.apache.ignite.internal");
    }

    /**
     * Check if given class is of JDK type.
     *
     * @param cls Class to check.
     * @return {@code True} if object is JDK type.
     */
    public static boolean isJdk(Class<?> cls) {
        if (cls.isPrimitive())
            return true;

        String s = cls.getName();

        return s.startsWith("java.") || s.startsWith("javax.");
    }

    /**
     * Finds a non-static and non-abstract method from the class it parents.
     *
     * Method.getMethod() does not return non-public method.
     *
     * @param cls Target class.
     * @param name Name of the method.
     * @param paramTypes Method parameters.
     * @return Method or {@code null}.
     */
    @Nullable public static Method findInheritableMethod(Class<?> cls, String name, Class<?>... paramTypes) {
        Method mtd = null;

        Class<?> cls0 = cls;

        while (cls0 != null) {
            try {
                mtd = cls0.getDeclaredMethod(name, paramTypes);

                break;
            }
            catch (NoSuchMethodException e) {
                cls0 = cls0.getSuperclass();
            }
        }

        if (mtd == null)
            return null;

        mtd.setAccessible(true);

        int mods = mtd.getModifiers();

        if ((mods & (Modifier.STATIC | Modifier.ABSTRACT)) != 0)
            return null;
        else if ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0)
            return mtd;
        else if ((mods & Modifier.PRIVATE) != 0)
            return cls == cls0 ? mtd : null;
        else {
            ClassLoader clsLdr = cls.getClassLoader();

            ClassLoader clsLdr0 = cls0.getClassLoader();

            return clsLdr == clsLdr0 && packageName(cls).equals(packageName(cls0)) ? mtd : null;
        }
    }

    /**
     * @param cls Class.
     * @return Package name.
     */
    public static String packageName(Class<?> cls) {
        Package pkg = cls.getPackage();

        return pkg == null ? "" : pkg.getName();
    }

    /**
     * Writes string to output stream accounting for {@code null} values.
     * <p>
     * Limitation for max string lenght of <code>65535</code> bytes is caused by {@link DataOutput#writeUTF}
     * used under the hood to perform an actual write.
     * </p>
     * <p>
     * If longer string is passes a {@link UTFDataFormatException} exception will be thrown.
     * </p>
     * <p>
     * To write longer strings use {@link #writeLongString(DataOutput, String)} writes string as is converting it into binary array of UTF-8
     * encoded characters.
     * To read the value back {@link #readLongString(DataInput)} should be used.
     * </p>
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeString(DataOutput out, String s) throws IOException {
        // Write null flag.
        out.writeBoolean(s == null);

        if (s != null)
            out.writeUTF(s);
    }

    /**
     * Reads string from input stream accounting for {@code null} values.
     *
     * Method enables to read strings shorter than <code>65535</code> bytes in UTF-8 otherwise an exception will be thrown.
     *
     * Strings written by {@link #writeString(DataOutput, String)} can be read by this method.
     *
     * @see #writeString(DataOutput, String) for more information about writing strings.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readString(DataInput in) throws IOException {
        // If value is not null, then read it. Otherwise return null.
        return !in.readBoolean() ? in.readUTF() : null;
    }

    /**
     * Writes string to output stream accounting for {@code null} values. <br/>
     *
     * This method can write string of any length, limit of <code>65535</code> is not applied.
     *
     * @param out Output stream to write to.
     * @param s String to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static void writeLongString(DataOutput out, @Nullable String s) throws IOException {
        // Write null flag.
        out.writeBoolean(isNull(s));

        if (isNull(s))
            return;

        int sLen = s.length();

        // Write string length.
        out.writeInt(sLen);

        // Write byte array.
        for (int i = 0; i < sLen; i++) {
            char c = s.charAt(i);
            int utfBytes = utfBytes(c);

            if (utfBytes == 1)
                out.writeByte((byte)c);
            else if (utfBytes == 3) {
                out.writeByte((byte)(0xE0 | (c >> 12) & 0x0F));
                out.writeByte((byte)(0x80 | (c >> 6) & 0x3F));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
            else {
                out.writeByte((byte)(0xC0 | ((c >> 6) & 0x1F)));
                out.writeByte((byte)(0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Reads string from input stream accounting for {@code null} values. <br/>
     *
     * This method can read string of any length, limit of <code>65535</code> is not applied.
     *
     * @param in Stream to read from.
     * @return Read string, possibly {@code null}.
     * @throws IOException If read failed.
     */
    @Nullable public static String readLongString(DataInput in) throws IOException {
        // Check null value.
        if (in.readBoolean())
            return null;

        // Read string length.
        int sLen = in.readInt();

        StringBuilder strBuilder = new StringBuilder(sLen);

        // Read byte array.
        for (int i = 0, b0, b1, b2; i < sLen; i++) {
            b0 = in.readByte() & 0xff;

            switch (b0 >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:   // 1 byte format: 0xxxxxxx
                    strBuilder.append((char)b0);
                    break;

                case 12:
                case 13:  // 2 byte format: 110xxxxx 10xxxxxx
                    b1 = in.readByte();

                    if ((b1 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x1F) << 6) | (b1 & 0x3F)));
                    break;

                case 14:  // 3 byte format: 1110xxxx 10xxxxxx 10xxxxxx
                    b1 = in.readByte();
                    b2 = in.readByte();

                    if ((b1 & 0xC0) != 0x80 || (b2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException();

                    strBuilder.append((char)(((b0 & 0x0F) << 12) | ((b1 & 0x3F) << 6) | (b2 & 0x3F)));
                    break;

                default:  // 10xx xxxx, 1111 xxxx
                    throw new UTFDataFormatException();
            }
        }

        return strBuilder.toString();
    }

    /**
     * Get number of bytes for {@link DataOutput#writeUTF},
     * depending on character: <br/>
     *
     * One byte - If a character <code>c</code> is in the range
     * <code>&#92;u0001</code> through <code>&#92;u007f</code>.<br/>
     *
     * Two bytes - If a character <code>c</code> is <code>&#92;u0000</code> or
     * is in the range <code>&#92;u0080</code> through <code>&#92;u07ff</code>.
     * <br/>
     *
     * Three bytes - If a character <code>c</code> is in the range
     * <code>&#92;u0800</code> through <code>uffff</code>.
     *
     * @param c Character.
     * @return Number of bytes.
     */
    public static int utfBytes(char c) {
        return (c >= 0x0001 && c <= 0x007F) ? 1 : (c > 0x07FF) ? 3 : 2;
    }

    /**
     * Retrieves {@code IGNITE_HOME} property. The property is retrieved from system
     * properties or from environment in that order.
     *
     * @return {@code IGNITE_HOME} property.
     */
    @Nullable public static String getIgniteHome() {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (CommonUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    // Resolve Ignite installation home directory.
                    ggHome = F.t(ggHome0 = resolveProjectHome());

                    if (ggHome0 != null)
                        System.setProperty(IGNITE_HOME, ggHome0);
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        return ggHome0;
    }

    /**
     * Resolve project home directory based on source code base.
     *
     * @return Project home directory (or {@code null} if it cannot be resolved).
     */
    @Nullable private static String resolveProjectHome() {
        assert Thread.holdsLock(CommonUtils.class);

        // Resolve Ignite home via environment variables.
        String ggHome0 = IgniteCommonsSystemProperties.getString(IGNITE_HOME);

        if (!F.isEmpty(ggHome0))
            return ggHome0;

        String appWorkDir = System.getProperty("user.dir");

        if (appWorkDir != null) {
            ggHome0 = findProjectHome(new File(appWorkDir));

            if (ggHome0 != null)
                return ggHome0;
        }

        URI classesUri;

        Class<CommonUtils> cls = CommonUtils.class;

        try {
            ProtectionDomain domain = cls.getProtectionDomain();

            // Should not happen, but to make sure our code is not broken.
            if (domain == null || domain.getCodeSource() == null || domain.getCodeSource().getLocation() == null) {
                logResolveFailed(cls, null);

                return null;
            }

            // Resolve path to class-file.
            classesUri = domain.getCodeSource().getLocation().toURI();

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (isWindows() && classesUri.getAuthority() != null)
                classesUri = new URI(classesUri.toString().replace("file://", "file:/"));
        }
        catch (URISyntaxException | SecurityException e) {
            logResolveFailed(cls, e);

            return null;
        }

        File classesFile;

        try {
            classesFile = new File(classesUri);
        }
        catch (IllegalArgumentException e) {
            logResolveFailed(cls, e);

            return null;
        }

        return findProjectHome(classesFile);
    }

    /**
     * Tries to find project home starting from specified directory and moving to root.
     *
     * @param startDir First directory in search hierarchy.
     * @return Project home path or {@code null} if it wasn't found.
     */
    private static String findProjectHome(File startDir) {
        for (File cur = startDir.getAbsoluteFile(); cur != null; cur = cur.getParentFile()) {
            // Check 'cur' is project home directory.
            if (!new File(cur, "bin").isDirectory() ||
                !new File(cur, "config").isDirectory())
                continue;

            return cur.getPath();
        }

        return null;
    }

    /**
     * @param cls Class.
     * @param e Exception.
     */
    private static void logResolveFailed(Class cls, Exception e) {
        warn(null, "Failed to resolve IGNITE_HOME automatically for class codebase " +
            "[class=" + cls + (e == null ? "" : ", e=" + e.getMessage()) + ']');
    }

    /**
     * @param path Ignite home. May be {@code null}.
     */
    public static void setIgniteHome(@Nullable String path) {
        GridTuple<String> ggHomeTup = ggHome;

        String ggHome0;

        if (ggHomeTup == null) {
            synchronized (CommonUtils.class) {
                // Double check.
                ggHomeTup = ggHome;

                if (ggHomeTup == null) {
                    if (F.isEmpty(path))
                        System.clearProperty(IGNITE_HOME);
                    else
                        System.setProperty(IGNITE_HOME, path);

                    ggHome = F.t(path);

                    return;
                }
                else
                    ggHome0 = ggHomeTup.get();
            }
        }
        else
            ggHome0 = ggHomeTup.get();

        if (ggHome0 != null && !ggHome0.equals(path)) {
            try {
                Path path0 = new File(ggHome0).toPath();

                Path path1 = new File(path).toPath();

                if (!Files.isSameFile(path0, path1))
                    throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                        "[igniteHome=" + path0 + ", newIgniteHome=" + path1 + ']');
            }
            catch (IOException ignore) {
                // Throw an exception if failed to follow symlinks.
                throw new IgniteException("Failed to set IGNITE_HOME after it has been already resolved " +
                    "[igniteHome=" + ggHome0 + ", newIgniteHome=" + path + ']');
            }
        }
    }

    /**
     * Nullifies Ignite home directory. For test purposes only.
     */
    public static void nullifyHomeDirectory() {
        ggHome = null;
    }

    /**
     * Indicates whether current OS is Linux flavor.
     *
     * @return {@code true} if current OS is Linux - {@code false} otherwise.
     */
    public static boolean isLinux() {
        return linux;
    }

    /**
     * Indicates whether current OS is Mac OS.
     *
     * @return {@code true} if current OS is Mac OS - {@code false} otherwise.
     */
    public static boolean isMacOs() {
        return mac;
    }

    /**
     * Indicates whether current OS is UNIX flavor.
     *
     * @return {@code true} if current OS is UNIX - {@code false} otherwise.
     */
    public static boolean isUnix() {
        return unix;
    }

    /**
     * Indicates whether current OS is Windows.
     *
     * @return {@code true} if current OS is Windows (any versions) - {@code false} otherwise.
     */
    public static boolean isWindows() {
        return win;
    }

    /**
     * Returns URLs of class loader
     *
     * @param clsLdr Class loader.
     */
    public static URL[] classLoaderUrls(ClassLoader clsLdr) {
        if (clsLdr == null)
            return EMPTY_URL_ARR;
        else if (clsLdr instanceof URLClassLoader)
            return ((URLClassLoader)clsLdr).getURLs();
        else if (bltClsLdrCls != null && urlClsLdrField != null && bltClsLdrCls.isAssignableFrom(clsLdr.getClass())) {
            try {
                synchronized (urlClsLdrField) {
                    // Backup accessible field state.
                    boolean accessible = urlClsLdrField.isAccessible();

                    try {
                        if (!accessible)
                            urlClsLdrField.setAccessible(true);

                        Object ucp = urlClsLdrField.get(clsLdr);

                        if (ucp instanceof URLClassLoader)
                            return ((URLClassLoader)ucp).getURLs();
                        else if (clsURLClassPath != null && clsURLClassPath.isInstance(ucp))
                            return (URL[])mthdURLClassPathGetUrls.invoke(ucp);
                        else
                            throw new RuntimeException("Unknown classloader: " + clsLdr.getClass());
                    }
                    finally {
                        // Recover accessible field state.
                        if (!accessible)
                            urlClsLdrField.setAccessible(false);
                    }
                }
            }
            catch (InvocationTargetException | IllegalAccessException e) {
                e.printStackTrace(System.err);

                return EMPTY_URL_ARR;
            }
        }
        else
            return EMPTY_URL_ARR;
    }

    /** */
    @Nullable private static Class defaultClassLoaderClass() {
        try {
            return Class.forName("jdk.internal.loader.BuiltinClassLoader");
        }
        catch (ClassNotFoundException e) {
            return null;
        }
    }

    /** */
    @Nullable private static Field urlClassLoaderField() {
        try {
            Class cls = defaultClassLoaderClass();

            return cls == null ? null : cls.getDeclaredField("ucp");
        }
        catch (NoSuchFieldException e) {
            return null;
        }
    }

    /**
     * Converts exception, but unlike {@link #convertException(IgniteCheckedException)}
     * does not wrap passed in exception if none suitable converter found.
     *
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static Exception convertExceptionNoWrap(IgniteCheckedException e) {
        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (Exception)e.getCause();

        return e;
    }

    /**
     * @param e Ignite checked exception.
     * @return Ignite runtime exception.
     */
    public static IgniteException convertException(IgniteCheckedException e) {
        IgniteClientDisconnectedException e0 = e.getCause(IgniteClientDisconnectedException.class);

        if (e0 != null) {
            assert e0.reconnectFuture() != null : e0;

            throw e0;
        }

        IgniteClientDisconnectedCheckedException disconnectedErr =
            e.getCause(IgniteClientDisconnectedCheckedException.class);

        if (disconnectedErr != null) {
            assert disconnectedErr.reconnectFuture() != null : disconnectedErr;

            e = disconnectedErr;
        }

        C1<IgniteCheckedException, IgniteException> converter = exceptionConverters.get(e.getClass());

        if (converter != null)
            return converter.apply(e);

        if (e.getCause() instanceof IgniteException)
            return (IgniteException)e.getCause();

        return new IgniteException(e.getMessage(), e);
    }

    /**
     * Gets IgniteClosure for an IgniteCheckedException class.
     *
     * @param clazz Class.
     * @return The IgniteClosure mapped to this exception class, or null if none.
     */
    public static C1<IgniteCheckedException, IgniteException> getExceptionConverter(Class<? extends IgniteCheckedException> clazz) {
        return exceptionConverters.get(clazz);
    }

    /**
     * Gets map with converters to convert internal checked exceptions to public API unchecked exceptions.
     *
     * @return Exception converters.
     */
    private static Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>> exceptionConverters() {
        Map<Class<? extends IgniteCheckedException>, C1<IgniteCheckedException, IgniteException>> m = new HashMap<>();

        m.put(IgniteInterruptedCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteInterruptedException(e.getMessage(), (InterruptedException)e.getCause());
            }
        });

        m.put(IgniteFutureCancelledCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureCancelledException(e.getMessage(), e);
            }
        });

        m.put(IgniteFutureTimeoutCheckedException.class, new C1<IgniteCheckedException, IgniteException>() {
            @Override public IgniteException apply(IgniteCheckedException e) {
                return new IgniteFutureTimeoutException(e.getMessage(), e);
            }
        });

        return m;
    }

    /**
     * Calculate a hashCode for an array.
     *
     * @param obj Object.
     */
    public static int hashCode(Object obj) {
        if (obj == null)
            return 0;

        if (obj.getClass().isArray()) {
            if (obj instanceof byte[])
                return Arrays.hashCode((byte[])obj);
            if (obj instanceof short[])
                return Arrays.hashCode((short[])obj);
            if (obj instanceof int[])
                return Arrays.hashCode((int[])obj);
            if (obj instanceof long[])
                return Arrays.hashCode((long[])obj);
            if (obj instanceof float[])
                return Arrays.hashCode((float[])obj);
            if (obj instanceof double[])
                return Arrays.hashCode((double[])obj);
            if (obj instanceof char[])
                return Arrays.hashCode((char[])obj);
            if (obj instanceof boolean[])
                return Arrays.hashCode((boolean[])obj);

            int result = 1;

            for (Object element : (Object[])obj)
                result = 31 * result + hashCode(element);

            return result;
        }
        else
            return obj.hashCode();
    }
}
