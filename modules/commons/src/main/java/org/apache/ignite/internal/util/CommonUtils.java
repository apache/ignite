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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UTFDataFormatException;
import java.lang.annotation.Annotation;
import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCommonsSystemProperties;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

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

    /** Length of numbered file name. */
    public static final int NUMBER_FILE_NAME_LENGTH = 16;

    /** Long date format pattern for log messages. */
    public static final DateTimeFormatter LONG_DATE_FMT =
        DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    /**
     * Short date format pattern for log messages in "quiet" mode.
     * Only time is included since we don't expect "quiet" mode to be used
     * for longer runs.
     */
    public static final DateTimeFormatter SHORT_DATE_FMT =
        DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    /** @see IgniteCommonsSystemProperties#IGNITE_MEMORY_PER_BYTE_COPY_THRESHOLD */
    public static final long DFLT_MEMORY_PER_BYTE_COPY_THRESHOLD = 0L;

    /** Ignite package. */
    public static final String IGNITE_PKG = "org.apache.ignite.";

    /** Empty integers array. */
    public static final int[] EMPTY_INTS = new int[0];

    /** Empty longs array. */
    public static final long[] EMPTY_LONGS = new long[0];

    /** */
    public static final long KB = 1024L;

    /** */
    public static final long MB = 1024L * 1024;

    /** */
    public static final long GB = 1024L * 1024 * 1024;

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

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = new HashMap<>(16, .5f);

    /** Boxed class map. */
    private static final Map<Class<?>, Class<?>> boxedClsMap = new HashMap<>(16, .5f);

    /** Class loader used to load Ignite. */
    private static final ClassLoader gridClassLoader = CommonUtils.class.getClassLoader();

    /** Default buffer size = 4K. */
    private static final int BUF_SIZE = 4096;

    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** Alphanumeric with underscore regexp pattern. */
    private static final Pattern ALPHANUMERIC_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z_0-9]+$");

    /** OS string. */
    private static final String osStr;

    /** JDK string. */
    private static String jdkStr;

    /** Indicates whether current OS is some version of Windows. */
    private static boolean win;

    /** Indicates whether current OS is UNIX flavor. */
    private static boolean unix;

    /** Indicates whether current OS is Linux flavor. */
    private static boolean linux;

    /** Indicates whether current OS is Mac OS. */
    private static boolean mac;

    /** Name of the JDK. */
    private static String jdkName;

    /** Version of the JDK. */
    private static String jdkVer;

    /** Name of the JVM implementation. */
    private static String jvmImplName;

    /** Will be set to {@code true} if detected a 32-bit JVM. */
    private static final boolean jvm32Bit;

    /** */
    private static final boolean assertionsEnabled;

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

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class>> classCache =
        new ConcurrentHashMap<>();

    static {
        boolean assertionsEnabled0 = true;

        try {
            assert false;

            assertionsEnabled0 = false;
        }
        catch (AssertionError ignored) {
            assertionsEnabled0 = true;
        }
        finally {
            assertionsEnabled = assertionsEnabled0;
        }

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

        String osArch = System.getProperty("os.arch");

        String javaRtName = System.getProperty("java.runtime.name");
        String javaRtVer = System.getProperty("java.runtime.version");
        String jdkName = System.getProperty("java.specification.name");
        String jdkVer = System.getProperty("java.specification.version");
        String osVer = System.getProperty("os.version");
        String jvmImplVer = System.getProperty("java.vm.version");
        String jvmImplVendor = System.getProperty("java.vm.vendor");
        String jvmImplName = System.getProperty("java.vm.name");

        // Best effort to detect a 32-bit JVM.
        String jvmArchDataModel = System.getProperty("sun.arch.data.model");

        String jdkStr = javaRtName + ' ' + javaRtVer + ' ' + jvmImplVendor + ' ' + jvmImplName + ' ' +
            jvmImplVer;

        osStr = osName + ' ' + osVer + ' ' + osArch;

        // Copy auto variables to static ones.
        CommonUtils.jdkName = jdkName;
        CommonUtils.jdkVer = jdkVer;
        CommonUtils.jdkStr = jdkStr;
        CommonUtils.jvmImplName = jvmImplName;

        jvm32Bit = "32".equals(jvmArchDataModel);

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

        primitiveMap.put("byte", byte.class);
        primitiveMap.put("short", short.class);
        primitiveMap.put("int", int.class);
        primitiveMap.put("long", long.class);
        primitiveMap.put("float", float.class);
        primitiveMap.put("double", double.class);
        primitiveMap.put("char", char.class);
        primitiveMap.put("boolean", boolean.class);
        primitiveMap.put("void", void.class);

        boxedClsMap.put(byte.class, Byte.class);
        boxedClsMap.put(short.class, Short.class);
        boxedClsMap.put(int.class, Integer.class);
        boxedClsMap.put(long.class, Long.class);
        boxedClsMap.put(float.class, Float.class);
        boxedClsMap.put(double.class, Double.class);
        boxedClsMap.put(char.class, Character.class);
        boxedClsMap.put(boolean.class, Boolean.class);
        boxedClsMap.put(void.class, Void.class);
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
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class name.
     * @return Instance.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(String cls) throws IgniteCheckedException {
        Class<?> cls0;

        try {
            cls0 = Class.forName(cls);
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        return (T)newInstance(cls0);
    }

    /**
     * Creates new instance of a class only if it has an empty constructor (can be non-public).
     *
     * @param cls Class to instantiate.
     * @return New instance of the class or {@code null} if empty constructor could not be assigned.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public static <T> T newInstance(Class<T> cls) throws IgniteCheckedException {
        boolean set = false;

        Constructor<T> ctor = null;

        try {
            ctor = cls.getDeclaredConstructor();

            if (ctor == null)
                return null;

            if (!ctor.isAccessible()) {
                ctor.setAccessible(true);

                set = true;
            }

            return ctor.newInstance();
        }
        catch (NoSuchMethodException e) {
            throw new IgniteCheckedException("Failed to find empty constructor for class: " + cls, e);
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new IgniteCheckedException("Failed to create new instance for class: " + cls, e);
        }
        finally {
            if (ctor != null && set)
                ctor.setAccessible(false);
        }
    }

    /**
     * Check whether class is in classpath.
     *
     * @return {@code True} if in classpath.
     */
    public static boolean inClassPath(String clsName) {
        try {
            Class.forName(clsName);

            return true;
        }
        catch (ClassNotFoundException ignore) {
            return false;
        }
    }

    /**
     * Pretty-formatting for minutes.
     *
     * @param mins Minutes to format.
     * @return Formatted presentation of minutes.
     */
    public static String formatMins(long mins) {
        assert mins >= 0;

        if (mins == 0)
            return "< 1 min";

        SB sb = new SB();

        long dd = mins / 1440; // 1440 mins = 60 mins * 24 hours

        if (dd > 0)
            sb.a(dd).a(dd == 1 ? " day " : " days ");

        mins %= 1440;

        long hh = mins / 60;

        if (hh > 0)
            sb.a(hh).a(hh == 1 ? " hour " : " hours ");

        mins %= 60;

        if (mins > 0)
            sb.a(mins).a(mins == 1 ? " min " : " mins ");

        return sb.toString().trim();
    }

    /**
     * Gets 8-character substring of UUID (for terse logging).
     *
     * @param id Input ID.
     * @return 8-character ID substring.
     */
    public static String id8(UUID id) {
        return id.toString().substring(0, 8);
    }

    /**
     * Writes array to output stream.
     *
     * @param out Output stream.
     * @param arr Array to write.
     * @param <T> Array type.
     * @throws IOException If failed.
     */
    public static <T> void writeArray(ObjectOutput out, T[] arr) throws IOException {
        int len = arr == null ? 0 : arr.length;

        out.writeInt(len);

        if (arr != null && arr.length > 0)
            for (T t : arr)
                out.writeObject(t);
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Object[] readArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Object[] arr = null;

        if (len > 0) {
            arr = new Object[len];

            for (int i = 0; i < len; i++)
                arr[i] = in.readObject();
        }

        return arr;
    }

    /**
     * Reads typed array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static <T> T[] readArray(ObjectInput in, Class<T> cls) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        T[] arr = null;

        if (len > 0) {
            arr = (T[])Array.newInstance(cls, len);

            for (int i = 0; i < len; i++)
                arr[i] = (T)in.readObject();
        }

        return arr;
    }

    /**
     * Reads array from input stream.
     *
     * @param in Input stream.
     * @return Deserialized array.
     * @throws IOException If failed.
     * @throws ClassNotFoundException If class not found.
     */
    @Nullable public static Class<?>[] readClassArray(ObjectInput in) throws IOException, ClassNotFoundException {
        int len = in.readInt();

        Class<?>[] arr = null;

        if (len > 0) {
            arr = new Class<?>[len];

            for (int i = 0; i < len; i++)
                arr[i] = (Class<?>)in.readObject();
        }

        return arr;
    }

    /**
     *
     * @param out Output.
     * @param col Set to write.
     * @throws IOException If write failed.
     */
    public static void writeCollection(ObjectOutput out, Collection<?> col) throws IOException {
        if (col != null) {
            out.writeInt(col.size());

            for (Object o : col)
                out.writeObject(o);
        }
        else
            out.writeInt(-1);
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> Collection<E> readCollection(ObjectInput in)
        throws IOException, ClassNotFoundException {
        return readList(in);
    }

    /**
     *
     * @param m Map to seal.
     * @param <K> Key type.
     * @param <V> Value type
     * @return Sealed map.
     */
    public static <K, V> Map<K, V> sealMap(Map<K, V> m) {
        assert m != null;

        return Collections.unmodifiableMap(new HashMap<>(m));
    }

    /**
     * Seal collection.
     *
     * @param c Collection to seal.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(Collection<E> c) {
        return Collections.unmodifiableList(new ArrayList<>(c));
    }

    /**
     * Convert array to seal list.
     *
     * @param a Array for convert to seal list.
     * @param <E> Entry type
     * @return Sealed collection.
     */
    public static <E> List<E> sealList(E... a) {
        return Collections.unmodifiableList(Arrays.asList(a));
    }

    /**
     * Replace password in URI string with a single '*' character.
     * <p>
     * Parses given URI by applying &quot;.*://(.*:.*)@.*&quot;
     * regular expression pattern and than if URI matches it
     * replaces password strings between '/' and '@' with '*'.
     *
     * @param uri URI which password should be replaced.
     * @return Converted URI string
     */
    @Nullable public static String hidePassword(@Nullable String uri) {
        if (uri == null)
            return null;

        if (Pattern.matches(".*://(.*:.*)@.*", uri)) {
            int userInfoLastIdx = uri.indexOf('@');

            assert userInfoLastIdx != -1;

            String str = uri.substring(0, userInfoLastIdx);

            int userInfoStartIdx = str.lastIndexOf('/');

            str = str.substring(userInfoStartIdx + 1);

            String[] params = str.split(";");

            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < params.length; i++) {
                int idx;

                if ((idx = params[i].indexOf(':')) != -1)
                    params[i] = params[i].substring(0, idx + 1) + '*';

                builder.append(params[i]);

                if (i != params.length - 1)
                    builder.append(';');
            }

            return new StringBuilder(uri).replace(userInfoStartIdx + 1, userInfoLastIdx,
                builder.toString()).toString();
        }

        return uri;
    }

    /**
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader gridClassLoader() {
        return gridClassLoader;
    }


    /**
     * @param parent Parent to find.
     * @param ldr Loader to check.
     * @return {@code True} if parent found.
     */
    public static boolean hasParent(@Nullable ClassLoader parent, ClassLoader ldr) {
        if (parent != null) {
            for (; ldr != null; ldr = ldr.getParent()) {
                if (ldr.equals(parent))
                    return true;
            }

            return false;
        }

        return true;
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            out.write(arr);
        }
    }

    /**
     * Writes byte array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws java.io.IOException If write failed.
     */
    public static void writeByteArray(DataOutput out, @Nullable byte[] arr, int maxLen) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            int len = Math.min(arr.length, maxLen);

            out.writeInt(len);

            out.write(arr, 0, len);
        }
    }

    /**
     * Reads byte array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws java.io.IOException If read failed.
     */
    @Nullable public static byte[] readByteArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        byte[] res = new byte[len];

        in.readFully(res);

        return res;
    }

    /**
     * Join byte arrays into single one.
     *
     * @param bufs list of byte arrays to concatenate.
     * @return Concatenated byte's array.
     */
    public static byte[] join(byte[]... bufs) {
        int size = 0;
        for (byte[] buf : bufs) {
            size += buf.length;
        }

        byte[] res = new byte[size];
        int position = 0;
        for (byte[] buf : bufs) {
            arrayCopy(buf, 0, res, position, buf.length);
            position += buf.length;
        }

        return res;
    }

    /**
     * Convert string with hex values to byte array.
     *
     * @param hex Hexadecimal string to convert.
     * @return array of bytes defined as hex in string.
     * @throws IllegalArgumentException If input character differs from certain hex characters.
     */
    public static byte[] hexString2ByteArray(String hex) throws IllegalArgumentException {
        // If Hex string has odd character length.
        if (hex.length() % 2 != 0)
            hex = '0' + hex;

        char[] chars = hex.toCharArray();

        byte[] bytes = new byte[chars.length / 2];

        int byteCnt = 0;

        for (int i = 0; i < chars.length; i += 2) {
            int newByte = 0;

            newByte |= hexCharToByte(chars[i]);

            newByte <<= 4;

            newByte |= hexCharToByte(chars[i + 1]);

            bytes[byteCnt] = (byte)newByte;

            byteCnt++;
        }

        return bytes;
    }

    /**
     * Gets a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexLong(long val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Gets a hex string representation of the given int value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    public static String hexInt(int val) {
        return new SB().appendHex(val).toString();
    }

    /**
     * Return byte value for certain character.
     *
     * @param ch Character
     * @return Byte value.
     * @throws IllegalArgumentException If input character differ from certain hex characters.
     */
    private static byte hexCharToByte(char ch) throws IllegalArgumentException {
        switch (ch) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return (byte)(ch - '0');

            case 'a':
            case 'A':
                return 0xa;

            case 'b':
            case 'B':
                return 0xb;

            case 'c':
            case 'C':
                return 0xc;

            case 'd':
            case 'D':
                return 0xd;

            case 'e':
            case 'E':
                return 0xe;

            case 'f':
            case 'F':
                return 0xf;

            default:
                throw new IllegalArgumentException("Hex decoding wrong input character [character=" + ch + ']');
        }
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return toBytes(l, new byte[8], 0, 8);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array.
     *
     * @param l Long value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int longToBytes(long l, byte[] bytes, int off) {
        toBytes(l, bytes, off, 8);

        return off + 8;
    }

    /**
     * Converts primitive {@code int} type to byte array.
     *
     * @param i Integer value.
     * @return Array of bytes.
     */
    public static byte[] intToBytes(int i) {
        return toBytes(i, new byte[4], 0, 4);
    }

    /**
     * Converts primitive {@code int} type to byte array and stores it in specified
     * byte array.
     *
     * @param i Integer value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int intToBytes(int i, byte[] bytes, int off) {
        toBytes(i, bytes, off, 4);

        return off + 4;
    }

    /**
     * Converts primitive {@code short} type to byte array.
     *
     * @param s Short value.
     * @return Array of bytes.
     */
    public static byte[] shortToBytes(short s) {
        return toBytes(s, new byte[2], 0, 2);
    }

    /**
     * Converts primitive {@code short} type to byte array and stores it in specified
     * byte array.
     *
     * @param s Short value.
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    public static int shortToBytes(short s, byte[] bytes, int off) {
        toBytes(s, bytes, off, 2);

        return off + 2;
    }

    /**
     * Converts an UUID to byte array.
     *
     * @param uuid UUID value.
     * @return Encoded into byte array {@link java.util.UUID}.
     */
    public static byte[] uuidToBytes(@Nullable UUID uuid) {
        byte[] bytes = new byte[16];

        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, bytes.length);

        buf.order(ByteOrder.BIG_ENDIAN);

        if (uuid != null) {
            buf.putLong(uuid.getMostSignificantBits());
            buf.putLong(uuid.getLeastSignificantBits());
        }
        else {
            buf.putLong(0);
            buf.putLong(0);
        }

        return bytes;
    }

    /**
     * Constructs {@code short} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Short value.
     */
    public static short bytesToShort(byte[] bytes, int off) {
        return (short)fromBytes(bytes, off, 2);
    }

    /**
     * Constructs {@code int} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Integer value.
     */
    public static int bytesToInt(byte[] bytes, int off) {
        return (int)fromBytes(bytes, off, 4);
    }

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        return fromBytes(bytes, off, 8);
    }

    /**
     * Constructs {@link UUID} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return UUID value.
     */
    public static UUID bytesToUuid(byte[] bytes, int off) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, 16);

        buf.order(ByteOrder.BIG_ENDIAN);

        long most = buf.getLong();
        long least = buf.getLong();

        return most != 0 && least != 0 ? new UUID(most, least) : null;
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array. The highest byte in the value is the first byte in result array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @param limit Limit of bytes to write into output.
     * @return Array of bytes.
     */
    private static byte[] toBytes(long l, byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        for (int i = limit - 1; i >= 0; i--) {
            bytes[off + i] = (byte)(l & 0xFF);
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Constructs {@code long} from byte array. The first byte in array is the highest byte in result.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @param limit Amount of bytes to use in the source array.
     * @return Long value.
     */
    private static long fromBytes(byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;

        int bytesCnt = Math.min(bytes.length - off, limit);

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            res <<= 8;
            res |= bytes[off + i] & 0xFF;
        }

        return res;
    }

    /**
     * Compares fragments of byte arrays.
     *
     * @param a First array.
     * @param aOff First array offset.
     * @param b Second array.
     * @param bOff Second array offset.
     * @param len Length of fragments.
     * @return {@code true} if fragments are equal, {@code false} otherwise.
     */
    public static boolean bytesEqual(byte[] a, int aOff, byte[] b, int bOff, int len) {
        if (aOff + len > a.length || bOff + len > b.length)
            return false;
        else {
            for (int i = 0; i < len; i++)
                if (a[aOff + i] != b[bOff + i])
                    return false;

            return true;
        }
    }

    /**
     * Converts an array of characters representing hexidecimal values into an
     * array of bytes of those same values. The returned array will be half the
     * length of the passed array, as it takes two characters to represent any
     * given byte. An exception is thrown if the passed char array has an odd
     * number of elements.
     *
     * @param data An array of characters containing hexidecimal digits
     * @return A byte array containing binary data decoded from
     *         the supplied char array.
     * @throws IgniteCheckedException Thrown if an odd number or illegal of characters is supplied.
     */
    public static byte[] decodeHex(char[] data) throws IgniteCheckedException {
        int len = data.length;

        if ((len & 0x01) != 0)
            throw new IgniteCheckedException("Odd number of characters.");

        byte[] out = new byte[len >> 1];

        // Two characters form the hex value.
        for (int i = 0, j = 0; j < len; i++) {
            int f = toDigit(data[j], j) << 4;

            j++;

            f |= toDigit(data[j], j);

            j++;

            out[i] = (byte)(f & 0xFF);
        }

        return out;
    }

    /**
     * @param bytes Number of bytes to display.
     * @param si If {@code true}, then unit base is 1000, otherwise unit base is 1024.
     * @return Formatted size.
     */
    public static String readableSize(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;

        if (bytes < unit)
            return bytes + " B";

        int exp = (int)(Math.log(bytes) / Math.log(unit));

        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Generates file name from index.
     *
     * @param num Number to generate file name.
     * @param ext Optional extension
     * @return File name.
     */
    public static String fixedLengthNumberName(long num, @Nullable String ext) {
        SB b = new SB();

        String segmentStr = Long.toString(num);

        for (int i = segmentStr.length(); i < NUMBER_FILE_NAME_LENGTH; i++)
            b.a('0');

        b.a(segmentStr);

        if (ext != null)
            b.a(ext);

        return b.toString();
    }

    /**
     * @param fileName File name.
     * @return Number of this file.
     */
    public static long fixedLengthFileNumber(String fileName) {
        return Long.parseLong(fileName.substring(0, NUMBER_FILE_NAME_LENGTH));
    }

    /**
     * @param ext Optional extension.
     * @return Pattern to match numbered file name with the specific extension.
     */
    public static Pattern fixedLengthNumberNamePattern(@Nullable String ext) {
        String pattern = "\\d{" + NUMBER_FILE_NAME_LENGTH + "}";

        if (ext != null)
            pattern += ext.replaceAll("\\.", "\\\\\\.");

        return Pattern.compile(pattern);
    }

    /**
     * Makes a {@code '+---+'} dash line.
     *
     * @param len Length of the dash line to make.
     * @return Dash line.
     */
    public static String dash(int len) {
        char[] dash = new char[len];

        Arrays.fill(dash, '-');

        dash[0] = dash[len - 1] = '+';

        return new String(dash);
    }

    /**
     * Formats system time in milliseconds for printing in logs.
     *
     * @param sysTime System time.
     * @return Formatted time string.
     */
    public static String format(long sysTime) {
        return LONG_DATE_FMT.format(Instant.ofEpochMilli(sysTime));
    }

    /**
     * Converts enumeration to iterable so it can be used in {@code foreach} construct.
     *
     * @param <T> Types of instances for iteration.
     * @param e Enumeration to convert.
     * @return Iterable over the given enumeration.
     */
    public static <T> Iterable<T> asIterable(final Enumeration<T> e) {
        return new Iterable<T>() {
            @Override public Iterator<T> iterator() {
                return new Iterator<T>() {
                    @Override public boolean hasNext() {
                        return e.hasMoreElements();
                    }

                    @Override public T next() {
                        return e.nextElement();
                    }

                    @Override public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Copy source file (or folder) to destination file (or folder). Supported source & destination:
     * <ul>
     * <li>File to File</li>
     * <li>File to Folder</li>
     * <li>Folder to Folder (Copy the content of the directory and not the directory itself)</li>
     * </ul>
     *
     * @param src Source file or folder.
     * @param dest Destination file or folder.
     * @param overwrite Whether or not overwrite existing files and folders.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void copy(File src, File dest, boolean overwrite) throws IOException {
        assert src != null;
        assert dest != null;

        /*
         * Supported source & destination:
         * ===============================
         * 1. File -> File
         * 2. File -> Directory
         * 3. Directory -> Directory
         */

        // Source must exist.
        if (!src.exists())
            throw new FileNotFoundException("Source can't be found: " + src);

        // Check that source and destination are not the same.
        if (src.getAbsoluteFile().equals(dest.getAbsoluteFile()))
            throw new IOException("Source and destination are the same [src=" + src + ", dest=" + dest + ']');

        if (dest.exists()) {
            if (!dest.isDirectory() && !overwrite)
                throw new IOException("Destination already exists: " + dest);

            if (!dest.canWrite())
                throw new IOException("Destination is not writable:" + dest);
        }
        else {
            File parent = dest.getParentFile();

            if (parent != null && !parent.exists())
                // Ignore any errors here.
                // We will get errors when we'll try to open the file stream.
                //noinspection ResultOfMethodCallIgnored
                parent.mkdirs();

            // If source is a directory, we should create destination directory.
            if (src.isDirectory())
                //noinspection ResultOfMethodCallIgnored
                dest.mkdir();
        }

        if (src.isDirectory()) {
            // In this case we have Directory -> Directory.
            // Note that we copy the content of the directory and not the directory itself.

            File[] files = src.listFiles();

            for (File file : files) {
                if (file.isDirectory()) {
                    File dir = new File(dest, file.getName());

                    if (!dir.exists() && !dir.mkdirs())
                        throw new IOException("Can't create directory: " + dir);

                    copy(file, dir, overwrite);
                }
                else
                    copy(file, dest, overwrite);
            }
        }
        else {
            // In this case we have File -> File or File -> Directory.
            File file = dest.exists() && dest.isDirectory() ? new File(dest, src.getName()) : dest;

            if (!overwrite && file.exists())
                throw new IOException("Destination already exists: " + file);

            FileInputStream in = null;
            FileOutputStream out = null;

            try {
                in = new FileInputStream(src);
                out = new FileOutputStream(file);

                copy(in, out);
            }
            finally {
                if (in != null)
                    in.close();

                if (out != null) {
                    out.getFD().sync();

                    out.close();
                }
            }
        }
    }

    /**
     * Copies input byte stream to output byte stream.
     *
     * @param in Input byte stream.
     * @param out Output byte stream.
     * @return Number of the copied bytes.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        assert in != null;
        assert out != null;

        byte[] buf = new byte[BUF_SIZE];

        int cnt = 0;

        for (int n; (n = in.read(buf)) > 0; ) {
            out.write(buf, 0, n);

            cnt += n;
        }

        return cnt;
    }

    /**
     * Reads file to string using specified charset.
     *
     * @param fileName File name.
     * @param charset File charset.
     * @return File content.
     * @throws IOException If error occurred.
     */
    public static String readFileToString(String fileName, String charset) throws IOException {
        try (Reader input = new InputStreamReader(new FileInputStream(fileName), charset)) {
            StringWriter output = new StringWriter();

            char[] buf = new char[4096];

            int n;

            while ((n = input.read(buf)) != -1)
                output.write(buf, 0, n);

            return output.toString();
        }
    }

    /**
     * Writes string to file.
     *
     * @param file File.
     * @param s String to write.
     * @param charset Encoding.
     * @param append If {@code true}, then specified string will be added to the end of the file.
     * @throws IOException Thrown if an I/O error occurs.
     */
    public static void writeStringToFile(File file, String s, String charset, boolean append) throws IOException {
        if (s == null)
            return;

        try (OutputStream out = new FileOutputStream(file, append)) {
            out.write(s.getBytes(charset));
        }
    }

    /**
     * Utility method that sets cause into exception and returns it.
     *
     * @param e Exception to set cause to and return.
     * @param cause Optional cause to set (if not {@code null}).
     * @param <E> Type of the exception.
     * @return Passed in exception with optionally set cause.
     */
    public static <E extends Throwable> E withCause(E e, @Nullable Throwable cause) {
        assert e != null;

        if (cause != null)
            e.initCause(cause);

        return e;
    }

    /**
     * Deletes file or directory with all sub-directories and files. Not thread-safe.
     *
     * @param file File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(@Nullable File file) {
        return file != null && delete(file.toPath());
    }

    /**
     * Converts size in bytes to human-readable size in megabytes.
     *
     * @param sizeInBytes Size of any object (file, memory region etc) in bytes.
     * @return Size converted to megabytes.
     */
    public static int sizeInMegabytes(long sizeInBytes) {
        return (int)(sizeInBytes / MB);
    }

    /**
     * Deletes file or directory with all sub-directories and files. Not thread-safe.
     *
     * @param path File or directory to delete.
     * @return {@code true} if and only if the file or directory is successfully deleted,
     *      {@code false} otherwise
     */
    public static boolean delete(Path path) {
        if (Files.isDirectory(path)) {
            try {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                    for (Path innerPath : stream) {
                        boolean res = delete(innerPath);

                        if (!res)
                            return false;
                    }
                }
            }
            catch (IOException e) {
                return false;
            }
        }

        if (path.toFile().getName().endsWith("jar")) {
            try {
                // Why do we do this?
                new JarFile(path.toString(), false).close();
            }
            catch (IOException ignore) {
                // Ignore it here...
            }
        }

        try {
            Files.delete(path);

            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /**
     * @param dir Directory to create along with all non-existent parent directories.
     * @return {@code True} if directory exists (has been created or already existed),
     *      {@code false} if has not been created and does not exist.
     */
    public static boolean mkdirs(File dir) {
        assert dir != null;

        return dir.mkdirs() || dir.exists();
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String byteArray2HexString(byte[] arr) {
        return byteArray2HexString(arr, true);
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @param toUpper If {@code true} returns upper cased result.
     * @return Hex string.
     */
    public static String byteArray2HexString(byte[] arr, boolean toUpper) {
        StringBuilder sb = new StringBuilder(arr.length << 1);

        for (byte b : arr)
            addByteAsHex(sb, b);

        return toUpper ? sb.toString().toUpperCase() : sb.toString();
    }

    /**
     * @param sb String builder.
     * @param b Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
    }

    /**
     * Checks for containment of the value in the array.
     * Both array cells and value may be {@code null}. Two {@code null}s are considered equal.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @param vals Additional values.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsObjectArray(@Nullable Object[] arr, Object val, @Nullable Object... vals) {
        if (arr == null || arr.length == 0)
            return false;

        for (Object o : arr) {
            if (Objects.equals(o, val))
                return true;

            if (vals != null && vals.length > 0)
                for (Object v : vals)
                    if (Objects.equals(o, v))
                        return true;
        }

        return false;
    }

    /**
     * Checks for containment of the value in the array.
     *
     * @param arr Array of objects.
     * @param val Value to check for containment inside of array.
     * @return {@code true} if contains object, {@code false} otherwise.
     */
    public static boolean containsIntArray(int[] arr, int val) {
        assert arr != null;

        if (arr.length == 0)
            return false;

        for (int i : arr)
            if (i == val)
                return true;

        return false;
    }

    /**
     * Checks for containment of given string value in the specified array.
     * Array's cells and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param arr Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringArray(String[] arr, @Nullable String val, boolean ignoreCase) {
        assert arr != null;

        for (String s : arr) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Checks for containment of given string value in the specified collection.
     * Collection elements and string value can be {@code null}. Tow {@code null}s are considered equal.
     *
     * @param c Array of strings.
     * @param val Value to check for containment inside of array.
     * @param ignoreCase Ignoring case if {@code true}.
     * @return {@code true} if contains string, {@code false} otherwise.
     */
    public static boolean containsStringCollection(Iterable<String> c, @Nullable String val, boolean ignoreCase) {
        assert c != null;

        for (String s : c) {
            // If both are nulls, then they are equal.
            if (s == null && val == null)
                return true;

            // Only one is null and the other one isn't.
            if (s == null || val == null)
                continue;

            // Both are not nulls.
            if (ignoreCase) {
                if (s.equalsIgnoreCase(val))
                    return true;
            }
            else if (s.equals(val))
                return true;
        }

        return false;
    }

    /**
     * Closes given resource suppressing possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     * @param e Suppressor exception
     */
    public static void closeWithSuppressingException(@Nullable AutoCloseable rsrc, @NotNull Exception e) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
    }

    /**
     * Quietly closes given resource ignoring possible checked exception.
     *
     * @param rsrc Resource to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable AutoCloseable rsrc) {
        if (rsrc != null)
            try {
                rsrc.close();
            }
            catch (Exception ignored) {
                // No-op.
            }
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
     * Quietly closes given {@link Socket} ignoring possible checked exception.
     *
     * @param sock Socket to close. If it's {@code null} - it's no-op.
     */
    public static void closeQuiet(@Nullable Socket sock) {
        if (sock == null)
            return;

        try {
            // Avoid tls 1.3 incompatibility https://bugs.openjdk.java.net/browse/JDK-8208526
            sock.shutdownOutput();
            sock.shutdownInput();
        }
        catch (Exception ignored) {
            // No-op.
        }

        try {
            sock.close();
        }
        catch (Exception ignored) {
            // No-op.
        }
    }

    /**
     * Quietly releases file lock ignoring all possible exceptions.
     *
     * @param lock File lock. If it's {@code null} - it's no-op.
     */
    public static void releaseQuiet(@Nullable FileLock lock) {
        if (lock != null)
            try {
                lock.release();
            }
            catch (Exception ignored) {
                // No-op.
            }
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
     * Quietly rollbacks JDBC connection ignoring possible checked exception.
     *
     * @param rsrc JDBC connection to rollback. If connection is {@code null}, it's no-op.
     */
    public static void rollbackConnectionQuiet(@Nullable Connection rsrc) {
        if (rsrc != null)
            try {
                rsrc.rollback();
            }
            catch (SQLException ignored) {
                // No-op.
            }
    }

    /**
     * Mask component name to make sure that it is not {@code null}.
     *
     * @param name Component name to mask, possibly {@code null}.
     * @return Component name.
     */
    public static String maskName(@Nullable String name) {
        return name == null ? "default" : name;
    }

    /**
     * @param s String to check.
     * @return {@code true} if given string contains only alphanumeric and underscore symbols.
     */
    public static boolean alphanumericUnderscore(String s) {
        return ALPHANUMERIC_UNDERSCORE_PATTERN.matcher(s).matches();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param t Thread to interrupt.
     */
    public static void interrupt(@Nullable Thread t) {
        if (t != null)
            t.interrupt();
    }

    /**
     * Convenience method that interrupts a given thread if it's not {@code null}.
     *
     * @param workers Threads to interrupt.
     */
    public static void interrupt(Iterable<? extends Thread> workers) {
        if (workers != null)
            for (Thread worker : workers)
                worker.interrupt();
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
     * Writes int array to output stream accounting for <tt>null</tt> values.
     *
     * @param out Output stream to write to.
     * @param arr Array to write, possibly <tt>null</tt>.
     * @throws IOException If write failed.
     */
    public static void writeIntArray(DataOutput out, @Nullable int[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (int b : arr)
                out.writeInt(b);
        }
    }

    /**
     * Writes long array to output stream.
     *
     * @param out Output stream to write to.
     * @param arr Array to write.
     * @throws IOException If write failed.
     */
    public static void writeLongArray(DataOutput out, @Nullable long[] arr) throws IOException {
        if (arr == null)
            out.writeInt(-1);
        else {
            out.writeInt(arr.length);

            for (long b : arr)
                out.writeLong(b);
        }
    }

    /**
     * Reads int array from input stream accounting for <tt>null</tt> values.
     *
     * @param in Stream to read from.
     * @return Read byte array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static int[] readIntArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        int[] res = new int[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readInt();

        return res;
    }

    /**
     * Reads long array from input stream.
     *
     * @param in Stream to read from.
     * @return Read long array, possibly <tt>null</tt>.
     * @throws IOException If read failed.
     */
    @Nullable public static long[] readLongArray(DataInput in) throws IOException {
        int len = in.readInt();

        if (len == -1)
            return null; // Value "-1" indicates null.

        long[] res = new long[len];

        for (int i = 0; i < len; i++)
            res[i] = in.readLong();

        return res;
    }

    /**
     * @param out Output.
     * @param map Map to write.
     * @throws IOException If write failed.
     */
    public static void writeMap(ObjectOutput out, Map<?, ?> map) throws IOException {
        if (map != null) {
            out.writeInt(map.size());

            for (Map.Entry<?, ?> e : map.entrySet()) {
                out.writeObject(e.getKey());
                out.writeObject(e.getValue());
            }
        }
        else
            out.writeInt(-1);
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> Map<K, V> readMap(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        Map<K, V> map = new HashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
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

    /**
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> TreeMap<K, V> readTreeMap(
        ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        if (size == -1)
            return null;

        TreeMap<K, V> map = new TreeMap<>();

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * Read hash map.
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> HashMap<K, V> readHashMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        HashMap<K, V> map = newHashMap(size);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     *
     * @param in Input.
     * @return Read map.
     * @throws IOException If de-serialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <K, V> LinkedHashMap<K, V> readLinkedMap(ObjectInput in)
        throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        LinkedHashMap<K, V> map = new LinkedHashMap<>(size, 1.0f);

        for (int i = 0; i < size; i++)
            map.put((K)in.readObject(), (V)in.readObject());

        return map;
    }

    /**
     * @param in Input.
     * @return Deserialized list.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @Nullable public static <E> List<E> readList(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        List<E> col = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            col.add((E)in.readObject());

        return col;
    }

    /**
     * @param in Input.
     * @return Deserialized set.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialized class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public static <E> Set<E> readSet(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        // Check null flag.
        if (size == -1)
            return null;

        Set<E> set = new HashSet(size, 1.0f);

        for (int i = 0; i < size; i++)
            set.add((E)in.readObject());

        return set;
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
     * Writes enum to output stream accounting for {@code null} values.
     * Note: method writes only one byte for every enum. Therefore, this method
     * only for Enums with maximum count of values equals to 128.
     *
     * @param out Output stream to write to.
     * @param e Enum value to write, possibly {@code null}.
     * @throws IOException If write failed.
     */
    public static <E extends Enum<E>> void writeEnum(DataOutput out, E e) throws IOException {
        out.writeByte(e == null ? -1 : e.ordinal());
    }

    /** */
    public static <E extends Enum<E>> E readEnum(DataInput in, Class<E> enumCls) throws IOException {
        byte ordinal = in.readByte();

        if (ordinal == (byte)-1)
            return null;

        int idx = ordinal & 0xFF;

        E[] values = enumCls.getEnumConstants();

        return idx < values.length ? values[idx] : null;
    }

    /**
     * Gets annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        T ann = cls.getAnnotation(annCls);

        if (ann != null)
            return ann;

        for (Class<?> itf : cls.getInterfaces()) {
            ann = getAnnotation(itf, annCls); // Recursion.

            if (ann != null)
                return ann;
        }

        if (!cls.isInterface()) {
            ann = getAnnotation(cls.getSuperclass(), annCls);

            if (ann != null)
                return ann;
        }

        return null;
    }

    /**
     * Gets declared annotation for a class.
     *
     * @param <T> Type of annotation to return.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return Instance of annotation, or {@code null} if not found.
     */
    @Nullable public static <T extends Annotation> T getDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        if (cls == Object.class)
            return null;

        return cls.getDeclaredAnnotation(annCls);
    }

    /**
     * Indicates if class has given declared annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Class<?> cls, Class<T> annCls) {
        return getDeclaredAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasDeclaredAnnotation(Object o, Class<T> annCls) {
        return o != null && hasDeclaredAnnotation(o.getClass(), annCls);
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param <T> Annotation type.
     * @param cls Class to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Class<?> cls, Class<T> annCls) {
        return getAnnotation(cls, annCls) != null;
    }

    /**
     * Indicates if class has given annotation.
     *
     * @param o Object to get annotation from.
     * @param annCls Annotation to get.
     * @return {@code true} if class has annotation or {@code false} otherwise.
     */
    public static <T extends Annotation> boolean hasAnnotation(Object o, Class<T> annCls) {
        return o != null && hasAnnotation(o.getClass(), annCls);
    }

    /**
     * Provides all interfaces of {@code cls} including inherited ones. Excludes duplicated ones in case of multiple
     * inheritance.
     *
     * @param cls Class to search for interfaces.
     * @return Collection of interfaces of {@code cls}.
     */
    public static Collection<Class<?>> allInterfaces(Class<?> cls) {
        Set<Class<?>> interfaces = new HashSet<>();

        while (cls != null) {
            interfaces.addAll(Arrays.asList(cls.getInterfaces()));

            cls = cls.getSuperclass();
        }

        return interfaces;
    }

    /**
     * Gets simple class name taking care of empty names.
     *
     * @param cls Class to get the name for.
     * @return Simple class name.
     */
    public static String getSimpleName(Class<?> cls) {
        String name = cls.getSimpleName();

        if (F.isEmpty(name))
            name = cls.getName().substring(cls.getPackage().getName().length() + 1);

        return name;
    }

    /**
     * Checks if the map passed in is contained in base map.
     *
     * @param base Base map.
     * @param map Map to check.
     * @return {@code True} if all entries within map are contained in base map,
     *      {@code false} otherwise.
     */
    public static boolean containsAll(Map<?, ?> base, Map<?, ?> map) {
        assert base != null;
        assert map != null;

        for (Map.Entry<?, ?> entry : map.entrySet())
            if (base.containsKey(entry.getKey())) {
                Object val = base.get(entry.getKey());

                if (val == null && entry.getValue() == null)
                    continue;

                if (val == null || entry.getValue() == null || !val.equals(entry.getValue()))
                    // Mismatch found.
                    return false;
            }
            else
                return false;

        // All entries in 'map' are contained in base map.
        return true;
    }

    /**
     * Gets resource path for the class.
     *
     * @param clsName Class name.
     * @return Resource name for the class.
     */
    public static String classNameToResourceName(String clsName) {
        return clsName.replaceAll("\\.", "/") + ".class";
    }

    /**
     * Gets threading MBean.
     *
     * @return Threading MBean.
     */
    public static ThreadMXBean getThreadMx() {
        return ManagementFactory.getThreadMXBean();
    }

    /**
     * Gets amount of RAM memory available on this machine.
     *
     * @return Total amount of memory in bytes or -1 if any exception happened.
     */
    public static long getTotalMemoryAvailable() {
        MBeanServer mBeanSrv = ManagementFactory.getPlatformMBeanServer();

        Object attr;

        try {
            attr = mBeanSrv.getAttribute(
                ObjectName.getInstance("java.lang", "type", "OperatingSystem"),
                "TotalPhysicalMemorySize");
        }
        catch (Exception e) {
            return -1;
        }

        return (attr instanceof Long) ? (Long)attr : -1;
    }

    /**
     * Gets compilation MBean.
     *
     * @return Compilation MBean.
     */
    public static CompilationMXBean getCompilerMx() {
        return ManagementFactory.getCompilationMXBean();
    }

    /**
     * Tests whether or not given class is loadable provided class loader.
     *
     * @param clsName Class name to test.
     * @param ldr Class loader to test with. If {@code null} - we'll use system class loader instead.
     *      If System class loader is not set - this method will return {@code false}.
     * @return {@code True} if class is loadable, {@code false} otherwise.
     */
    public static boolean isLoadableBy(String clsName, @Nullable ClassLoader ldr) {
        assert clsName != null;

        if (ldr == null)
            ldr = gridClassLoader();

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
     * @return {@code True} if assertions enabled.
     */
    public static boolean assertionsEnabled() {
        return assertionsEnabled;
    }

    /**
     * Gets OS string.
     *
     * @return OS string.
     */
    public static String osString() {
        return osStr;
    }

    /**
     * Gets JDK string.
     *
     * @return JDK string.
     */
    public static String jdkString() {
        return jdkStr;
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
     * Gets JDK version.
     *
     * @return JDK version.
     */
    public static String jdkVersion() {
        return jdkVer;
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
     * Gets JVM implementation name.
     *
     * @return JVM implementation name.
     */
    public static String jvmName() {
        return jvmImplName;
    }

    /**
     * Does a best effort to detect if we a running on a 32-bit JVM.
     *
     * @return {@code true} if detected that we are running on a 32-bit JVM.
     */
    public static boolean jvm32Bit() {
        return jvm32Bit;
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
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Callable to run.
     * @param <R> Return type.
     * @return Return value.
     * @throws IgniteCheckedException If call failed.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, Callable<R> c) throws IgniteCheckedException {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.call();
        }
        catch (IgniteCheckedException | RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     * @param <R> Return type.
     * @return Return value.
     */
    @Nullable public static <R> R wrapThreadLoader(ClassLoader ldr, IgniteOutClosure<R> c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            return c.apply();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Sets thread context class loader to the given loader, executes the closure, and then
     * resets thread context class loader to its initial value.
     *
     * @param ldr Class loader to run the closure under.
     * @param c Closure to run.
     */
    public static void wrapThreadLoader(ClassLoader ldr, Runnable c) {
        Thread curThread = Thread.currentThread();

        // Get original context class loader.
        ClassLoader ctxLdr = curThread.getContextClassLoader();

        try {
            curThread.setContextClassLoader(ldr);

            c.run();
        }
        finally {
            // Set the original class loader back.
            curThread.setContextClassLoader(ctxLdr);
        }
    }

    /**
     * Get string representation of an object properly catching all exceptions.
     *
     * @param obj Object.
     * @return Result or {@code null}.
     */
    @Nullable public static String toStringSafe(@Nullable Object obj) {
        if (obj == null)
            return null;
        else {
            try {
                return obj.toString();
            }
            catch (Exception e) {
                try {
                    return "Failed to convert object to string: " + e.getMessage();
                }
                catch (Exception e0) {
                    return "Failed to convert object to string (error message is not available)";
                }
            }
        }
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static int[] toIntArray(@Nullable Collection<Integer> c) {
        if (c == null || c.isEmpty())
            return EMPTY_INTS;

        int[] arr = new int[c.size()];

        int idx = 0;

        for (Integer i : c)
            arr[idx++] = i;

        return arr;
    }

    /**
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    public static int[] addAll(int[] arr1, int[] arr2) {
        int[] all = new int[arr1.length + arr2.length];

        System.arraycopy(arr1, 0, all, 0, arr1.length);
        System.arraycopy(arr2, 0, all, arr1.length, arr2.length);

        return all;
    }

    /**
     * Converts array of integers into list.
     *
     * @param arr Array of integers.
     * @param p Optional predicate array.
     * @return List of integers.
     */
    public static List<Integer> toIntList(@Nullable int[] arr, IgnitePredicate<Integer>... p) {
        if (arr == null || arr.length == 0)
            return Collections.emptyList();

        List<Integer> ret = new ArrayList<>(arr.length);

        if (F.isEmpty(p))
            for (int i : arr)
                ret.add(i);
        else {
            for (int i : arr)
                if (F.isAll(i, p))
                    ret.add(i);
        }

        return ret;
    }

    /**
     * Converts collection of integers into array.
     *
     * @param c Collection of integers.
     * @return Integer array.
     */
    public static long[] toLongArray(@Nullable Collection<Long> c) {
        if (c == null || c.isEmpty())
            return EMPTY_LONGS;

        long[] arr = new long[c.size()];

        int idx = 0;

        for (Long l : c)
            arr[idx++] = l;

        return arr;
    }

    /**
     * Concats two integers to long.
     *
     * @param high Highest bits.
     * @param low Lowest bits.
     * @return Long.
     */
    public static long toLong(int high, int low) {
        return (((long)high) << Integer.SIZE) | (low & 0xffffffffL);
    }

    /**
     * Copies all elements from collection to array and asserts that
     * array is big enough to hold the collection. This method should
     * always be preferred to {@link Collection#toArray(Object[])}
     * method.
     *
     * @param c Collection to convert to array.
     * @param arr Array to populate.
     * @param <T> Element type.
     * @return Passed in array.
     */
    public static <T> T[] toArray(Collection<? extends T> c, T[] arr) {
        T[] a = c.toArray(arr);

        assert a == arr;

        return arr;
    }

    /**
     * Returns array which is the union of two arrays
     * (array of elements contained in any of provided arrays).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is union of {@code a} and {@code b}.
     */
    public static int[] unique(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen + bLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                res[resLen++] = b[j++];
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        while (j < bLen)
            res[resLen++] = b[j++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Returns array which is the difference between two arrays
     * (array of elements contained in first array but not contained in second).
     * <p/>
     * Note: arrays must be increasing.
     *
     * @param a First array.
     * @param aLen Length of prefix {@code a}.
     * @param b Second array.
     * @param bLen Length of prefix {@code b}.
     * @return Increasing array which is difference between {@code a} and {@code b}.
     */
    public static int[] difference(int[] a, int aLen, int[] b, int bLen) {
        assert a != null;
        assert b != null;
        assert isIncreasingArray(a, aLen);
        assert isIncreasingArray(b, bLen);

        int[] res = new int[aLen];
        int resLen = 0;

        int i = 0;
        int j = 0;

        while (i < aLen && j < bLen) {
            if (a[i] == b[j])
                i++;
            else if (a[i] < b[j])
                res[resLen++] = a[i++];
            else
                j++;
        }

        while (i < aLen)
            res[resLen++] = a[i++];

        return copyIfExceeded(res, resLen);
    }

    /**
     * Checks if array prefix increases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) increases.
     */
    public static boolean isIncreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] >= arr[i])
                return false;
        }

        return true;
    }

    /**
     * Checks if array prefix do not decreases.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return {@code True} if {@code arr} from 0 to ({@code len} - 1) do not decreases.
     */
    public static boolean isNonDecreasingArray(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        if (arr.length == 0)
            return true;

        for (int i = 1; i < len; i++) {
            if (arr[i - 1] > arr[i])
                return false;
        }

        return true;
    }

    /**
     * Copies array only if array length greater than needed length.
     *
     * @param arr Array.
     * @param len Prefix length.
     * @return Old array if length of {@code arr} is equals to {@code len},
     *      otherwise copy of array.
     */
    public static int[] copyIfExceeded(int[] arr, int len) {
        assert arr != null;
        assert 0 <= len && len <= arr.length;

        return len == arr.length ? arr : Arrays.copyOf(arr, len);
    }

    /**
     * Utility method creating {@link JMException} with given cause. Keeps only the error message to avoid
     * deserialization failure on remote side due to the other class path.
     *
     * @param e Cause exception.
     * @return Newly created {@link JMException}.
     */
    public static JMException jmException(Throwable e) {
        return new JMException(e.getMessage());
    }

    /**
     * Unwraps closure exceptions.
     *
     * @param t Exception.
     * @return Unwrapped exception.
     */
    public static Exception unwrap(Throwable t) {
        assert t != null;

        while (true) {
            if (t instanceof Error)
                throw (Error)t;

            if (t instanceof GridClosureException) {
                t = ((GridClosureException)t).unwrap();

                continue;
            }

            return (Exception)t;
        }
    }

    /**
     * Casts the passed {@code Throwable t} to {@link IgniteCheckedException}.<br>
     * If {@code t} is a {@link GridClosureException}, it is unwrapped and then cast to {@link IgniteCheckedException}.
     * If {@code t} is an {@link IgniteCheckedException}, it is returned.
     * If {@code t} is not a {@link IgniteCheckedException}, a new {@link IgniteCheckedException} caused by {@code t}
     * is returned.
     *
     * @param t Throwable to cast.
     * @return {@code t} cast to {@link IgniteCheckedException}.
     */
    public static IgniteCheckedException cast(Throwable t) {
        assert t != null;

        t = unwrap(t);

        return t instanceof IgniteCheckedException
            ? (IgniteCheckedException)t
            : new IgniteCheckedException(t);
    }

    /**
     * Checks if object is a primitive array.
     *
     * @param obj Object to check.
     * @return {@code True} if Object is primitive array.
     */
    public static boolean isPrimitiveArray(Object obj) {
        if (obj == null)
            return false;

        Class<?> cls = obj.getClass();

        return cls.isArray() && cls.getComponentType().isPrimitive();
    }

    /**
     * Awaits for condition ignoring interrupts.
     *
     * @param cond Condition to await for.
     */
    public static void awaitQuiet(Condition cond) {
        cond.awaitUninterruptibly();
    }

    /**
     * Awaits for the latch until it is counted down,
     * ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be
     * recovered prior to return.
     *
     * @param latch Latch to wait for.
     */
    public static void awaitQuiet(CountDownLatch latch) {
        boolean interrupted = false;

        while (true) {
            try {
                latch.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
    }

    /**
     * Awaits for the barrier ignoring interruptions.
     * <p>
     * If calling thread was interrupted, interrupted status will be recovered prior to return. If the barrier is
     * already broken, return immediately without throwing any exceptions.
     *
     * @param barrier Barrier to wait for.
     */
    public static void awaitQuiet(CyclicBarrier barrier) {
        boolean interrupted = false;

        while (true) {
            try {
                barrier.await();

                break;
            }
            catch (InterruptedException ignored) {
                interrupted = true;
            }
            catch (BrokenBarrierException ignored) {
                break;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();
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
     * Gets field value.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Field value.
     */
    public static <T> T field(Object obj, String fieldName) {
        assert obj != null;
        assert fieldName != null;

        try {
            for (Class cls = obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : cls.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        field.setAccessible(true);

                        return (T)field.get(obj);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']', e);
        }

        throw new IgniteException("Failed to get field value [fieldName=" + fieldName + ", obj=" + obj + ']');
    }

    /**
     * Check that field exist.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Boolean flag.
     */
    public static boolean hasField(Object obj, String fieldName) {
        try {
            field(obj, fieldName);

            return true;
        }
        catch (IgniteException e) {
            return false;
        }
    }

    /**
     * @param cls Class to check.
     * @return {@code True} if class is final.
     */
    public static boolean isFinal(Class<?> cls) {
        return Modifier.isFinal(cls.getModifiers());
    }

    /**
     * Gets field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T field(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            for (Class c = cls; cls != Object.class; cls = cls.getSuperclass()) {
                for (Field field : c.getDeclaredFields()) {
                    if (field.getName().equals(fieldName)) {
                        if (!Modifier.isStatic(field.getModifiers()))
                            throw new IgniteCheckedException("Failed to get class field (field is not static) [cls=" +
                                cls + ", fieldName=" + fieldName + ']');

                        boolean accessible = field.isAccessible();

                        T val;

                        try {
                            field.setAccessible(true);

                            val = (T)field.get(null);
                        }
                        finally {
                            if (!accessible)
                                field.setAccessible(false);
                        }

                        return val;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to get field value (field was not found) [fieldName=" + fieldName +
            ", cls=" + cls + ']');
    }

    /**
     * Invokes method.
     *
     * @param cls Object.
     * @param obj Object.
     * @param mtdName Field name.
     * @param params Parameters.
     * @return Field value.
     * @throws IgniteCheckedException If static field with given name cannot be retreived.
     */
    public static <T> T invoke(@Nullable Class<?> cls, @Nullable Object obj, String mtdName,
        Object... params) throws IgniteCheckedException {
        assert cls != null || obj != null;
        assert mtdName != null;

        try {
            for (cls = cls != null ? cls : obj.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
                Method mtd = null;

                for (Method declaredMtd : cls.getDeclaredMethods()) {
                    if (declaredMtd.getName().equals(mtdName)) {
                        if (mtd == null)
                            mtd = declaredMtd;
                        else
                            throw new IgniteCheckedException("Failed to invoke (ambigous method name) [mtdName=" +
                                mtdName + ", cls=" + cls + ']');
                    }
                }

                if (mtd == null)
                    continue;

                boolean accessible = mtd.isAccessible();

                T res;

                try {
                    mtd.setAccessible(true);

                    res = (T)mtd.invoke(obj, params);
                }
                finally {
                    if (!accessible)
                        mtd.setAccessible(false);
                }

                return res;
            }
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to invoke [mtdName=" + mtdName + ", cls=" + cls + ']',
                e);
        }

        throw new IgniteCheckedException("Failed to invoke (method was not found) [mtdName=" + mtdName +
            ", cls=" + cls + ']');
    }

    /**
     * Gets property value.
     *
     * @param obj Object.
     * @param propName Property name.
     * @return Field value.
     */
    public static <T> T property(Object obj, String propName) {
        assert obj != null;
        assert propName != null;

        try {
            Method m;

            try {
                m = obj.getClass().getMethod("get" + capitalFirst(propName));
            }
            catch (NoSuchMethodException ignored) {
                m = obj.getClass().getMethod("is" + capitalFirst(propName));
            }

            assert F.isEmpty(m.getParameterTypes());

            boolean accessible = m.isAccessible();

            try {
                m.setAccessible(true);

                return (T)m.invoke(obj);
            }
            finally {
                m.setAccessible(accessible);
            }
        }
        catch (Exception e) {
            throw new IgniteException(
                "Failed to get property value [property=" + propName + ", obj=" + obj + ']', e);
        }
    }

    /**
     * Gets static field value.
     *
     * @param cls Class.
     * @param fieldName Field name.
     * @return Field value.
     * @throws IgniteCheckedException If failed.
     */
    public static <T> T staticField(Class<?> cls, String fieldName) throws IgniteCheckedException {
        assert cls != null;
        assert fieldName != null;

        try {
            do {
                for (Field field : cls.getDeclaredFields())
                    if (field.getName().equals(fieldName)) {
                        boolean accessible = field.isAccessible();

                        if (!accessible)
                            field.setAccessible(true);

                        T val = (T)field.get(null);

                        if (!accessible)
                            field.setAccessible(false);

                        return val;
                    }

                if (cls == Object.class)
                    break;

                cls = cls.getSuperclass();
            }
            while (true);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']', e);
        }

        throw new IgniteCheckedException("Failed to get field value [fieldName=" + fieldName + ", cls=" + cls + ']');
    }

    /**
     * Capitalizes the first character of the given string.
     *
     * @param str String.
     * @return String with capitalized first character.
     */
    private static String capitalFirst(@Nullable String str) {
        return str == null ? null :
            str.isEmpty() ? "" : Character.toUpperCase(str.charAt(0)) + str.substring(1);
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
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * Gets absolute value for long. If argument is {@link Long#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Argument.
     * @return Absolute value.
     */
    public static long safeAbs(long i) {
        i = Math.abs(i);

        return i < 0 ? 0 : i;
    }

    /**
     * When {@code long} value given is positive returns that value, otherwise returns provided default value.
     *
     * @param i Input value.
     * @param dflt Default value.
     * @return {@code i} if {@code i > 0} and {@code dflt} otherwise.
     */
    public static long ensurePositive(long i, long dflt) {
        return i <= 0 ? dflt : i;
    }

    /**
     * Gets wrapper class for a primitive type.
     *
     * @param cls Class. If {@code null}, method is no-op.
     * @return Wrapper class or original class if it is non-primitive.
     */
    @Nullable public static Class<?> box(@Nullable Class<?> cls) {
        if (cls == null)
            return null;

        if (!cls.isPrimitive())
            return cls;

        return boxedClsMap.get(cls);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return forName(clsName, ldr, null, GridBinaryMarshaller.USE_CACHE.get());
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        IgnitePredicate<String> clsFilter
    ) throws ClassNotFoundException {
        return forName(clsName, ldr, clsFilter, GridBinaryMarshaller.USE_CACHE.get());
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
            ldr = gridClassLoader();

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
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    public static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     * <p>
     * This function has been taken from Java 8 ConcurrentHashMap with
     * slightly modifications.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(Object key) {
        return hash(key.hashCode());
    }

    /**
     * A primitive override of {@link #hash(Object)} to avoid unnecessary boxing.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(long key) {
        int val = (int)(key ^ (key >>> 32));

        return hash(val);
    }

    /**
     * @return PID of the current JVM or {@code -1} if it can't be determined.
     */
    public static int jvmPid() {
        // Should be something like this: 1160@mbp.local
        String name = ManagementFactory.getRuntimeMXBean().getName();

        try {
            int idx = name.indexOf('@');

            return idx > 0 ? Integer.parseInt(name.substring(0, idx)) : -1;
        }
        catch (NumberFormatException ignored) {
            return -1;
        }
    }

    /**
     * @return Input arguments passed to the JVM which does not include the arguments to the <tt>main</tt> method.
     */
    public static List<String> jvmArgs() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments();
    }
}
