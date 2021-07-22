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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used throughout the system.
 */
public class IgniteUtils {
    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** Version of the JDK. */
    private static final String jdkVer = System.getProperty("java.specification.version");

    /** Class loader used to load Ignite. */
    private static final ClassLoader igniteClassLoader = IgniteUtils.class.getClassLoader();

    private static final boolean assertionsEnabled;

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = Map.of(
        "byte", byte.class,
        "short", short.class,
        "int", int.class,
        "long", long.class,
        "float", float.class,
        "double", double.class,
        "char", char.class,
        "boolean", boolean.class,
        "void", void.class
    );

    /** */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class<?>>> classCache =
        new ConcurrentHashMap<>();

    /*
      Initializes enterprise check.
     */
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
    }

    /**
     * Get JDK version.
     *
     * @return JDK version.
     */
    public static String jdkVersion() {
        return jdkVer;
    }

    /**
     * Get major Java version from a string.
     *
     * @param verStr Version string.
     * @return Major version or zero if failed to resolve.
     */
    public static int majorJavaVersion(String verStr) {
        if (verStr == null || verStr.isEmpty())
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
     * Returns a capacity that is sufficient to keep the map from being resized as
     * long as it grows no larger than expSize and the load factor is &gt;= its
     * default (0.75).
     *
     * Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of the created map.
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
     * Creates new {@link HashMap} with expected size.
     *
     * @param expSize Expected size of the created map.
     * @param <K> Type of the map's keys.
     * @param <V> Type of the map's values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of the map's keys.
     * @param <V> Type of the map's values.
     * @return New map.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int expSize) {
        return new LinkedHashMap<>(capacity(expSize));
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
     * @param obj Value to hash.
     * @return Hash value.
     */
    public static int hash(Object obj) {
        return hash(obj.hashCode());
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
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr) {
        return toHexString(arr, Integer.MAX_VALUE);
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @param maxLen Maximum length of result string. Rounds down to a power of two.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr, int maxLen) {
        assert maxLen >= 0 : "maxLem must be not negative.";

        int capacity = Math.min(arr.length << 1, maxLen);

        int lim = capacity >> 1;

        StringBuilder sb = new StringBuilder(capacity);

        for (int i = 0; i < lim; i++)
            addByteAsHex(sb, arr[i]);

        return sb.toString().toUpperCase();
    }

    /**
     * @param sb String builder.
     * @param b Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
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
     * Returns a first non-null value in a given array, if such is present.
     *
     * @param vals Input array.
     * @return First non-null value, or {@code null}, if array is empty or contains
     *      only nulls.
     */
    @SafeVarargs
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
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader igniteClassLoader() {
        return igniteClassLoader;
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
        return forName(clsName, ldr, null);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param clsFilter Predicate to filter class names.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
        String clsName,
        @Nullable ClassLoader ldr,
        Predicate<String> clsFilter
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null)
            return cls;

        if (ldr == null)
            ldr = igniteClassLoader;

        ConcurrentMap<String, Class<?>> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class<?>> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null)
                ldrMap = old;
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.test(clsName))
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");

            cls = Class.forName(clsName, true, ldr);

            Class<?> old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null)
                cls = old;
        }

        return cls;
    }

    /**
     * Deletes a file or a directory with all sub-directories and files.
     *
     * @param path File or directory to delete.
     * @return {@code true} if the file or directory is successfully deleted or does not exist, {@code false} otherwise
     */
    public static boolean deleteIfExists(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc != null)
                        throw exc;

                    Files.delete(dir);

                    return FileVisitResult.CONTINUE;
                }

                @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);

                    return FileVisitResult.CONTINUE;
                }
            });

            return true;
        }
        catch (NoSuchFileException e) {
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /**
     * @return {@code True} if assertions enabled.
     */
    public static boolean assertionsEnabled() {
        return assertionsEnabled;
    }

    /**
     * Shuts down the given executor service gradually, first disabling new submissions and later, if
     * necessary, cancelling remaining tasks.
     *
     * <p>The method takes the following steps:
     *
     * <ol>
     *   <li>calls {@link ExecutorService#shutdown()}, disabling acceptance of new submitted tasks.
     *   <li>awaits executor service termination for half of the specified timeout.
     *   <li>if the timeout expires, it calls {@link ExecutorService#shutdownNow()}, cancelling
     *       pending tasks and interrupting running tasks.
     *   <li>awaits executor service termination for the other half of the specified timeout.
     * </ol>
     *
     * <p>If, at any step of the process, the calling thread is interrupted, the method calls {@link
     * ExecutorService#shutdownNow()} and returns.
     *
     * @param service the {@code ExecutorService} to shut down
     * @param timeout the maximum time to wait for the {@code ExecutorService} to terminate
     * @param unit the time unit of the timeout argument
     */
    public static void shutdownAndAwaitTermination(ExecutorService service, long timeout, TimeUnit unit) {
        long halfTimeoutNanos = unit.toNanos(timeout) / 2;

        // Disable new tasks from being submitted
        service.shutdown();

        try {
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                service.shutdownNow();
                // Wait the other half of the timeout for tasks to respond to being cancelled
                service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        }
        catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
    }

    /**
     * Closes all provided objects. If any of the {@link AutoCloseable#close} methods throw an exception, only the first
     * thrown exception will be propagated to the caller, after all other objects are closed, similar to
     * the try-with-resources block.
     *
     * @param closeables Collection of objects to close.
     * @throws Exception If failed to close.
     */
    public static void closeAll(Collection<? extends AutoCloseable> closeables) throws Exception {
        Exception ex = null;

        for (AutoCloseable closeable : closeables) {
            try {
                if (closeable != null)
                    closeable.close();
            }
            catch (Exception e) {
                if (ex == null)
                    ex = e;
                else
                    ex.addSuppressed(e);
            }
        }

        if (ex != null)
            throw ex;
    }

    /**
     * Closes all provided objects.
     *
     * @param closeables Array of closeable objects to close.
     * @throws Exception If failed to close.
     *
     * @see #closeAll(Collection)
     */
    public static void closeAll(AutoCloseable... closeables) throws Exception {
        closeAll(Arrays.asList(closeables));
    }
}
