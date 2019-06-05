/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This utility class contains miscellaneous functions.
 */
public class Utils {

    /**
     * An 0-size byte array.
     */
    public static final byte[] EMPTY_BYTES = {};

    /**
     * An 0-size int array.
     */
    public static final int[] EMPTY_INT_ARRAY = {};

    /**
     * An 0-size long array.
     */
    private static final long[] EMPTY_LONG_ARRAY = {};

    private static final int GC_DELAY = 50;
    private static final int MAX_GC = 8;
    private static long lastGC;

    private static final HashMap<String, byte[]> RESOURCES = new HashMap<>();

    private Utils() {
        // utility class
    }

    /**
     * Calculate the index of the first occurrence of the pattern in the byte
     * array, starting with the given index. This methods returns -1 if the
     * pattern has not been found, and the start position if the pattern is
     * empty.
     *
     * @param bytes the byte array
     * @param pattern the pattern
     * @param start the start index from where to search
     * @return the index
     */
    public static int indexOf(byte[] bytes, byte[] pattern, int start) {
        if (pattern.length == 0) {
            return start;
        }
        if (start > bytes.length) {
            return -1;
        }
        int last = bytes.length - pattern.length + 1;
        int patternLen = pattern.length;
        next:
        for (; start < last; start++) {
            for (int i = 0; i < patternLen; i++) {
                if (bytes[start + i] != pattern[i]) {
                    continue next;
                }
            }
            return start;
        }
        return -1;
    }

    /**
     * Calculate the hash code of the given byte array.
     *
     * @param value the byte array
     * @return the hash code
     */
    public static int getByteArrayHash(byte[] value) {
        int len = value.length;
        int h = len;
        if (len < 50) {
            for (int i = 0; i < len; i++) {
                h = 31 * h + value[i];
            }
        } else {
            int step = len / 16;
            for (int i = 0; i < 4; i++) {
                h = 31 * h + value[i];
                h = 31 * h + value[--len];
            }
            for (int i = 4 + step; i < len; i += step) {
                h = 31 * h + value[i];
            }
        }
        return h;
    }

    /**
     * Compare two byte arrays. This method will always loop over all bytes and
     * doesn't use conditional operations in the loop to make sure an attacker
     * can not use a timing attack when trying out passwords.
     *
     * @param test the first array
     * @param good the second array
     * @return true if both byte arrays contain the same bytes
     */
    public static boolean compareSecure(byte[] test, byte[] good) {
        if ((test == null) || (good == null)) {
            return (test == null) && (good == null);
        }
        int len = test.length;
        if (len != good.length) {
            return false;
        }
        if (len == 0) {
            return true;
        }
        // don't use conditional operations inside the loop
        int bits = 0;
        for (int i = 0; i < len; i++) {
            // this will never reset any bits
            bits |= test[i] ^ good[i];
        }
        return bits == 0;
    }

    /**
     * Copy the contents of the source array to the target array. If the size if
     * the target array is too small, a larger array is created.
     *
     * @param source the source array
     * @param target the target array
     * @return the target array or a new one if the target array was too small
     */
    public static byte[] copy(byte[] source, byte[] target) {
        int len = source.length;
        if (len > target.length) {
            target = new byte[len];
        }
        System.arraycopy(source, 0, target, 0, len);
        return target;
    }

    /**
     * Create an array of bytes with the given size. If this is not possible
     * because not enough memory is available, an OutOfMemoryError with the
     * requested size in the message is thrown.
     * <p>
     * This method should be used if the size of the array is user defined, or
     * stored in a file, so wrong size data can be distinguished from regular
     * out-of-memory.
     * </p>
     *
     * @param len the number of bytes requested
     * @return the byte array
     * @throws OutOfMemoryError if the allocation was too large
     */
    public static byte[] newBytes(int len) {
        if (len == 0) {
            return EMPTY_BYTES;
        }
        try {
            return new byte[len];
        } catch (OutOfMemoryError e) {
            Error e2 = new OutOfMemoryError("Requested memory: " + len);
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Creates a copy of array of bytes with the new size. If this is not possible
     * because not enough memory is available, an OutOfMemoryError with the
     * requested size in the message is thrown.
     * <p>
     * This method should be used if the size of the array is user defined, or
     * stored in a file, so wrong size data can be distinguished from regular
     * out-of-memory.
     * </p>
     *
     * @param bytes source array
     * @param len the number of bytes in the new array
     * @return the byte array
     * @throws OutOfMemoryError if the allocation was too large
     * @see Arrays#copyOf(byte[], int)
     */
    public static byte[] copyBytes(byte[] bytes, int len) {
        if (len == 0) {
            return EMPTY_BYTES;
        }
        try {
            return Arrays.copyOf(bytes, len);
        } catch (OutOfMemoryError e) {
            Error e2 = new OutOfMemoryError("Requested memory: " + len);
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Create a new byte array and copy all the data. If the size of the byte
     * array is zero, the same array is returned.
     *
     * @param b the byte array (may not be null)
     * @return a new byte array
     */
    public static byte[] cloneByteArray(byte[] b) {
        if (b == null) {
            return null;
        }
        int len = b.length;
        if (len == 0) {
            return EMPTY_BYTES;
        }
        return Arrays.copyOf(b, len);
    }

    /**
     * Get the used memory in KB.
     * This method possibly calls System.gc().
     *
     * @return the used memory
     */
    public static int getMemoryUsed() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.totalMemory() - rt.freeMemory();
        return (int) (mem >> 10);
    }

    /**
     * Get the free memory in KB.
     * This method possibly calls System.gc().
     *
     * @return the free memory
     */
    public static int getMemoryFree() {
        collectGarbage();
        Runtime rt = Runtime.getRuntime();
        long mem = rt.freeMemory();
        return (int) (mem >> 10);
    }

    /**
     * Get the maximum memory in KB.
     *
     * @return the maximum memory
     */
    public static long getMemoryMax() {
        long max = Runtime.getRuntime().maxMemory();
        return max / 1024;
    }

    public static long getGarbageCollectionTime() {
        long totalGCTime = 0;
        for (GarbageCollectorMXBean gcMXBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long collectionTime = gcMXBean.getCollectionTime();
            if(collectionTime > 0) {
                totalGCTime += collectionTime;
            }
        }
        return totalGCTime;
    }

    private static synchronized void collectGarbage() {
        Runtime runtime = Runtime.getRuntime();
        long total = runtime.totalMemory();
        long time = System.nanoTime();
        if (lastGC + TimeUnit.MILLISECONDS.toNanos(GC_DELAY) < time) {
            for (int i = 0; i < MAX_GC; i++) {
                runtime.gc();
                long now = runtime.totalMemory();
                if (now == total) {
                    lastGC = System.nanoTime();
                    break;
                }
                total = now;
            }
        }
    }

    /**
     * Create an int array with the given size.
     *
     * @param len the number of bytes requested
     * @return the int array
     */
    public static int[] newIntArray(int len) {
        if (len == 0) {
            return EMPTY_INT_ARRAY;
        }
        return new int[len];
    }

    /**
     * Create a long array with the given size.
     *
     * @param len the number of bytes requested
     * @return the int array
     */
    public static long[] newLongArray(int len) {
        if (len == 0) {
            return EMPTY_LONG_ARRAY;
        }
        return new long[len];
    }

    /**
     * Find the top limit values using given comparator and place them as in a
     * full array sort, in descending order.
     *
     * @param array the array.
     * @param offset the offset.
     * @param limit the limit.
     * @param comp the comparator.
     */
    public static <X> void sortTopN(X[] array, int offset, int limit,
            Comparator<? super X> comp) {
        partitionTopN(array, offset, limit, comp);
        Arrays.sort(array, offset,
                (int) Math.min((long) offset + limit, array.length), comp);
    }

    /**
     * Find the top limit values using given comparator and place them as in a
     * full array sort. This method does not sort the top elements themselves.
     *
     * @param array the array
     * @param offset the offset
     * @param limit the limit
     * @param comp the comparator
     */
    private static <X> void partitionTopN(X[] array, int offset, int limit,
            Comparator<? super X> comp) {
        partialQuickSort(array, 0, array.length - 1, comp, offset, offset +
                limit - 1);
    }

    private static <X> void partialQuickSort(X[] array, int low, int high,
            Comparator<? super X> comp, int start, int end) {
        if (low > end || high < start || (low > start && high < end)) {
            return;
        }
        if (low == high) {
            return;
        }
        int i = low, j = high;
        // use a random pivot to protect against
        // the worst case order
        int p = low + MathUtils.randomInt(high - low);
        X pivot = array[p];
        int m = (low + high) >>> 1;
        X temp = array[m];
        array[m] = pivot;
        array[p] = temp;
        while (i <= j) {
            while (comp.compare(array[i], pivot) < 0) {
                i++;
            }
            while (comp.compare(array[j], pivot) > 0) {
                j--;
            }
            if (i <= j) {
                temp = array[i];
                array[i++] = array[j];
                array[j--] = temp;
            }
        }
        if (low < j) {
            partialQuickSort(array, low, j, comp, start, end);
        }
        if (i < high) {
            partialQuickSort(array, i, high, comp, start, end);
        }
    }

    /**
     * Checks if given classes have a common Comparable superclass.
     *
     * @param c1 the first class
     * @param c2 the second class
     * @return true if they have
     */
    public static boolean haveCommonComparableSuperclass(
            Class<?> c1, Class<?> c2) {
        if (c1 == c2 || c1.isAssignableFrom(c2) || c2.isAssignableFrom(c1)) {
            return true;
        }
        Class<?> top1;
        do {
            top1 = c1;
            c1 = c1.getSuperclass();
        } while (Comparable.class.isAssignableFrom(c1));

        Class<?> top2;
        do {
            top2 = c2;
            c2 = c2.getSuperclass();
        } while (Comparable.class.isAssignableFrom(c2));

        return top1 == top2;
    }

    /**
     * Get a resource from the resource map.
     *
     * @param name the name of the resource
     * @return the resource data
     */
    public static byte[] getResource(String name) throws IOException {
        byte[] data = RESOURCES.get(name);
        if (data == null) {
            data = loadResource(name);
            if (data != null) {
                RESOURCES.put(name, data);
            }
        }
        return data;
    }

    private static byte[] loadResource(String name) throws IOException {
        InputStream in = Utils.class.getResourceAsStream("data.zip");
        if (in == null) {
            in = Utils.class.getResourceAsStream(name);
            if (in == null) {
                return null;
            }
            return IOUtils.readBytesAndClose(in, 0);
        }

        try (ZipInputStream zipIn = new ZipInputStream(in)) {
            while (true) {
                ZipEntry entry = zipIn.getNextEntry();
                if (entry == null) {
                    break;
                }
                String entryName = entry.getName();
                if (!entryName.startsWith("/")) {
                    entryName = "/" + entryName;
                }
                if (entryName.equals(name)) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    IOUtils.copy(zipIn, out);
                    zipIn.closeEntry();
                    return out.toByteArray();
                }
                zipIn.closeEntry();
            }
        } catch (IOException e) {
            // if this happens we have a real problem
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Calls a static method via reflection. This will try to use the method
     * where the most parameter classes match exactly (this algorithm is simpler
     * than the one in the Java specification, but works well for most cases).
     *
     * @param classAndMethod a string with the entire class and method name, eg.
     *            "java.lang.System.gc"
     * @param params the method parameters
     * @return the return value from this call
     */
    public static Object callStaticMethod(String classAndMethod,
            Object... params) throws Exception {
        int lastDot = classAndMethod.lastIndexOf('.');
        String className = classAndMethod.substring(0, lastDot);
        String methodName = classAndMethod.substring(lastDot + 1);
        return callMethod(null, Class.forName(className), methodName, params);
    }

    /**
     * Calls an instance method via reflection. This will try to use the method
     * where the most parameter classes match exactly (this algorithm is simpler
     * than the one in the Java specification, but works well for most cases).
     *
     * @param instance the instance on which the call is done
     * @param methodName a string with the method name
     * @param params the method parameters
     * @return the return value from this call
     */
    public static Object callMethod(
            Object instance,
            String methodName,
            Object... params) throws Exception {
        return callMethod(instance, instance.getClass(), methodName, params);
    }

    private static Object callMethod(
            Object instance, Class<?> clazz,
            String methodName,
            Object... params) throws Exception {
        Method best = null;
        int bestMatch = 0;
        boolean isStatic = instance == null;
        for (Method m : clazz.getMethods()) {
            if (Modifier.isStatic(m.getModifiers()) == isStatic &&
                    m.getName().equals(methodName)) {
                int p = match(m.getParameterTypes(), params);
                if (p > bestMatch) {
                    bestMatch = p;
                    best = m;
                }
            }
        }
        if (best == null) {
            throw new NoSuchMethodException(methodName);
        }
        return best.invoke(instance, params);
    }

    /**
     * Creates a new instance. This will try to use the constructor where the
     * most parameter classes match exactly (this algorithm is simpler than the
     * one in the Java specification, but works well for most cases).
     *
     * @param className a string with the entire class, eg. "java.lang.Integer"
     * @param params the constructor parameters
     * @return the newly created object
     */
    public static Object newInstance(String className, Object... params)
            throws Exception {
        Constructor<?> best = null;
        int bestMatch = 0;
        for (Constructor<?> c : Class.forName(className).getConstructors()) {
            int p = match(c.getParameterTypes(), params);
            if (p > bestMatch) {
                bestMatch = p;
                best = c;
            }
        }
        if (best == null) {
            throw new NoSuchMethodException(className);
        }
        return best.newInstance(params);
    }

    private static int match(Class<?>[] params, Object[] values) {
        int len = params.length;
        if (len == values.length) {
            int points = 1;
            for (int i = 0; i < len; i++) {
                Class<?> pc = getNonPrimitiveClass(params[i]);
                Object v = values[i];
                Class<?> vc = v == null ? null : v.getClass();
                if (pc == vc) {
                    points++;
                } else if (vc == null) {
                    // can't verify
                } else if (!pc.isAssignableFrom(vc)) {
                    return 0;
                }
            }
            return points;
        }
        return 0;
    }

    /**
     * Returns a static field.
     *
     * @param classAndField a string with the entire class and field name
     * @return the field value
     */
    public static Object getStaticField(String classAndField) throws Exception {
        int lastDot = classAndField.lastIndexOf('.');
        String className = classAndField.substring(0, lastDot);
        String fieldName = classAndField.substring(lastDot + 1);
        return Class.forName(className).getField(fieldName).get(null);
    }

    /**
     * Returns a static field.
     *
     * @param instance the instance on which the call is done
     * @param fieldName the field name
     * @return the field value
     */
    public static Object getField(Object instance, String fieldName)
            throws Exception {
        return instance.getClass().getField(fieldName).get(instance);
    }

    /**
     * Returns true if the class is present in the current class loader.
     *
     * @param fullyQualifiedClassName a string with the entire class name, eg.
     *        "java.lang.System"
     * @return true if the class is present
     */
    public static boolean isClassPresent(String fullyQualifiedClassName) {
        try {
            Class.forName(fullyQualifiedClassName);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Convert primitive class names to java.lang.* class names.
     *
     * @param clazz the class (for example: int)
     * @return the non-primitive class (for example: java.lang.Integer)
     */
    public static Class<?> getNonPrimitiveClass(Class<?> clazz) {
        if (!clazz.isPrimitive()) {
            return clazz;
        } else if (clazz == boolean.class) {
            return Boolean.class;
        } else if (clazz == byte.class) {
            return Byte.class;
        } else if (clazz == char.class) {
            return Character.class;
        } else if (clazz == double.class) {
            return Double.class;
        } else if (clazz == float.class) {
            return Float.class;
        } else if (clazz == int.class) {
            return Integer.class;
        } else if (clazz == long.class) {
            return Long.class;
        } else if (clazz == short.class) {
            return Short.class;
        } else if (clazz == void.class) {
            return Void.class;
        }
        return clazz;
    }

    /**
     * Parses the specified string to boolean value.
     *
     * @param value
     *            string to parse
     * @param defaultValue
     *            value to return if value is null or on parsing error
     * @param throwException
     *            throw exception on parsing error or return default value instead
     * @return parsed or default value
     * @throws IllegalArgumentException
     *             on parsing error if {@code throwException} is true
     */
    public static boolean parseBoolean(String value, boolean defaultValue, boolean throwException) {
        if (value == null) {
            return defaultValue;
        }
        switch (value.length()) {
        case 1:
            if (value.equals("1") || value.equalsIgnoreCase("t") || value.equalsIgnoreCase("y")) {
                return true;
            }
            if (value.equals("0") || value.equalsIgnoreCase("f") || value.equalsIgnoreCase("n")) {
                return false;
            }
            break;
        case 2:
            if (value.equalsIgnoreCase("no")) {
                return false;
            }
            break;
        case 3:
            if (value.equalsIgnoreCase("yes")) {
                return true;
            }
            break;
        case 4:
            if (value.equalsIgnoreCase("true")) {
                return true;
            }
            break;
        case 5:
            if (value.equalsIgnoreCase("false")) {
                return false;
            }
        }
        if (throwException) {
            throw new IllegalArgumentException(value);
        }
        return defaultValue;
    }

    /**
     * Get the system property. If the system property is not set, or if a
     * security exception occurs, the default value is returned.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value
     */
    public static String getProperty(String key, String defaultValue) {
        try {
            return System.getProperty(key, defaultValue);
        } catch (SecurityException se) {
            return defaultValue;
        }
    }

    /**
     * Get the system property. If the system property is not set, or if a
     * security exception occurs, the default value is returned.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value
     */
    public static int getProperty(String key, int defaultValue) {
        String s = getProperty(key, null);
        if (s != null) {
            try {
                return Integer.decode(s);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultValue;
    }

    /**
     * Get the system property. If the system property is not set, or if a
     * security exception occurs, the default value is returned.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the value
     */
    public static boolean getProperty(String key, boolean defaultValue) {
        return parseBoolean(getProperty(key, null), defaultValue, false);
    }

    /**
     * Scale the value with the available memory. If 1 GB of RAM is available,
     * the value is returned, if 2 GB are available, then twice the value, and
     * so on.
     *
     * @param value the value to scale
     * @return the scaled value
     */
    public static int scaleForAvailableMemory(int value) {
        long maxMemory = Runtime.getRuntime().maxMemory();
        if (maxMemory != Long.MAX_VALUE) {
            // we are limited by an -XmX parameter
            return (int) (value * maxMemory / (1024 * 1024 * 1024));
        }
        try {
            OperatingSystemMXBean mxBean = ManagementFactory
                    .getOperatingSystemMXBean();
            // this method is only available on the class
            // com.sun.management.OperatingSystemMXBean, which mxBean
            // is an instance of under the Oracle JDK, but it is not present on
            // Android and other JDK's
            Method method = Class.forName(
                    "com.sun.management.OperatingSystemMXBean").
                    getMethod("getTotalPhysicalMemorySize");
            long physicalMemorySize = ((Number) method.invoke(mxBean)).longValue();
            return (int) (value * physicalMemorySize / (1024 * 1024 * 1024));
        } catch (Exception e) {
            // ignore
        }
        return value;
    }

    /**
     * The utility methods will try to use the provided class factories to
     * convert binary name of class to Class object. Used by H2 OSGi Activator
     * in order to provide a class from another bundle ClassLoader.
     */
    public interface ClassFactory {

        /**
         * Check whether the factory can return the named class.
         *
         * @param name the binary name of the class
         * @return true if this factory can return a valid class for the
         *         provided class name
         */
        boolean match(String name);

        /**
         * Load the class.
         *
         * @param name the binary name of the class
         * @return the class object
         * @throws ClassNotFoundException If the class is not handle by this
         *             factory
         */
        Class<?> loadClass(String name)
                throws ClassNotFoundException;
    }
}
