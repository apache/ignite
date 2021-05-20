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

import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * Collection of utility methods used throughout the system.
 */
public class IgniteUtils {
    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** Version of the JDK. */
    private static String jdkVer;

    /*
      Initializes enterprise check.
     */
    static {
        IgniteUtils.jdkVer = System.getProperty("java.specification.version");
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
}
