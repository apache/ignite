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

package org.apache.ignite.internal.direct;

import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.clock.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import sun.misc.*;

import java.util.*;

/**
 * Direct marshalling utility methods.
 */
public class DirectUtils {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * Do not instantiate.
     */
    private DirectUtils() {
        // No-op.
    }

    /**
     * @param uuid {@link UUID}.
     * @return Array.
     */
    public static byte[] uuidToArray(UUID uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.getLeastSignificantBits());
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link UUID}.
     */
    public static UUID arrayToUuid(byte[] arr) {
        if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new UUID(most, least);
        }
    }

    /**
     * @param uuid {@link IgniteUuid}.
     * @return Array.
     */
    public static byte[] gridUuidToArray(IgniteUuid uuid) {
        byte[] arr = null;

        if (uuid != null) {
            arr = new byte[24];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, uuid.globalId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, uuid.globalId().getLeastSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, uuid.localId());
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link IgniteUuid}.
     */
    public static IgniteUuid arrayToIgniteUuid(byte[] arr) {
        if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            long loc = UNSAFE.getLong(arr, BYTE_ARR_OFF + 16);

            return new IgniteUuid(new UUID(most, least), loc);
        }
    }

    /**
     * @param ver {@link GridClockDeltaVersion}.
     * @return Array.
     */
    public static byte[] clockDeltaVersionToArray(GridClockDeltaVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[16];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, ver.version());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.topologyVersion());
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link GridClockDeltaVersion}.
     */
    public static GridClockDeltaVersion arrayToClockDeltaVersion(byte[] arr) {
        if (arr == null)
            return null;
        else {
            long ver = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long topVer = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);

            return new GridClockDeltaVersion(ver, topVer);
        }
    }

    /**
     * @param list {@link GridByteArrayList}.
     * @return Array.
     */
    public static byte[] byteArrayListToArray(GridByteArrayList list) {
        return list != null ? list.array() : null;
    }

    /**
     * @param arr Array.
     * @return {@link GridByteArrayList}.
     */
    public static GridByteArrayList arrayToByteArrayList(byte[] arr) {
        return arr != null ? new GridByteArrayList(arr) : null;
    }

    /**
     * @param list {@link GridLongList}.
     * @return Array.
     */
    public static long[] longListToArray(GridLongList list) {
        return list != null ? list.array() : null;
    }

    /**
     * @param arr Array.
     * @return {@link GridLongList}.
     */
    public static GridLongList arrayToLongList(long[] arr) {
        return arr != null ? new GridLongList(arr) : null;
    }

    /**
     * @param ver {@link GridCacheVersion}.
     * @return Array.
     */
    public static byte[] cacheVersionToArray(GridCacheVersion ver) {
        byte[] arr = null;

        if (ver != null) {
            arr = new byte[24];

            UNSAFE.putInt(arr, BYTE_ARR_OFF, ver.topologyVersion());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 4, ver.nodeOrderAndDrIdRaw());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, ver.globalTime());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 16, ver.order());
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link GridCacheVersion}.
     */
    public static GridCacheVersion arrayToCacheVersion(byte[] arr) {
        if (arr == null)
            return null;
        else {
            int topVerDrId = UNSAFE.getInt(arr, BYTE_ARR_OFF);
            int nodeOrder = UNSAFE.getInt(arr, BYTE_ARR_OFF + 4);
            long globalTime = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            long order = UNSAFE.getLong(arr, BYTE_ARR_OFF + 16);

            return new GridCacheVersion(topVerDrId, nodeOrder, globalTime, order);
        }
    }

    /**
     * @param id {@link GridDhtPartitionExchangeId}.
     * @return Array.
     */
    public static byte[] dhtPartitionExchangeIdToArray(GridDhtPartitionExchangeId id) {
        byte[] arr = null;

        if (id != null) {
            arr = new byte[28];

            UNSAFE.putLong(arr, BYTE_ARR_OFF, id.nodeId().getMostSignificantBits());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 8, id.nodeId().getLeastSignificantBits());
            UNSAFE.putInt(arr, BYTE_ARR_OFF + 16, id.event());
            UNSAFE.putLong(arr, BYTE_ARR_OFF + 20, id.topologyVersion());
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link GridDhtPartitionExchangeId}.
     */
    public static GridDhtPartitionExchangeId arrayToDhtPartitionExchangeId(byte[] arr) {
        if (arr == null)
            return null;
        else {
            long most = UNSAFE.getLong(arr, BYTE_ARR_OFF);
            long least = UNSAFE.getLong(arr, BYTE_ARR_OFF + 8);
            int evt = UNSAFE.getInt(arr, BYTE_ARR_OFF + 16);
            long topVer = UNSAFE.getLong(arr, BYTE_ARR_OFF + 20);

            return new GridDhtPartitionExchangeId(new UUID(most, least), evt, topVer);
        }
    }

    /**
     * @param bytes {@link GridCacheValueBytes}.
     * @return Array.
     */
    public static byte[] valueBytesToArray(GridCacheValueBytes bytes) {
        byte[] arr = null;

        if (bytes != null) {
            byte[] bytes0 = bytes.get();

            if (bytes0 != null) {
                int len = bytes0.length;

                arr = new byte[len + 2];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, true);
                UNSAFE.copyMemory(bytes0, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + 1, len);
                UNSAFE.putBoolean(arr, BYTE_ARR_OFF + 1 + len, bytes.isPlain());
            }
            else {
                arr = new byte[1];

                UNSAFE.putBoolean(arr, BYTE_ARR_OFF, false);
            }
        }

        return arr;
    }

    /**
     * @param arr Array.
     * @return {@link GridCacheValueBytes}.
     */
    public static GridCacheValueBytes arrayToValueBytes(byte[] arr) {
        if (arr == null)
            return null;
        else {
            boolean notNull = UNSAFE.getBoolean(arr, BYTE_ARR_OFF);

            if (notNull) {
                int len = arr.length - 2;

                assert len >= 0 : len;

                byte[] bytesArr = new byte[len];

                UNSAFE.copyMemory(arr, BYTE_ARR_OFF + 1, bytesArr, BYTE_ARR_OFF, len);

                boolean isPlain = UNSAFE.getBoolean(arr, BYTE_ARR_OFF + 1 + len);

                return new GridCacheValueBytes(bytesArr, isPlain);
            }
            else
                return new GridCacheValueBytes();
        }
    }

    /**
     * @param str {@link String}.
     * @return Array.
     */
    public static byte[] stringToArray(String str) {
        return str != null ? str.getBytes() : null;
    }

    /**
     * @param arr Array.
     * @return {@link String}.
     */
    public static String arrayToString(byte[] arr) {
        return arr != null ? new String(arr) : null;
    }

    /**
     * @param bits {@link BitSet}.
     * @return Array.
     */
    public static long[] bitSetToArray(BitSet bits) {
        return bits != null ? bits.toLongArray() : null;
    }

    /**
     * @param arr Array.
     * @return {@link BitSet}.
     */
    public static BitSet arrayToBitSet(long[] arr) {
        return arr != null ? BitSet.valueOf(arr) : null;
    }

//    /**
//     * @param name Field name.
//     * @param e Enum.
//     * @return Whether value was fully written.
//     */
//    protected boolean putEnum(String name, @Nullable Enum<?> e) {
//        return writer.writeByte(name, e != null ? (byte)e.ordinal() : -1);
//    }
}
