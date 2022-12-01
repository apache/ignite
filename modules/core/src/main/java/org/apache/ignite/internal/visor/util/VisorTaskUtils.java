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

package org.apache.ignite.internal.visor.util;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.eviction.AbstractEvictionPolicyFactory;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED_OVER;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_REJECTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_JOB_TIMEDOUT;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.events.EventType.EVT_TASK_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
public class VisorTaskUtils {
    /** Default substitute for {@code null} names. */
    private static final String DFLT_EMPTY_NAME = "<default>";

    /** Only task event types that Visor should collect. */
    public static final int[] VISOR_TASK_EVTS = {
        EVT_JOB_STARTED,
        EVT_JOB_FINISHED,
        EVT_JOB_TIMEDOUT,
        EVT_JOB_FAILED,
        EVT_JOB_FAILED_OVER,
        EVT_JOB_REJECTED,
        EVT_JOB_CANCELLED,

        EVT_TASK_STARTED,
        EVT_TASK_FINISHED,
        EVT_TASK_FAILED,
        EVT_TASK_TIMEDOUT
    };

    /**
     * @param name Grid-style nullable name.
     * @return Name with {@code null} replaced to &lt;default&gt;.
     */
    public static String escapeName(@Nullable Object name) {
        return name == null ? DFLT_EMPTY_NAME : name.toString();
    }

    /**
     * @param name Escaped name.
     * @return Name or {@code null} for default name.
     */
    public static String unescapeName(String name) {
        assert name != null;

        return DFLT_EMPTY_NAME.equals(name) ? null : name;
    }

    /**
     * Concat arrays in one.
     *
     * @param arrays Arrays.
     * @return Summary array.
     */
    public static int[] concat(int[]... arrays) {
        assert arrays != null;
        assert arrays.length > 1;

        int len = 0;

        for (int[] a : arrays)
            len += a.length;

        int[] r = Arrays.copyOf(arrays[0], len);

        for (int i = 1, shift = 0; i < arrays.length; i++) {
            shift += arrays[i - 1].length;
            System.arraycopy(arrays[i], 0, r, shift, arrays[i].length);
        }

        return r;
    }

    /**
     * Returns compact class host.
     *
     * @param obj Object to compact.
     * @return String.
     */
    @Nullable public static Object compactObject(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof Enum)
            return obj.toString();

        if (obj instanceof String || obj instanceof Boolean || obj instanceof Number)
            return obj;

        if (obj instanceof Collection) {
            Collection col = (Collection)obj;

            Object[] res = new Object[col.size()];

            int i = 0;

            for (Object elm : col)
                res[i++] = compactObject(elm);

            return res;
        }

        if (obj.getClass().isArray()) {
            Class<?> arrType = obj.getClass().getComponentType();

            if (arrType.isPrimitive()) {
                if (obj instanceof boolean[])
                    return Arrays.toString((boolean[])obj);
                if (obj instanceof byte[])
                    return Arrays.toString((byte[])obj);
                if (obj instanceof short[])
                    return Arrays.toString((short[])obj);
                if (obj instanceof int[])
                    return Arrays.toString((int[])obj);
                if (obj instanceof long[])
                    return Arrays.toString((long[])obj);
                if (obj instanceof float[])
                    return Arrays.toString((float[])obj);
                if (obj instanceof double[])
                    return Arrays.toString((double[])obj);
            }

            Object[] arr = (Object[])obj;

            int iMax = arr.length - 1;

            StringBuilder sb = new StringBuilder("[");

            for (int i = 0; i <= iMax; i++) {
                sb.append(compactObject(arr[i]));

                if (i != iMax)
                    sb.append(", ");
            }

            sb.append(']');

            return sb.toString();
        }

        return U.compact(obj.getClass().getName());
    }

    /**
     * Compact class names.
     *
     * @param cls Class object for compact.
     * @return Compacted string.
     */
    @Nullable public static String compactClass(Class cls) {
        if (cls == null)
            return null;

        return U.compact(cls.getName());
    }

    /**
     * Compact class names.
     *
     * @param obj Object for compact.
     * @return Compacted string.
     */
    @Nullable public static String compactClass(@Nullable Object obj) {
        if (obj == null)
            return null;

        return compactClass(obj.getClass());
    }

    /**
     * Compact classes names.

     * @param clss Classes to compact.
     * @return Compacted string.
     */
    @Nullable public static List<String> compactClasses(Class<?>[] clss) {
        if (clss == null)
            return null;

        int len = clss.length;

        List<String> res = new ArrayList<>(len);

        for (Class<?> cls: clss)
            res.add(U.compact(cls.getName()));

        return res;
    }

    /**
     * Joins iterable collection elements to string.
     *
     * @param col Iterable collection.
     * @return String.
     */
    @Nullable public static String compactIterable(Iterable col) {
        if (col == null || !col.iterator().hasNext())
            return null;

        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        for (Object s : col)
            sb.append(s).append(sep);

        if (sb.length() > 0)
            sb.setLength(sb.length() - sep.length());

        return U.compact(sb.toString());
    }

    /**
     * Extract max size from eviction policy if available.
     *
     * @param plc Eviction policy.
     * @return Extracted max size.
     */
    public static Integer evictionPolicyMaxSize(@Nullable Factory plc) {
        if (plc instanceof AbstractEvictionPolicyFactory)
            return ((AbstractEvictionPolicyFactory)plc).getMaxSize();

        return null;
    }

    /**
     * Pretty-formatting for duration.
     *
     * @param ms Millisecond to format.
     * @return Formatted presentation.
     */
    private static String formatDuration(long ms) {
        assert ms >= 0;

        if (ms == 0)
            return "< 1 ms";

        SB sb = new SB();

        long dd = ms / 1440000; // 1440 mins = 60 mins * 24 hours

        if (dd > 0)
            sb.a(dd).a(dd == 1 ? " day " : " days ");

        ms %= 1440000;

        long hh = ms / 60000;

        if (hh > 0)
            sb.a(hh).a(hh == 1 ? " hour " : " hours ");

        long min = ms / 60000;

        if (min > 0)
            sb.a(min).a(min == 1 ? " min " : " mins ");

        ms %= 60000;

        if (ms > 0)
            sb.a(ms).a(" ms ");

        return sb.toString().trim();
    }

    /**
     * @param log Logger.
     * @param time Time.
     * @param msg Message.
     */
    private static void log0(@Nullable IgniteLogger log, long time, String msg) {
        if (log != null) {
            if (log.isDebugEnabled())
                log.debug(msg);
            else
                log.warning(msg);
        }
        else
            X.println(String.format(
                "[%s][%s]%s",
                IgniteUtils.DEBUG_DATE_FMT.format(Instant.ofEpochMilli(time)),
                Thread.currentThread().getName(),
                msg
            ));
    }

    /**
     * Log start.
     *
     * @param log Logger.
     * @param clazz Class.
     * @param start Start time.
     */
    public static void logStart(@Nullable IgniteLogger log, Class<?> clazz, long start) {
        log0(log, start, "[" + clazz.getSimpleName() + "]: STARTED");
    }

    /**
     * Log finished.
     *
     * @param log Logger.
     * @param clazz Class.
     * @param start Start time.
     */
    public static void logFinish(@Nullable IgniteLogger log, Class<?> clazz, long start) {
        final long end = System.currentTimeMillis();

        log0(log, end, String.format("[%s]: FINISHED, duration: %s", clazz.getSimpleName(), formatDuration(end - start)));
    }

    /**
     * Log task mapped.
     *
     * @param log Logger.
     * @param clazz Task class.
     * @param nodes Mapped nodes.
     */
    public static void logMapped(@Nullable IgniteLogger log, Class<?> clazz, Collection<ClusterNode> nodes) {
        log0(log, System.currentTimeMillis(),
            String.format("[%s]: MAPPED: %s", clazz.getSimpleName(), U.toShortString(nodes)));
    }

    /**
     * Log message.
     *
     * @param log Logger.
     * @param msg Message to log.
     * @param clazz class.
     * @param start start time.
     * @return Time when message was logged.
     */
    public static long log(@Nullable IgniteLogger log, String msg, Class<?> clazz, long start) {
        final long end = System.currentTimeMillis();

        log0(log, end, String.format("[%s]: %s, duration: %s", clazz.getSimpleName(), msg, formatDuration(end - start)));

        return end;
    }

    /**
     * Log message.
     *
     * @param log Logger.
     * @param msg Message.
     */
    public static void log(@Nullable IgniteLogger log, String msg) {
        log0(log, System.currentTimeMillis(), " " + msg);
    }

    /**
     * Special wrapper over address that can be sorted in following order:
     *     IPv4, private IPv4, IPv4 local host, IPv6.
     *     Lower addresses first.
     */
    public static class SortableAddress implements Comparable<SortableAddress> {
        /** */
        private int type;

        /** */
        private BigDecimal bits;

        /** */
        private String addr;

        /**
         * Constructor.
         *
         * @param addr Address as string.
         */
        public SortableAddress(String addr) {
            this.addr = addr;

            if (addr.indexOf(':') > 0)
                type = 4; // IPv6
            else {
                try {
                    InetAddress inetAddr = InetAddress.getByName(addr);

                    if (inetAddr.isLoopbackAddress())
                        type = 3;  // localhost
                    else if (inetAddr.isSiteLocalAddress())
                        type = 2;  // private IPv4
                    else
                        type = 1; // other IPv4
                }
                catch (UnknownHostException ignored) {
                    type = 5;
                }
            }

            bits = BigDecimal.valueOf(0L);

            try {
                String[] octets = addr.contains(".") ? addr.split(".") : addr.split(":");

                int len = octets.length;

                for (int i = 0; i < len; i++) {
                    long oct = F.isEmpty(octets[i]) ? 0 : Long.valueOf( octets[i]);
                    long pow = Double.valueOf(Math.pow(256, octets.length - 1 - i)).longValue();

                    bits = bits.add(BigDecimal.valueOf(oct * pow));
                }
            }
            catch (Exception ignore) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull SortableAddress o) {
            return (type == o.type ? bits.compareTo(o.bits) : Integer.compare(type, o.type));
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SortableAddress other = (SortableAddress)o;

            return addr != null ? addr.equals(other.addr) : other.addr == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return addr != null ? addr.hashCode() : 0;
        }

        /**
         * @return Address.
         */
        public String address() {
            return addr;
        }
    }

    /**
     * Check whether cache restarting in progress.
     *
     * @param ignite Grid.
     * @param cacheName Cache name to check.
     * @return {@code true} when cache restarting in progress.
     */
    public static boolean isRestartingCache(IgniteEx ignite, String cacheName) {
        IgniteCacheProxy<Object, Object> proxy = ignite.context().cache().jcache(cacheName);

        return proxy instanceof IgniteCacheProxyImpl && ((IgniteCacheProxyImpl)proxy).isRestarting();
    }
}
