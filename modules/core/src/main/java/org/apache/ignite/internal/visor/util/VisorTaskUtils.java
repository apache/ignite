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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicyMBean;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyMBean;
import org.apache.ignite.cache.eviction.random.RandomEvictionPolicyMBean;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.event.VisorGridDiscoveryEventV2;
import org.apache.ignite.internal.visor.event.VisorGridEvent;
import org.apache.ignite.internal.visor.event.VisorGridEventsLost;
import org.apache.ignite.internal.visor.file.VisorFileBlock;
import org.apache.ignite.internal.visor.log.VisorLogFile;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.lang.System.getProperty;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_DIR;
import static org.apache.ignite.events.EventType.EVTS_DISCOVERY;
import static org.apache.ignite.events.EventType.EVT_CLASS_DEPLOY_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED_OVER;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_REJECTED;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_JOB_TIMEDOUT;
import static org.apache.ignite.events.EventType.EVT_TASK_DEPLOY_FAILED;
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

    /** Throttle count for lost events. */
    private static final int EVENTS_LOST_THROTTLE = 10;

    /** Period to grab events. */
    private static final int EVENTS_COLLECT_TIME_WINDOW = 10 * 60 * 1000;

    /** Empty buffer for file block. */
    private static final byte[] EMPTY_FILE_BUF = new byte[0];

    /** Log files count limit */
    public static final int LOG_FILES_COUNT_LIMIT = 5000;

    /** */
    private static final int DFLT_BUFFER_SIZE = 4096;

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

    /** Only non task event types that Visor should collect. */
    public static final int[] VISOR_NON_TASK_EVTS = {
        EVT_CLASS_DEPLOY_FAILED,
        EVT_TASK_DEPLOY_FAILED
    };

    /** Only non task event types that Visor should collect. */
    public static final int[] VISOR_ALL_EVTS = concat(VISOR_TASK_EVTS, VISOR_NON_TASK_EVTS);

    /** Maximum folder depth. I.e. if depth is 4 we look in starting folder and 3 levels of sub-folders. */
    public static final int MAX_FOLDER_DEPTH = 4;

    /** Comparator for log files by last modified date. */
    private static final Comparator<VisorLogFile> LAST_MODIFIED = new Comparator<VisorLogFile>() {
        @Override public int compare(VisorLogFile f1, VisorLogFile f2) {
            return Long.compare(f2.lastModified(), f1.lastModified());
        }
    };

    /** Debug date format. */
    private static final ThreadLocal<SimpleDateFormat> DEBUG_DATE_FMT = new ThreadLocal<SimpleDateFormat>() {
        /** {@inheritDoc} */
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("HH:mm:ss,SSS");
        }
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

            sb.append("]");

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
     * Joins array elements to string.
     *
     * @param arr Array.
     * @return String.
     */
    @Nullable public static String compactArray(Object[] arr) {
        if (arr == null || arr.length == 0)
            return null;

        String sep = ", ";

        StringBuilder sb = new StringBuilder();

        for (Object s : arr)
            sb.append(s).append(sep);

        if (sb.length() > 0)
            sb.setLength(sb.length() - sep.length());

        return U.compact(sb.toString());
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property name.
     * @param dflt Function that returns {@code Integer}.
     * @return {@code Integer} value
     */
    public static Integer intValue(String propName, Integer dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Integer.getInteger(sysProp) : dflt;
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns {@code Boolean}.
     * @return {@code Boolean} value
     */
    public static boolean boolValue(String propName, boolean dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Boolean.getBoolean(sysProp) : dflt;
    }

    /**
     * Helper function to get value from map.
     *
     * @param map Map to take value from.
     * @param key Key to search in map.
     * @param ifNull Default value if {@code null} was returned by map.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Value from map or default value if map return {@code null}.
     */
    public static <K, V> V getOrElse(Map<K, V> map, K key, V ifNull) {
        assert map != null;

        V res = map.get(key);

        return res != null ? res : ifNull;
    }

    /**
     * Checks for explicit events configuration.
     *
     * @param ignite Grid instance.
     * @return {@code true} if all task events explicitly specified in configuration.
     */
    public static boolean checkExplicitTaskMonitoring(Ignite ignite) {
        int[] evts = ignite.configuration().getIncludeEventTypes();

        if (F.isEmpty(evts))
            return false;

        for (int evt : VISOR_TASK_EVTS) {
            if (!F.contains(evts, evt))
                return false;
        }

        return true;
    }

    /** Events comparator by event local order. */
    private static final Comparator<Event> EVTS_ORDER_COMPARATOR = new Comparator<Event>() {
        @Override public int compare(Event o1, Event o2) {
            return Long.compare(o1.localOrder(), o2.localOrder());
        }
    };

    /** Mapper from grid event to Visor data transfer object. */
    public static final VisorEventMapper EVT_MAPPER = new VisorEventMapper();

    /** Mapper from grid event to Visor data transfer object. */
    public static final VisorEventMapper EVT_MAPPER_V2 = new VisorEventMapper() {
        @Override protected VisorGridEvent discoveryEvent(DiscoveryEvent de, int type, IgniteUuid id, String name,
            UUID nid, long ts, String msg, String shortDisplay) {
            ClusterNode node = de.eventNode();

            return new VisorGridDiscoveryEventV2(type, id, name, nid, ts, msg, shortDisplay, node.id(),
                F.first(node.addresses()), node.isDaemon(), de.topologyVersion());
        }
    };

    /**
     * Grabs local events and detects if events was lost since last poll.
     *
     * @param ignite Target grid.
     * @param evtOrderKey Unique key to take last order key from node local map.
     * @param evtThrottleCntrKey Unique key to take throttle count from node local map.
     * @param all If {@code true} then collect all events otherwise collect only non task events.
     * @param evtMapper Closure to map grid events to Visor data transfer objects.
     * @return Collections of node events
     */
    public static Collection<VisorGridEvent> collectEvents(Ignite ignite, String evtOrderKey, String evtThrottleCntrKey,
        boolean all, IgniteClosure<Event, VisorGridEvent> evtMapper) {
        int[] evtTypes = all ? VISOR_ALL_EVTS : VISOR_NON_TASK_EVTS;

        // Collect discovery events for Web Console.
        if (evtOrderKey.startsWith("CONSOLE_"))
            evtTypes = concat(evtTypes, EVTS_DISCOVERY);

        return collectEvents(ignite, evtOrderKey, evtThrottleCntrKey, evtTypes, evtMapper);
    }

    /**
     * Grabs local events and detects if events was lost since last poll.
     *
     * @param ignite Target grid.
     * @param evtOrderKey Unique key to take last order key from node local map.
     * @param evtThrottleCntrKey Unique key to take throttle count from node local map.
     * @param evtTypes Event types to collect.
     * @param evtMapper Closure to map grid events to Visor data transfer objects.
     * @return Collections of node events
     */
    public static Collection<VisorGridEvent> collectEvents(Ignite ignite, String evtOrderKey, String evtThrottleCntrKey,
        int[] evtTypes, IgniteClosure<Event, VisorGridEvent> evtMapper) {
        assert ignite != null;
        assert evtTypes != null && evtTypes.length > 0;

        ConcurrentMap<String, Long> nl = ignite.cluster().nodeLocalMap();

        final long lastOrder = getOrElse(nl, evtOrderKey, -1L);
        final long throttle = getOrElse(nl, evtThrottleCntrKey, 0L);

        // When we first time arrive onto a node to get its local events,
        // we'll grab only last those events that not older than given period to make sure we are
        // not grabbing GBs of data accidentally.
        final long notOlderThan = System.currentTimeMillis() - EVENTS_COLLECT_TIME_WINDOW;

        // Flag for detecting gaps between events.
        final AtomicBoolean lastFound = new AtomicBoolean(lastOrder < 0);

        IgnitePredicate<Event> p = new IgnitePredicate<Event>() {
            /** */
            private static final long serialVersionUID = 0L;

            @Override public boolean apply(Event e) {
                // Detects that events were lost.
                if (!lastFound.get() && (lastOrder == e.localOrder()))
                    lastFound.set(true);

                // Retains events by lastOrder, period and type.
                return e.localOrder() > lastOrder && e.timestamp() > notOlderThan;
            }
        };

        Collection<Event> evts = ignite.events().localQuery(p, evtTypes);

        // Update latest order in node local, if not empty.
        if (!evts.isEmpty()) {
            Event maxEvt = Collections.max(evts, EVTS_ORDER_COMPARATOR);

            nl.put(evtOrderKey, maxEvt.localOrder());
        }

        // Update throttle counter.
        if (!lastFound.get())
            nl.put(evtThrottleCntrKey, throttle == 0 ? EVENTS_LOST_THROTTLE : throttle - 1);

        boolean lost = !lastFound.get() && throttle == 0;

        Collection<VisorGridEvent> res = new ArrayList<>(evts.size() + (lost ? 1 : 0));

        if (lost)
            res.add(new VisorGridEventsLost(ignite.cluster().localNode().id()));

        for (Event e : evts) {
            VisorGridEvent visorEvt = evtMapper.apply(e);

            if (visorEvt != null)
                res.add(visorEvt);
        }

        return res;
    }

    /**
     * Finds all files in folder and in it's sub-tree of specified depth.
     *
     * @param file Starting folder
     * @param maxDepth Depth of the tree. If 1 - just look in the folder, no sub-folders.
     * @param filter file filter.
     * @return List of found files.
     */
    public static List<VisorLogFile> fileTree(File file, int maxDepth, @Nullable FileFilter filter) {
        if (file.isDirectory()) {
            File[] files = (filter == null) ? file.listFiles() : file.listFiles(filter);

            if (files == null)
                return Collections.emptyList();

            List<VisorLogFile> res = new ArrayList<>(files.length);

            for (File f : files) {
                if (f.isFile() && f.length() > 0)
                    res.add(new VisorLogFile(f));
                else if (maxDepth > 1)
                    res.addAll(fileTree(f, maxDepth - 1, filter));
            }

            return res;
        }

        return F.asList(new VisorLogFile(file));
    }

    /**
     * @param fld Folder with files to match.
     * @param ptrn Pattern to match against file name.
     * @return Collection of matched files.
     */
    public static List<VisorLogFile> matchedFiles(File fld, final String ptrn) {
        List<VisorLogFile> files = fileTree(fld, MAX_FOLDER_DEPTH,
            new FileFilter() {
                @Override public boolean accept(File f) {
                    return !f.isHidden() && (f.isDirectory() || f.isFile() && f.getName().matches(ptrn));
                }
            }
        );

        Collections.sort(files, LAST_MODIFIED);

        return files;
    }

    /** Text files mime types. */
    private static final String[] TEXT_MIME_TYPE = new String[] {"text/plain", "application/xml", "text/html", "x-sh"};

    /**
     * Check is text file.
     *
     * @param f file reference.
     * @param emptyOk default value if empty file.
     * @return Is text file.
     */
    public static boolean textFile(File f, boolean emptyOk) {
        if (f.length() == 0)
            return emptyOk;

        String detected = VisorMimeTypes.getContentType(f);

        for (String mime : TEXT_MIME_TYPE)
            if (mime.equals(detected))
                return true;

        return false;
    }

    /**
     * Decode file charset.
     *
     * @param f File to process.
     * @return File charset.
     * @throws IOException in case of error.
     */
    public static Charset decode(File f) throws IOException {
        SortedMap<String, Charset> charsets = Charset.availableCharsets();

        String[] firstCharsets = {Charset.defaultCharset().name(), "US-ASCII", "UTF-8", "UTF-16BE", "UTF-16LE"};

        Collection<Charset> orderedCharsets = U.newLinkedHashSet(charsets.size());

        for (String c : firstCharsets)
            if (charsets.containsKey(c))
                orderedCharsets.add(charsets.get(c));

        orderedCharsets.addAll(charsets.values());

        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            FileChannel ch = raf.getChannel();

            ByteBuffer buf = ByteBuffer.allocate(DFLT_BUFFER_SIZE);

            ch.read(buf);

            buf.flip();

            for (Charset charset : orderedCharsets) {
                CharsetDecoder decoder = charset.newDecoder();

                decoder.reset();

                try {
                    decoder.decode(buf);

                    return charset;
                }
                catch (CharacterCodingException ignored) {
                }
            }
        }

        return Charset.defaultCharset();
    }

    /**
     * Read block from file.
     *
     * @param file - File to read.
     * @param off - Marker position in file to start read from if {@code -1} read last blockSz bytes.
     * @param blockSz - Maximum number of chars to read.
     * @param lastModified - File last modification time.
     * @return Read file block.
     * @throws IOException In case of error.
     */
    public static VisorFileBlock readBlock(File file, long off, int blockSz, long lastModified) throws IOException {
        RandomAccessFile raf = null;

        try {
            long fSz = file.length();
            long fLastModified = file.lastModified();

            long pos = off >= 0 ? off : Math.max(fSz - blockSz, 0);

            // Try read more that file length.
            if (fLastModified == lastModified && fSz != 0 && pos >= fSz)
                throw new IOException("Trying to read file block with wrong offset: " + pos + " while file size: " + fSz);

            if (fSz == 0)
                return new VisorFileBlock(file.getPath(), pos, fLastModified, 0, false, EMPTY_FILE_BUF);
            else {
                int toRead = Math.min(blockSz, (int)(fSz - pos));

                raf = new RandomAccessFile(file, "r");
                raf.seek(pos);

                byte[] buf = new byte[toRead];
                int cntRead = raf.read(buf, 0, toRead);

                if (cntRead != toRead)
                    throw new IOException("Count of requested and actually read bytes does not match [cntRead=" +
                        cntRead + ", toRead=" + toRead + ']');

                boolean zipped = buf.length > 512;

                return new VisorFileBlock(file.getPath(), pos, fSz, fLastModified, zipped, zipped ? zipBytes(buf) : buf);
            }
        }
        finally {
            U.close(raf, null);
        }
    }

    /**
     * Resolve IGFS profiler logs directory.
     *
     * @param igfs IGFS instance to resolve logs dir for.
     * @return {@link Path} to log dir or {@code null} if not found.
     * @throws IgniteCheckedException if failed to resolve.
     */
    public static Path resolveIgfsProfilerLogsDir(IgniteFileSystem igfs) throws IgniteCheckedException {
        String logsDir;

        if (igfs instanceof IgfsEx)
            logsDir = ((IgfsEx)igfs).clientLogDirectory();
        else if (igfs == null)
            throw new IgniteCheckedException("Failed to get profiler log folder (IGFS instance not found)");
        else
            throw new IgniteCheckedException("Failed to get profiler log folder (unexpected IGFS instance type)");

        URL logsDirUrl = U.resolveIgniteUrl(logsDir != null ? logsDir : DFLT_IGFS_LOG_DIR);

        return logsDirUrl != null ? new File(logsDirUrl.getPath()).toPath() : null;
    }

    /**
     * Extract max size from eviction policy if available.
     *
     * @param plc Eviction policy.
     * @return Extracted max size.
     */
    public static Integer evictionPolicyMaxSize(@Nullable EvictionPolicy plc) {
        if (plc instanceof LruEvictionPolicyMBean)
            return ((LruEvictionPolicyMBean)plc).getMaxSize();

        if (plc instanceof RandomEvictionPolicyMBean)
            return ((RandomEvictionPolicyMBean)plc).getMaxSize();

        if (plc instanceof FifoEvictionPolicyMBean)
            return ((FifoEvictionPolicyMBean)plc).getMaxSize();

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
            X.println(String.format("[%s][%s]%s",
                DEBUG_DATE_FMT.get().format(time), Thread.currentThread().getName(), msg));
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
        final long end = U.currentTimeMillis();

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
        log0(log, U.currentTimeMillis(),
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
        final long end = U.currentTimeMillis();

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
        log0(log, U.currentTimeMillis(), " " + msg);
    }

    /**
     * Checks if address can be reached using one argument InetAddress.isReachable() version or ping command if failed.
     *
     * @param addr Address to check.
     * @param reachTimeout Timeout for the check.
     * @return {@code True} if address is reachable.
     */
    public static boolean reachableByPing(InetAddress addr, int reachTimeout) {
        try {
            if (addr.isReachable(reachTimeout))
                return true;

            String cmd = String.format("ping -%s 1 %s", U.isWindows() ? "n" : "c", addr.getHostAddress());

            Process myProc = Runtime.getRuntime().exec(cmd);

            myProc.waitFor();

            return myProc.exitValue() == 0;
        }
        catch (IOException ignore) {
            return false;
        }
        catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();

            return false;
        }
    }

    /**
     * Start local node in terminal.
     *
     * @param log Logger.
     * @param cfgPath Path to node configuration to start with.
     * @param nodesToStart Number of nodes to start.
     * @param quite If {@code true} then start node in quiet mode.
     * @param envVars Optional map with environment variables.
     * @return List of started processes.
     * @throws IOException If failed to start.
     */
    public static List<Process> startLocalNode(@Nullable IgniteLogger log, String cfgPath, int nodesToStart,
        boolean quite, Map<String, String> envVars) throws IOException {
        String quitePar = quite ? "" : "-v";

        String cmdFile = new File("bin", U.isWindows() ? "ignite.bat" : "ignite.sh").getPath();

        File cmdFilePath = U.resolveIgnitePath(cmdFile);

        if (cmdFilePath == null || !cmdFilePath.exists())
            throw new FileNotFoundException(String.format("File not found: %s", cmdFile));

        String ignite = cmdFilePath.getCanonicalPath();

        File nodesCfgPath = U.resolveIgnitePath(cfgPath);

        if (nodesCfgPath == null || !nodesCfgPath.exists())
            throw new FileNotFoundException(String.format("File not found: %s", cfgPath));

        String nodeCfg = nodesCfgPath.getCanonicalPath();

        log(log, String.format("Starting %s local %s with '%s' config", nodesToStart, nodesToStart > 1 ? "nodes" : "node", nodeCfg));

        List<Process> run = new ArrayList<>();

        try {
            for (int i = 0; i < nodesToStart; i++) {
                if (U.isMacOs()) {
                    Map<String, String> macEnv = new HashMap<>(System.getenv());

                    if (envVars != null) {
                        for (Map.Entry<String, String> ent : envVars.entrySet())
                            if (macEnv.containsKey(ent.getKey())) {
                                String old = macEnv.get(ent.getKey());

                                if (old == null || old.isEmpty())
                                    macEnv.put(ent.getKey(), ent.getValue());
                                else
                                    macEnv.put(ent.getKey(), old + ':' + ent.getValue());
                            }
                            else
                                macEnv.put(ent.getKey(), ent.getValue());
                    }

                    StringBuilder envs = new StringBuilder();

                    for (Map.Entry<String, String> entry : macEnv.entrySet()) {
                        String val = entry.getValue();

                        if (val.indexOf(';') < 0 && val.indexOf('\'') < 0)
                            envs.append(String.format("export %s='%s'; ",
                                    entry.getKey(), val.replace('\n', ' ').replace("'", "\'")));
                    }

                    run.add(openInConsole(envs.toString(), ignite, quitePar, nodeCfg));
                } else
                    run.add(openInConsole(null, envVars, ignite, quitePar, nodeCfg));
            }

            return run;
        }
        catch (Exception e) {
            for (Process proc: run)
                proc.destroy();

            throw e;
        }
    }

    /**
     * Run command in separated console.
     *
     * @param args A string array containing the program and its arguments.
     * @return Started process.
     * @throws IOException in case of error.
     */
    public static Process openInConsole(String... args) throws IOException {
        return openInConsole(null, args);
    }

    /**
     * Run command in separated console.
     *
     * @param workFolder Work folder for command.
     * @param args A string array containing the program and its arguments.
     * @return Started process.
     * @throws IOException in case of error.
     */
    public static Process openInConsole(@Nullable File workFolder, String... args) throws IOException {
        return openInConsole(workFolder, null, args);
    }

    /**
     * Run command in separated console.
     *
     * @param workFolder Work folder for command.
     * @param envVars Optional map with environment variables.
     * @param args A string array containing the program and its arguments.
     * @return Started process.
     * @throws IOException If failed to start process.
     */
    public static Process openInConsole(@Nullable File workFolder, Map<String, String> envVars, String... args)
        throws IOException {
        String[] commands = args;

        String cmd = F.concat(Arrays.asList(args), " ");

        if (U.isWindows())
            commands = F.asArray("cmd", "/c", String.format("start %s", cmd));

        if (U.isMacOs())
            commands = F.asArray("osascript", "-e",
                String.format("tell application \"Terminal\" to do script \"%s\"", cmd));

        if (U.isUnix())
            commands = F.asArray("xterm", "-sl", "1024", "-geometry", "200x50", "-e", cmd);

        ProcessBuilder pb = new ProcessBuilder(commands);

        if (workFolder != null)
            pb.directory(workFolder);

        if (envVars != null) {
            String sep = U.isWindows() ? ";" : ":";

            Map<String, String> goalVars = pb.environment();

            for (Map.Entry<String, String> var: envVars.entrySet()) {
                String envVar = goalVars.get(var.getKey());

                if (envVar == null || envVar.isEmpty())
                    envVar = var.getValue();
                else
                    envVar += sep + var.getValue();

                goalVars.put(var.getKey(), envVar);
            }
        }

        return pb.start();
    }

    /**
     * Zips byte array.
     *
     * @param input Input bytes.
     * @return Zipped byte array.
     * @throws IOException If failed.
     */
    public static byte[] zipBytes(byte[] input) throws IOException {
        return zipBytes(input, DFLT_BUFFER_SIZE);
    }

    /**
     * Zips byte array.
     *
     * @param input Input bytes.
     * @param initBufSize Initial buffer size.
     * @return Zipped byte array.
     * @throws IOException If failed.
     */
    public static byte[] zipBytes(byte[] input, int initBufSize) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(initBufSize);

        try (ZipOutputStream zos = new ZipOutputStream(bos)) {
            try {
                ZipEntry entry = new ZipEntry("");

                entry.setSize(input.length);

                zos.putNextEntry(entry);
                zos.write(input);
            }
            finally {
                zos.closeEntry();
            }
        }

        return bos.toByteArray();
    }

    /**
     * @param msg Exception message.
     * @return {@code true} if node failed to join grid.
     */
    public static boolean joinTimedOut(String msg) {
        return msg != null && msg.startsWith("Join process timed out.");
    }
}
