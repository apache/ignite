/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.event.*;
import org.gridgain.grid.kernal.visor.gui.dto.*;
import org.gridgain.grid.kernal.visor.gui.tasks.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Contains utility methods for Visor tasks and jobs.
 */
@SuppressWarnings("ExtendsUtilityClass")
public class VisorTaskUtilsEnt extends VisorTaskUtils {
    /** Throttle count for lost events. */
    private static final int EVENTS_LOST_THROTTLE = 10;

    /** Period to grab events. */
    private static final int EVENTS_COLLECT_TIME_WINDOW = 10 * 60 * 1000;

    /** Empty buffer for file block. */
    private static final byte[] EMPTY_FILE_BUF = new byte[0];

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
    private static final int[] VISOR_NON_TASK_EVTS = {
        EVT_CLASS_DEPLOY_FAILED,
        EVT_TASK_DEPLOY_FAILED,

        EVT_LIC_CLEARED,
        EVT_LIC_VIOLATION,
        EVT_LIC_GRACE_EXPIRED
    };

    /**
     * Maximum folder depth. I.e. if depth is 4 we look in starting folder and 3 levels of sub-folders.
     */
    public static final int MAX_FOLDER_DEPTH = 4;

    private static final Comparator<VisorLogFile> LAST_MODIFIED = new Comparator<VisorLogFile>() {
        @Override public int compare(VisorLogFile f1, VisorLogFile f2) {
            return Long.compare(f2.lastModified(), f1.lastModified());
        }
    };

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

        return res != null? res : ifNull;
    }

    /**
     * Checks for explicit events configuration.
     *
     * @param g Grid instance.
     * @return {@code true} if all task events explicitly specified in configuration.
     */
    public static boolean checkExplicitTaskMonitoring(Grid g) {
        int[] evts = g.configuration().getIncludeEventTypes();

        if (F.isEmpty(evts))
            return false;

        for (int evt : VISOR_TASK_EVTS) {
            if (!F.contains(evts, evt))
                return false;
        }

        return true;
    }

    private static final Comparator<GridEvent> EVENTS_ORDER_COMPARATOR = new Comparator<GridEvent>() {
        @Override public int compare(GridEvent o1, GridEvent o2) {
            return Long.compare(o1.localOrder(), o2.localOrder());
        }
    };

    /**
     * Grabs local events and detects if events was lost since last poll.
     *
     * @param g Target grid.
     * @param evtOrderKey Unique key to take last order key from node local map.
     * @param evtThrottleCntrKey  Unique key to take throttle count from node local map.
     * @param all If {@code true} then collect all events otherwise collect only non task events.
     * @return Collections of node events
     */
    public static Collection<VisorGridEvent> collectEvents(Grid g, String evtOrderKey, String evtThrottleCntrKey,
        final boolean all) {
        assert g != null;

        GridNodeLocalMap<String, Long> nl = g.nodeLocalMap();

        final long lastOrder = getOrElse(nl, evtOrderKey, -1L);
        final long throttle = getOrElse(nl, evtThrottleCntrKey, 0L);

        // When we first time arrive onto a node to get its local events,
        // we'll grab only last those events that not older than given period to make sure we are
        // not grabbing GBs of data accidentally.
        final long notOlderThan = System.currentTimeMillis() - EVENTS_COLLECT_TIME_WINDOW;

        // Flag for detecting gaps between events.
        final AtomicBoolean lastFound = new AtomicBoolean(lastOrder < 0);

        GridPredicate<GridEvent> p = new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent e) {
                // Detects that events were lost.
                if (!lastFound.get() && (lastOrder == e.localOrder()))
                    lastFound.set(true);

                // Retains events by lastOrder, period and type.
                return e.localOrder() > lastOrder && e.timestamp() > notOlderThan &&
                        all ? (F.contains(VISOR_TASK_EVTS, e.type()) || F.contains(VISOR_NON_TASK_EVTS, e.type()))
                            : F.contains(VISOR_NON_TASK_EVTS, e.type());
            }
        };

        Collection<GridEvent> evts = g.events().localQuery(p);

        // Update latest order in node local, if not empty.
        if (!evts.isEmpty()) {
            GridEvent maxEvt = Collections.max(evts, EVENTS_ORDER_COMPARATOR);

            nl.put(evtOrderKey, maxEvt.localOrder());
        }

        // Update throttle counter.
        if (!lastFound.get())
            nl.put(evtThrottleCntrKey, throttle == 0 ? EVENTS_LOST_THROTTLE : throttle - 1);

        boolean lost = !lastFound.get() && throttle == 0;

        Collection<VisorGridEvent> res = new ArrayList<>(evts.size() + (lost ? 1 : 0));

        if (lost)
            res.add(new VisorGridEventsLost(g.localNode().id()));

        for (GridEvent e : evts) {
            int tid = e.type();
            GridUuid id = e.id();
            String name = e.name();
            UUID nid = e.node().id();
            long t = e.timestamp();
            String msg = e.message();
            String shortDisplay = e.shortDisplay();

            if (e instanceof GridTaskEvent) {
                GridTaskEvent te = (GridTaskEvent)e;

                res.add(new VisorGridTaskEvent(tid, id, name, nid, t, msg, shortDisplay,
                        te.taskName(), te.taskClassName(), te.taskSessionId(), te.internal()));
            }
            else if (e instanceof GridJobEvent) {
                GridJobEvent je = (GridJobEvent)e;

                res.add(new VisorGridJobEvent(tid, id, name, nid, t, msg, shortDisplay,
                        je.taskName(), je.taskClassName(), je.taskSessionId(), je.jobId()));
            }
            else if (e instanceof GridDeploymentEvent) {
                GridDeploymentEvent de = (GridDeploymentEvent)e;

                res.add(new VisorGridDeploymentEvent(tid, id, name, nid, t, msg, shortDisplay, de.alias()));
            }
            else if (e instanceof GridLicenseEvent) {
                GridLicenseEvent le = (GridLicenseEvent)e;

                res.add(new VisorGridLicenseEvent(tid, id, name, nid, t, msg, shortDisplay, le.licenseId()));
            }
        }

        return res;
    }

    /**
     * Finds all files in folder and in it's sub-tree of specified depth.
     *
     * @param file Starting folder
     * @param maxDepth Depth of the tree. If 1 - just look in the folder, no sub-folders.
     * @param filter file filter.
     */
    public static List<VisorLogFile> fileTree(File file, int maxDepth, @Nullable FileFilter filter) {
        if (file.isDirectory()) {
            File[] files = (filter == null) ? file.listFiles() : file.listFiles(filter);

            if (files == null)
                return Collections.emptyList();

            List<VisorLogFile> res = new ArrayList<>(files.length);

            for (File f : files) {
                if (f.isFile())
                    res.add(new VisorLogFile(f));
                else if (maxDepth > 1)
                    res.addAll(fileTree(f, maxDepth - 1, filter));
            }

            return res;
        }

        if (filter == null || filter.accept(file))
            return F.asList(new VisorLogFile(file));

        return Collections.emptyList();
    }

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
    private static final String[] TEXT_MIME_TYPE = new String[]{ "text/plain", "application/xml", "text/html", "x-sh" };

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

    public static Charset decode(File f) throws IOException {
        SortedMap<String, Charset> charsets = Charset.availableCharsets();

        String[] firstCharsets = {Charset.defaultCharset().name(), "US-ASCII", "UTF-8", "UTF-16BE", "UTF-16LE"};

        Collection<Charset> orderedCharsets = new LinkedHashSet<>(charsets.size());

        for (String c : firstCharsets)
            if (charsets.containsKey(c))
                orderedCharsets.add(charsets.get(c));

        orderedCharsets.addAll(charsets.values());

        try (RandomAccessFile raf = new RandomAccessFile(f, "r")) {
            FileChannel channel = raf.getChannel();

            ByteBuffer buf = ByteBuffer.allocate(4096);

            channel.read(buf);

            buf.flip();

            for (Charset charset : orderedCharsets) {
                CharsetDecoder decoder = charset.newDecoder();

                decoder.reset();

                try {
                    decoder.decode(buf);

                    return charset;
                } catch (CharacterCodingException ignored) { }
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

                byte[] buf = new byte[toRead];

                raf = new RandomAccessFile(file, "r");

                raf.seek(pos);

                int cntRead = raf.read(buf, 0, toRead);

                if (cntRead != toRead)
                    throw new IOException("Count of requested and actually read bytes does not match [cntRead=" +
                        cntRead + ", toRead=" + toRead + ']');

                boolean zipped = buf.length > 512;

                return new VisorFileBlock(file.getPath(), pos, fSz, fLastModified, zipped, zipped ? U.zipBytes(buf) : buf);
            }
        }
        finally {
            U.close(raf, null);
        }
    }
}
