/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.GridGgfsMode;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.gui.dto.*;

import static org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopLogger.*;
import static org.gridgain.grid.kernal.visor.gui.tasks.VisorHadoopTaskUtilsEnt.*;

import java.nio.file.*;
import java.util.*;

/**
 * Task that parse hadoop profiler logs.
 */
@GridInternal
public class VisorGgfsProfilerTask extends VisorOneNodeTask<VisorGgfsProfilerTask.VisorGgfsProfilerArg, Collection<VisorGgfsProfilerEntry>> {
    /**
     * Arguments for {@link VisorGgfsProfilerTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String ggfsName;

        /**
         * @param nodeId Node Id.
         * @param ggfsName GGFS instance name.
         */
        public VisorGgfsProfilerArg(UUID nodeId, String ggfsName) {
            super(nodeId);

            this.ggfsName = ggfsName;
        }
    }

    /** Holder class for parsed data. */
    public static class VisorGgfsProfilerParsedLine {
        public long ts = 0;
        public int entryType = 0;
        public String path;
        public GridGgfsMode mode;
        public long streamId;
        public long dataLen;
        public boolean append;
        public boolean overwrite;
        public long pos;
        public int readLen;
        public long userTime;
        public long sysTime;
        public long totalBytes;

        public VisorGgfsProfilerParsedLine(long l, int entryType, String s, GridGgfsMode gridGgfsMode, long streamId, long l1, boolean b, boolean b1, long l2, int i, long l3, long l4, long l5) {

        }
    }


    /**
     * Job that do actual profiler work.
     */
    @SuppressWarnings("PublicInnerClass")
    class VisorGgfsProfilerJob extends VisorOneNodeJob<VisorGgfsProfilerArg, Collection<VisorGgfsProfilerEntry>> {
        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        public VisorGgfsProfilerJob(VisorGgfsProfilerArg arg) {
            super(arg);
        }

        @Override
        protected Collection<VisorGgfsProfilerEntry> run(VisorGgfsProfilerArg arg) throws GridException {
            try {
                Path logsDir = resolveGgfsProfilerLogsDir(g.ggfs(arg.ggfsName));

                if (logsDir != null)
                    return null; // TODO parse(logsDir, g, arg.ggfsName);
                else
                    return Collections.emptyList();
            } catch (IllegalArgumentException iae) {
                throw new GridException("Failed to parse profiler logs for GGFS: " + arg.ggfsName);
            }
        }

        /**
         * Parse boolean.
         */
        private boolean parseBoolean(String[] ss, int sz, int ix, boolean dflt) {
            if (sz <= ix)
                return dflt;
            else if ("0".equals(ss[ix]))
                return false;
            else if ("1".equals(ss[ix]))
                return true;
            else
                return dflt;
        }

        private int parseInt(String[] ss, int sz, int ix, int dflt) {
            if (sz <= ix)
                return dflt;
            else {
                String s = ss[ix];

                return s.isEmpty() ? dflt : Integer.parseInt(s);
            }
        }

        private long parseLong(String[] ss, int sz, int ix, long dflt) {
            if (sz <= ix)
                return dflt;
            else {
                String s = ss[ix];

                return s.isEmpty() ? dflt : Long.parseLong(s);
            }
        }

        private String parseString(String[] ss, int sz, int ix, String dflt) {
            if (sz <= ix)
                return dflt;
            else {
                String s = ss[ix];

                return s.isEmpty() ? dflt : s;
            }
        }

        /**
         * Parse line from log.
         *
         * @param s Line with text to parse.
         * @return Parsed data.
         */
        private VisorGgfsProfilerParsedLine parseLine(String s) {
            String[] ss = s.split(DELIM_FIELD);
            int sz = ss.length;

            long streamId = parseLong(ss, sz, LOG_COL_STREAM_ID, -1);

            if (streamId >= 0) {
                int entryType = parseInt(ss, sz, LOG_COL_ENTRY_TYPE, -1);

                // Parse only needed types.
                if (LOG_TYPES.contains(entryType))
                    return new VisorGgfsProfilerParsedLine(
                            parseLong(ss, sz, LOG_COL_TIMESTAMP, 0),
                            entryType,
                            parseString(ss, sz, LOG_COL_PATH, ""),
                            GridGgfsMode.valueOf(parseString(ss, sz, LOG_COL_GGFS_MODE, "")),
                            streamId,
                            parseLong(ss, sz, LOG_COL_DATA_LEN, 0),
                            parseBoolean(ss, sz, LOG_COL_APPEND, false),
                            parseBoolean(ss, sz, LOG_COL_OVERWRITE, false),
                            parseLong(ss, sz, LOG_COL_POS, 0),
                            parseInt(ss, sz, LOG_COL_READ_LEN, 0),
                            parseLong(ss, sz, LOG_COL_USER_TIME, 0),
                            parseLong(ss, sz, LOG_COL_SYSTEM_TIME, 0),
                            parseLong(ss, sz, LOG_COL_TOTAL_BYTES, 0)
                    );
            }

            return null;
        }

        /**
         * Aggregate information from parsed lines grouped by `streamId`.
         */
        private VisorGgfsProfilerEntry aggregateParsedLines(ArrayList<VisorGgfsProfilerParsedLine> lines) {
            String path = "";
            long ts = 0;
            long size = 0;
            long bytesRead = 0;
            long readTime = 0;
            long userReadTime = 0;
            long bytesWritten = 0;
            long writeTime = 0;
            long userWriteTime = 0;
            GridGgfsMode mode = null;

            VisorGgfsProfilerUniformityCounters counters = new VisorGgfsProfilerUniformityCounters();

            //lines.sortBy(_.ts).foreach(line => {
            for (VisorGgfsProfilerParsedLine line : lines) {
                if (!line.path.isEmpty())
                    path = line.path;

                ts = line.ts; // Remember last timestamp.

                // Remember last GGFS mode.
                mode = line.mode;

                switch (line.entryType) {
                    case TYPE_OPEN_IN:
                        size = line.dataLen; // Remember last file size.

                        counters.invalidate(size);
                        break;

                    case TYPE_OPEN_OUT:
                        if (line.overwrite) {
                            size = 0; // If file was overridden, set size to zero.

                            counters.invalidate(size);
                        }
                        break;

                    case TYPE_CLOSE_IN:
                        bytesRead += line.totalBytes; // Add to total bytes read.
                        readTime += line.sysTime; // Add to read time.
                        userReadTime += line.userTime; // Add to user read time.

                        counters.increment(line.pos, line.totalBytes);

                        break;

                    case TYPE_CLOSE_OUT:
                        size += line.totalBytes; // Add to files size.
                        bytesWritten += line.totalBytes; // Add to total bytes written.
                        writeTime += line.sysTime; // Add to write time.
                        userWriteTime += line.userTime; // Add to user write time.

                        counters.invalidate(size);

                        break;

                    case TYPE_RANDOM_READ:
                        counters.increment(line.pos, line.totalBytes);

                        break;

                    default:
                        throw new IllegalStateException("Unexpected GGFS profiler log entry type: " + line.entryType);
                }
            }

//            // Return only fully parsed data with path.
//            if (!path.isEmpty())
//                return new VisorGgfsProfilerEntry(
//                        path,
//                        ts,
//                        mode,
//                        size,
//                        bytesRead,
//                        readTime,
//                        userReadTime,
//                        bytesWritten,
//                        writeTime,
//                        userWriteTime,
//                        counters);
//            else
                return null;
        }
    }

    @Override protected VisorJob<VisorGgfsProfilerArg, Collection<VisorGgfsProfilerEntry>> job(
        VisorGgfsProfilerArg arg) {
        return null; // TODO: CODE: implement.
    }
}

//    object VisorGgfsProfilerTask {

//
//
//    def parseFile(p: Path): Iterable[VisorGgfsProfilerEntry] = {
//    val parsedLines = new collection.mutable.ArrayBuffer[VisorGgfsProfilerParsedLine](512)
//
//    val br = Files.newBufferedReader(p, Charset.forName("UTF-8"))
//
//    try {
//    var line = br.readLine() // Skip first line with columns header.
//
//    if (line != null) {
//    // Check file header.
//    if (line.equalsIgnoreCase(GGHL.HDR))
//    do {
//    line = br.readLine()
//
//    if (line != null)
//    try
//    parseLine(line).foreach(ln => parsedLines += ln)
//    catch {
//    case nfe: NumberFormatException => // Skip invalid lines.
//    }
//    }
//    while (line != null)
//    }
//    }
//    finally {
//    br.close()
//    }
//
//    parsedLines.groupBy(_.streamId) // Group parsed lines by streamId.
//    .values.map(aggregateParsedLines) // Aggregate each group.
//    .flatten // Skip empty aggregation results.
//    .groupBy(_.path) // Group by files.
//    .values.map(aggregateGgfsProfilerEntries) // Aggregate by files.
//    }
//
//    /**
//     * Parse all GGFS log files in specified log directory.
//     *
//     * @param logDir Folder were log files located.
//     * @return List of line with aggregated information by files.
//     */
//    def parse(logDir: Path, g: GridEx, ggfsName: String): List[VisorGgfsProfilerEntry] = {
//    val matcher = FileSystems.getDefault.getPathMatcher("glob:ggfs-log-" + ggfsName + "-*.csv")
//    val dirStream = Files.newDirectoryStream(logDir)
//
//    try {
//    val parsedFiles = new collection.mutable.ArrayBuffer[Iterable[VisorGgfsProfilerEntry]](512)
//
//    dirStream.iterator()
//    .filter(p => matcher.matches(p.getFileName))
//    .foreach(p => {
//    try {
//    parsedFiles += parseFile(p)
//    }
//    catch {
//    case nsfe: NoSuchFileException => () // Files was deleted, skip it.
//
//    case e: Exception => g.log().warning("Failed to parse GGFS profiler log file: " + p, e)
//    }
//    })
//
//    parsedFiles.flatten.toList
//    }
//    finally {
//    dirStream.close()
//    }
//    }
//    }
//
///** Wrapper class to hold parsed data. */
//    case class VisorGgfsProfilerParsedLine(
//    ts: Long,
//    entryType: Int,
//    path: String,
//    mode: Option[GridGgfsMode],
//    streamId: Long,
//    dataLen: Long,
//    append: Boolean,
//    overwrite: Boolean,
//    pos: Long,
//    readLen: Int,
//    userTime: Long,
//    sysTime: Long,
//    totalBytes: Long)

