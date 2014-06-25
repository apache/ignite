/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.gui.dto.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopLogger.*;
import static org.gridgain.grid.kernal.visor.gui.tasks.VisorHadoopTaskUtilsEnt.*;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * Task that parse hadoop profiler logs.
 */
@GridInternal
public class VisorGgfsProfilerTask extends VisorOneNodeTask<String, Collection<VisorGgfsProfilerEntry>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Holder class for parsed data.
     */
    private static class VisorGgfsProfilerParsedLine {
        /** Timestamp. */
        private final long ts;

        /** Log entry type. */
        private final int entryType;

        /** File path. */
        private final String path;

        /** File GGFS mode. */
        private final GridGgfsMode mode;

        /** Stream ID. */
        private final long streamId;

        /** Data length. */
        private final long dataLen;

        /** Overwrite flag. Available only for OPEN_OUT event. */
        private final boolean overwrite;

        /** Position of data being randomly read or seek. Available only for RANDOM_READ or SEEK events. */
        private final long pos;

        /** User time. Available only for CLOSE_IN/CLOSE_OUT events. */
        private final long userTime;

        /** System time (either read or write). Available only for CLOSE_IN/CLOSE_OUT events. */
        private final long sysTime;

        /** Total amount of read or written bytes. Available only for CLOSE_IN/CLOSE_OUT events.*/
        private final long totalBytes;

        /**
         * Create holder for log line.
         */
        private VisorGgfsProfilerParsedLine(
            long ts,
            int entryType,
            String path,
            GridGgfsMode mode,
            long streamId,
            long dataLen,
            boolean overwrite,
            long pos,
            long userTime,
            long sysTime,
            long totalBytes
        ) {
            this.ts = ts;
            this.entryType = entryType;
            this.path = path;
            this.mode = mode;
            this.streamId = streamId;
            this.dataLen = dataLen;
            this.overwrite = overwrite;
            this.pos = pos;
            this.userTime = userTime;
            this.sysTime = sysTime;
            this.totalBytes = totalBytes;
        }
    }

    /**
     * Comparator to sort parsed log lines by timestamp.
     */
    private static final Comparator<VisorGgfsProfilerParsedLine> PARSED_LINE_BY_TS_COMPARATOR =
        new Comparator<VisorGgfsProfilerParsedLine>() {
            @Override public int compare(VisorGgfsProfilerParsedLine a, VisorGgfsProfilerParsedLine b) {
                return a.ts < b.ts ? -1
                    : a.ts > b.ts ? 1
                    : 0;
            }
    };

    /**
     * Job that do actual profiler work.
     */
    private static class VisorGgfsProfilerJob extends VisorJob<String, Collection<VisorGgfsProfilerEntry>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        private VisorGgfsProfilerJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorGgfsProfilerEntry> run(String arg) throws GridException {
            try {
                Path logsDir = resolveGgfsProfilerLogsDir(g.ggfs(arg));

                if (logsDir != null)
                    return parse(logsDir, arg);
                else
                    return Collections.emptyList();
            }
            catch (IOException | IllegalArgumentException e) {
                throw new GridException("Failed to parse profiler logs for GGFS: " + arg, e);
            }
        }

        /**
         * Parse boolean.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @return Parsed boolean.
         */
        private boolean parseBoolean(String[] ss, int ix) {
            return ix < ss.length && "1".equals(ss[ix]);
        }

        /**
         * Parse integer.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @param dflt Default value if string is empty or index is out of array bounds.
         * @return Parsed integer.
         * @exception NumberFormatException  if the string does not contain a parsable integer.
         */
        private int parseInt(String[] ss, int ix, int dflt) {
            if (ss.length <= ix)
                return dflt;
            else {
                String s = ss[ix];

                return s.isEmpty() ? dflt : Integer.parseInt(s);
            }
        }

        /**
         * Parse long.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @param dflt Default value if string is empty or index is out of array bounds.
         * @return Parsed integer.
         * @exception NumberFormatException if the string does not contain a parsable long.
         */
        private long parseLong(String[] ss, int ix, long dflt) {
            if (ss.length <= ix)
                return dflt;
            else {
                String s = ss[ix];

                return s.isEmpty() ? dflt : Long.parseLong(s);
            }
        }

        /**
         * Parse string.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @return Parsed string.
         */
        private String parseString(String[] ss, int ix) {
            if (ss.length <= ix)
                return "";
            else {
                String s = ss[ix];

                return s.isEmpty() ? "" : s;
            }
        }

        /**
         * Parse GGFS mode from string.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @return Parsed GGFS mode or {@code null} if string is empty.
         */
        private GridGgfsMode parseGgfsMode(String[] ss, int ix) {
            if (ss.length <= ix)
                return null;
            else {
                String s = ss[ix];

                return s.isEmpty() ? null : GridGgfsMode.valueOf(s);
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

            long streamId = parseLong(ss, LOG_COL_STREAM_ID, -1);

            if (streamId >= 0) {
                int entryType = parseInt(ss, LOG_COL_ENTRY_TYPE, -1);

                // Parse only needed types.
                if (LOG_TYPES.contains(entryType))
                    return new VisorGgfsProfilerParsedLine(
                        parseLong(ss, LOG_COL_TIMESTAMP, 0),
                        entryType,
                        parseString(ss, LOG_COL_PATH),
                        parseGgfsMode(ss, LOG_COL_GGFS_MODE),
                        streamId,
                        parseLong(ss, LOG_COL_DATA_LEN, 0),
                        parseBoolean(ss, LOG_COL_OVERWRITE),
                        parseLong(ss, LOG_COL_POS, 0),
                        parseLong(ss, LOG_COL_USER_TIME, 0),
                        parseLong(ss, LOG_COL_SYSTEM_TIME, 0),
                        parseLong(ss, LOG_COL_TOTAL_BYTES, 0)
                    );
            }

            return null;
        }

        /**
         * Aggregate information from parsed lines grouped by {@code streamId}.
         */
        private VisorGgfsProfilerEntry aggregateParsedLines(List<VisorGgfsProfilerParsedLine> lines) {
            VisorGgfsProfilerUniformityCounters counters = new VisorGgfsProfilerUniformityCounters();

            Collections.sort(lines, PARSED_LINE_BY_TS_COMPARATOR);

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

            for (VisorGgfsProfilerParsedLine line : lines) {
                if (!line.path.isEmpty())
                    path = line.path;

                ts = line.ts; // Remember last timestamp.

                // Remember last GGFS mode.
                if (line.mode != null)
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

            // Return only fully parsed data with path.
            return path.isEmpty() ? null :
                new VisorGgfsProfilerEntry(
                    path,
                    ts,
                    mode,
                    size,
                    bytesRead,
                    readTime,
                    userReadTime,
                    bytesWritten,
                    writeTime,
                    userWriteTime,
                    counters);
        }

        /**
         * @param p Path to log file to parse.
         * @return Collection of parsed and aggregated entries.
         * @throws IOException if failed to read log file.
         */
        private Collection<VisorGgfsProfilerEntry> parseFile(Path p) throws IOException {
            List<VisorGgfsProfilerParsedLine> parsedLines = new ArrayList<>(512);

            try (BufferedReader br = Files.newBufferedReader(p, Charset.forName("UTF-8"))) {
                String line = br.readLine(); // Skip first line with columns header.

                if (line != null) {
                    // Check file header.
                    if (line.equalsIgnoreCase(HDR))
                        line = br.readLine();

                        while (line != null) {
                            try {
                                VisorGgfsProfilerParsedLine ln = parseLine(line);

                                if (ln != null)
                                    parsedLines.add(ln);
                            }
                            catch (NumberFormatException ignored) {
                                // Skip invalid lines.
                            }

                            line = br.readLine();
                        }
                }
            }

            // Group parsed lines by streamId.
            Map<Long, List<VisorGgfsProfilerParsedLine>> byStreamId = new HashMap<>();

            for (VisorGgfsProfilerParsedLine line: parsedLines) {
                List<VisorGgfsProfilerParsedLine> grp = byStreamId.get(line.streamId);

                if (grp == null) {
                    grp = new ArrayList<>();

                    byStreamId.put(line.streamId, grp);
                }

                grp.add(line);
            }

            // Aggregate each group.
            Collection<VisorGgfsProfilerEntry> entries = new ArrayList<>(byStreamId.size());

            for (List<VisorGgfsProfilerParsedLine> lines : byStreamId.values()) {
                VisorGgfsProfilerEntry entry = aggregateParsedLines(lines);

                if (entry != null)
                    entries.add(entry);
            }

            // Group by files.
            Map<String, List<VisorGgfsProfilerEntry>> byPath = new HashMap<>();

            for (VisorGgfsProfilerEntry entry: entries) {
                List<VisorGgfsProfilerEntry> grp = byPath.get(entry.path());

                if (grp == null) {
                    grp = new ArrayList<>();

                    byPath.put(entry.path(), grp);
                }

                grp.add(entry);
            }

            // Aggregate by files.
            Collection<VisorGgfsProfilerEntry> res = new ArrayList<>(byPath.size());

            for (List<VisorGgfsProfilerEntry> lst : byPath.values())
                res.add(VisorGgfsProfiler.aggregateGgfsProfilerEntries(lst));

            return res;
        }

        /**
         * Parse all GGFS log files in specified log directory.
         *
         * @param logDir Folder were log files located.
         * @return List of line with aggregated information by files.
         */
        private Collection<VisorGgfsProfilerEntry> parse(Path logDir, String ggfsName) throws IOException {
            Collection<VisorGgfsProfilerEntry> parsedFiles = new ArrayList<>(512);

            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(logDir)) {
                PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:ggfs-log-" + ggfsName + "-*.csv");

                for (Path p : dirStream) {
                    if (matcher.matches(p.getFileName())) {
                        try {
                            parsedFiles.addAll(parseFile(p));
                        }
                        catch (NoSuchFileException ignored) {
                            // Files was deleted, skip it.
                        }
                        catch (Exception e) {
                            g.log().warning("Failed to parse GGFS profiler log file: " + p, e);
                        }
                    }
                }
            }

            return parsedFiles;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGgfsProfilerJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorGgfsProfilerJob job(String arg) {
        return new VisorGgfsProfilerJob(arg);
    }
}
