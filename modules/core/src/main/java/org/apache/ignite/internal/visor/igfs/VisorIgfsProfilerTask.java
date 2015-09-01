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

package org.apache.ignite.internal.visor.igfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

import static org.apache.ignite.internal.igfs.common.IgfsLogger.DELIM_FIELD;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.HDR;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_CLOSE_IN;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_CLOSE_OUT;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_OPEN_IN;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_OPEN_OUT;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_RANDOM_READ;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.resolveIgfsProfilerLogsDir;

/**
 * Task that parse hadoop profiler logs.
 */
@GridInternal
public class VisorIgfsProfilerTask extends VisorOneNodeTask<String, Collection<VisorIgfsProfilerEntry>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Holder class for parsed data.
     */
    private static class VisorIgfsProfilerParsedLine {
        /** Timestamp. */
        private final long ts;

        /** Log entry type. */
        private final int entryType;

        /** File path. */
        private final String path;

        /** File IGFS mode. */
        private final IgfsMode mode;

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

        /** Total amount of read or written bytes. Available only for CLOSE_IN/CLOSE_OUT events. */
        private final long totalBytes;

        /**
         * Create holder for log line.
         */
        private VisorIgfsProfilerParsedLine(
            long ts,
            int entryType,
            String path,
            IgfsMode mode,
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
    private static final Comparator<VisorIgfsProfilerParsedLine> PARSED_LINE_BY_TS_COMPARATOR =
        new Comparator<VisorIgfsProfilerParsedLine>() {
            @Override public int compare(VisorIgfsProfilerParsedLine a, VisorIgfsProfilerParsedLine b) {
                return a.ts < b.ts ? -1
                    : a.ts > b.ts ? 1
                    : 0;
            }
        };

    /**
     * Job that do actual profiler work.
     */
    private static class VisorIgfsProfilerJob extends VisorJob<String, Collection<VisorIgfsProfilerEntry>> {
        /** */
        private static final long serialVersionUID = 0L;

        // Named column indexes in log file.
        /** */
        private static final int LOG_COL_TIMESTAMP = 0;
        /** */
        private static final int LOG_COL_THREAD_ID = 1;
        /** */
        private static final int LOG_COL_ENTRY_TYPE = 3;
        /** */
        private static final int LOG_COL_PATH = 4;
        /** */
        private static final int LOG_COL_IGFS_MODE = 5;
        /** */
        private static final int LOG_COL_STREAM_ID = 6;
        /** */
        private static final int LOG_COL_DATA_LEN = 8;
        /** */
        private static final int LOG_COL_OVERWRITE = 10;
        /** */
        private static final int LOG_COL_POS = 13;
        /** */
        private static final int LOG_COL_USER_TIME = 17;
        /** */
        private static final int LOG_COL_SYSTEM_TIME = 18;
        /** */
        private static final int LOG_COL_TOTAL_BYTES = 19;

        /** List of log entries that should be parsed. */
        private static final Set<Integer> LOG_TYPES = F.asSet(
            TYPE_OPEN_IN,
            TYPE_OPEN_OUT,
            TYPE_RANDOM_READ,
            TYPE_CLOSE_IN,
            TYPE_CLOSE_OUT
        );

        /**
         * Create job with given argument.
         *
         * @param arg IGFS name.
         * @param debug Debug flag.
         */
        private VisorIgfsProfilerJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorIgfsProfilerEntry> run(String arg) {
            try {
                Path logsDir = resolveIgfsProfilerLogsDir(ignite.fileSystem(arg));

                if (logsDir != null)
                    return parse(logsDir, arg);
                else
                    return Collections.emptyList();
            }
            catch (IOException | IllegalArgumentException e) {
                throw new IgniteException("Failed to parse profiler logs for IGFS: " + arg, e);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
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
         * @throws NumberFormatException if the string does not contain a parsable integer.
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
         * @throws NumberFormatException if the string does not contain a parsable long.
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
         * Parse IGFS mode from string.
         *
         * @param ss Array of source strings.
         * @param ix Index of array item to parse.
         * @return Parsed IGFS mode or {@code null} if string is empty.
         */
        private IgfsMode parseIgfsMode(String[] ss, int ix) {
            if (ss.length <= ix)
                return null;
            else {
                String s = ss[ix];

                return s.isEmpty() ? null : IgfsMode.valueOf(s);
            }
        }

        /**
         * Parse line from log.
         *
         * @param s Line with text to parse.
         * @return Parsed data.
         */
        private VisorIgfsProfilerParsedLine parseLine(String s) {
            String[] ss = s.split(DELIM_FIELD);

            long streamId = parseLong(ss, LOG_COL_STREAM_ID, -1);

            if (streamId >= 0) {
                int entryType = parseInt(ss, LOG_COL_ENTRY_TYPE, -1);

                // Parse only needed types.
                if (LOG_TYPES.contains(entryType))
                    return new VisorIgfsProfilerParsedLine(
                        parseLong(ss, LOG_COL_TIMESTAMP, 0),
                        entryType,
                        parseString(ss, LOG_COL_PATH),
                        parseIgfsMode(ss, LOG_COL_IGFS_MODE),
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
        private VisorIgfsProfilerEntry aggregateParsedLines(List<VisorIgfsProfilerParsedLine> lines) {
            VisorIgfsProfilerUniformityCounters counters = new VisorIgfsProfilerUniformityCounters();

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
            IgfsMode mode = null;

            for (VisorIgfsProfilerParsedLine line : lines) {
                if (!line.path.isEmpty())
                    path = line.path;

                ts = line.ts; // Remember last timestamp.

                // Remember last IGFS mode.
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
                        throw new IllegalStateException("Unexpected IGFS profiler log entry type: " + line.entryType);
                }
            }

            // Return only fully parsed data with path.
            return path.isEmpty() ? null :
                new VisorIgfsProfilerEntry(
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
        private Collection<VisorIgfsProfilerEntry> parseFile(Path p) throws IOException {
            Collection<VisorIgfsProfilerParsedLine> parsedLines = new ArrayList<>(512);

            try (BufferedReader br = Files.newBufferedReader(p, Charset.forName("UTF-8"))) {
                String line = br.readLine(); // Skip first line with columns header.

                if (line != null) {
                    // Check file header.
                    if (line.equalsIgnoreCase(HDR))
                        line = br.readLine();

                    while (line != null) {
                        try {
                            VisorIgfsProfilerParsedLine ln = parseLine(line);

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
            Map<Long, List<VisorIgfsProfilerParsedLine>> byStreamId = new HashMap<>();

            for (VisorIgfsProfilerParsedLine line : parsedLines) {
                List<VisorIgfsProfilerParsedLine> grp = byStreamId.get(line.streamId);

                if (grp == null) {
                    grp = new ArrayList<>();

                    byStreamId.put(line.streamId, grp);
                }

                grp.add(line);
            }

            // Aggregate each group.
            Collection<VisorIgfsProfilerEntry> entries = new ArrayList<>(byStreamId.size());

            for (List<VisorIgfsProfilerParsedLine> lines : byStreamId.values()) {
                VisorIgfsProfilerEntry entry = aggregateParsedLines(lines);

                if (entry != null)
                    entries.add(entry);
            }

            // Group by files.
            Map<String, List<VisorIgfsProfilerEntry>> byPath = new HashMap<>();

            for (VisorIgfsProfilerEntry entry : entries) {
                List<VisorIgfsProfilerEntry> grp = byPath.get(entry.path());

                if (grp == null) {
                    grp = new ArrayList<>();

                    byPath.put(entry.path(), grp);
                }

                grp.add(entry);
            }

            // Aggregate by files.
            Collection<VisorIgfsProfilerEntry> res = new ArrayList<>(byPath.size());

            for (List<VisorIgfsProfilerEntry> lst : byPath.values())
                res.add(VisorIgfsProfiler.aggregateIgfsProfilerEntries(lst));

            return res;
        }

        /**
         * Parse all IGFS log files in specified log directory.
         *
         * @param logDir Folder were log files located.
         * @return List of line with aggregated information by files.
         */
        private Collection<VisorIgfsProfilerEntry> parse(Path logDir, String igfsName) throws IOException {
            Collection<VisorIgfsProfilerEntry> parsedFiles = new ArrayList<>(512);

            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(logDir)) {
                PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:igfs-log-" + igfsName + "-*.csv");

                for (Path p : dirStream) {
                    if (matcher.matches(p.getFileName())) {
                        try {
                            parsedFiles.addAll(parseFile(p));
                        }
                        catch (NoSuchFileException ignored) {
                            // Files was deleted, skip it.
                        }
                        catch (Exception e) {
                            ignite.log().warning("Failed to parse IGFS profiler log file: " + p, e);
                        }
                    }
                }
            }

            return parsedFiles;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsProfilerJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorIgfsProfilerJob job(String arg) {
        return new VisorIgfsProfilerJob(arg, debug);
    }
}