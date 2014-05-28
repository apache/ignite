/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.gui.dto.*;

import java.nio.file.*;
import java.util.*;
import static org.gridgain.grid.kernal.visor.gui.tasks.VisorHadoopTaskUtilsEnt.*;

/**
 * TODO: Add class description.
 */
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

        @Override protected Collection<VisorGgfsProfilerEntry> run(VisorGgfsProfilerArg arg) throws GridException {
            try {
                Path logsDir = resolveGgfsProfilerLogsDir(g.ggfs(arg.ggfsName));

                if (logsDir != null)
                    return null; // TODO parse(logsDir, g, arg.ggfsName);
                else
                    return Collections.emptyList();
            }
            catch(IllegalArgumentException iae) {
                throw new GridException("Failed to parse profiler logs for GGFS: " + arg.ggfsName);
            }
        }
    }


    @Override protected VisorJob<VisorGgfsProfilerArg, Collection<VisorGgfsProfilerEntry>> job(
        VisorGgfsProfilerArg arg) {
        return null; // TODO: CODE: implement.
    }
}

//@SerialVersionUID(0)
//class VisorGgfsProfilerJob(arg: VisorGgfsProfilerArg)
//    extends VisorOneNodeJob[VisorGgfsProfilerArg, List[VisorGgfsProfilerEntry]](arg) {
//@impl protected def run(): List[VisorGgfsProfilerEntry] =
//    try {
//    resolveGgfsProfilerLogsDir(g.ggfs(arg.ggfsName)) match {
//    case Some(logsDir) => VisorGgfsProfilerTask.parse(logsDir, g, arg.ggfsName)
//    case None => List.empty
//    }
//    }
//    catch {
//    case iae: IllegalArgumentException =>
//    throw new GridException("Failed to parse profiler logs for GGFS: " + arg.ggfsName)
//    }
//    }
//
///**
// * Task that parse hadoop profiler logs.
// */
//@GridInternal
//class VisorGgfsProfilerTask extends VisorOneNodeTask[VisorGgfsProfilerArg, List[VisorGgfsProfilerEntry]] {
//@impl protected def job(arg: VisorGgfsProfilerArg) = new VisorGgfsProfilerJob(arg)
//    }
//
///**
// * Singleton companion object.
// */
//    object VisorGgfsProfilerTask {
//    /**
//     * Parse line from log.
//     *
//     * @param s Line with text to parse.
//     * @return Parsed data.
//     */
//    def parseLine(s: String): Option[VisorGgfsProfilerParsedLine] = {
//    val ss = s.split(GGHL.DELIM_FIELD)
//    val sz = ss.size
//
//    def parseBoolean(ix: Int, dflt: Boolean = false): Boolean = {
//    if (sz <= ix)
//    dflt
//    else
//    ss(ix) match {
//    case "0" => false
//    case "1" => true
//    case _ => dflt
//    }
//    }
//
//    def parseInt(ix: Int, dflt: Int = 0): Int = {
//    if (sz <= ix)
//    dflt
//    else {
//    val s = ss(ix)
//
//    if (s.isEmpty) dflt else s.toInt
//    }
//    }
//
//    def parseLong(ix: Int, dflt: Long = 0): Long = {
//    if (sz <= ix)
//    dflt
//    else {
//    val s = ss(ix)
//
//    if (s.isEmpty) dflt else s.toLong
//    }
//    }
//
//    def parseString(ix: Int, dflt: String = ""): String = {
//    if (sz <= ix)
//    dflt
//    else {
//    val s = ss(ix)
//
//    if (s.isEmpty) dflt else s
//    }
//    }
//
//    val streamId = parseLong(LOG_COL_STREAM_ID, -1)
//
//    if (streamId >= 0) {
//    val entryType = parseInt(LOG_COL_ENTRY_TYPE)
//
//    // Parse only needed types.
//    if (LOG_TYPES.contains(entryType)) {
//    Some(VisorGgfsProfilerParsedLine(
//    parseLong(LOG_COL_TIMESTAMP),
//    entryType,
//    parseString(LOG_COL_PATH),
//    GGFS_MODES.get(parseString(LOG_COL_GGFS_MODE)),
//    streamId,
//    parseLong(LOG_COL_DATA_LEN),
//    parseBoolean(LOG_COL_APPEND),
//    parseBoolean(LOG_COL_OVERWRITE),
//    parseLong(LOG_COL_POS),
//    parseInt(LOG_COL_READ_LEN),
//    parseLong(LOG_COL_USER_TIME),
//    parseLong(LOG_COL_SYSTEM_TIME),
//    parseLong(LOG_COL_TOTAL_BYTES)
//    ))
//    }
//    else
//    None
//    }
//    else
//    None
//    }
//
//    /**
//     * Aggregate information from parsed lines grouped by `streamId`.
//     */
//    def aggregateParsedLines(lines: collection.mutable.ArrayBuffer[VisorGgfsProfilerParsedLine]): Option[VisorGgfsProfilerEntry] = {
//    var path = ""
//    var ts = 0L
//    var size = 0L
//    var bytesRead = 0L
//    var readTime = 0L
//    var userReadTime = 0L
//    var bytesWritten = 0L
//    var writeTime = 0L
//    var userWriteTime = 0L
//    var mode: Option[GridGgfsMode] = None
//    val counters = new VisorGgfsProfilerUniformityCounters
//
//    lines.sortBy(_.ts).foreach(line => {
//    if (line.path.nonEmpty)
//    path = line.path
//
//    ts = line.ts // Remember last timestamp.
//
//    // Remember last GGFS mode.
//    line.mode.foreach(m => mode = line.mode)
//
//    line.entryType match {
//    case GGHL.TYPE_OPEN_IN =>
//    size = line.dataLen // Remember last file size.
//
//    counters.invalidate(size)
//
//    case GGHL.TYPE_OPEN_OUT =>
//    if (line.overwrite) {
//    size = 0 // If file was overridden, set size to zero.
//
//    counters.invalidate(size)
//    }
//
//    case GGHL.TYPE_CLOSE_IN =>
//    bytesRead += line.totalBytes // Add to total bytes read.
//    readTime += line.sysTime // Add to read time.
//    userReadTime += line.userTime // Add to user read time.
//
//    counters.increment(line.pos, line.totalBytes)
//
//    case GGHL.TYPE_CLOSE_OUT =>
//    size += line.totalBytes // Add to files size.
//    bytesWritten += line.totalBytes // Add to total bytes written.
//    writeTime += line.sysTime // Add to write time.
//    userWriteTime += line.userTime // Add to user write time.
//
//    counters.invalidate(size)
//
//    case GGHL.TYPE_RANDOM_READ =>
//    counters.increment(line.pos, line.totalBytes)
//
//    case _ => throw new IllegalStateException("Unexpected GGFS profiler log entry type: " + line.entryType)
//    }
//    })
//
//    // Return only fully parsed data with path.
//    if (path.nonEmpty)
//    Some(new VisorGgfsProfilerEntryImpl(
//    path,
//    ts,
//    mode,
//    size,
//    bytesRead,
//    readTime,
//    userReadTime,
//    bytesWritten,
//    writeTime,
//    userWriteTime,
//    counters))
//    else
//    None
//    }
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

