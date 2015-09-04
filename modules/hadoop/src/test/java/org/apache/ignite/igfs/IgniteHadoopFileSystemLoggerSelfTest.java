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

package org.apache.ignite.igfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsFileImpl;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfo;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.DELIM_FIELD;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.DELIM_FIELD_VAL;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.HDR;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_CLOSE_IN;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_CLOSE_OUT;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_DELETE;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_DIR_LIST;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_DIR_MAKE;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_MARK;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_OPEN_IN;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_OPEN_OUT;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_RANDOM_READ;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_RENAME;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_RESET;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_SEEK;
import static org.apache.ignite.internal.igfs.common.IgfsLogger.TYPE_SKIP;

/**
 * Grid IGFS client logger test.
 */
public class IgniteHadoopFileSystemLoggerSelfTest extends IgfsCommonAbstractTest {
    /** Path string. */
    private static final String PATH_STR = "/dir1/dir2/file;test";

    /** Path string with escaped semicolons. */
    private static final String PATH_STR_ESCAPED = PATH_STR.replace(';', '~');

    /** Path. */
    private static final IgfsPath PATH = new IgfsPath(PATH_STR);

    /** IGFS name. */
    private static final String IGFS_NAME = "igfs";

    /** Log file path. */
    private static final String LOG_DIR = U.getIgniteHome();

    /** Endpoint address. */
    private static final String ENDPOINT = "localhost:10500";

    /** Log file name. */
    private static final String LOG_FILE = LOG_DIR + File.separator + "igfs-log-" + IGFS_NAME + "-" + U.jvmPid() +
        ".csv";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        removeLogs();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        removeLogs();
    }

    /**
     * Remove existing logs.
     *
     * @throws Exception If failed.
     */
    private void removeLogs() throws Exception {
        File dir = new File(LOG_DIR);

        File[] logs = dir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.startsWith("igfs-log-");
            }
        });

        for (File log : logs)
            log.delete();
    }

    /**
     * Ensure correct static loggers creation/removal as well as file creation.
     *
     * @throws Exception If failed.
     */
    public void testCreateDelete() throws Exception {
        IgfsLogger log = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        IgfsLogger sameLog0 = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        // Loggers for the same endpoint must be the same object.
        assert log == sameLog0;

        IgfsLogger otherLog = IgfsLogger.logger("other" + ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        // Logger for another endpoint must be different.
        assert log != otherLog;

        otherLog.close();

        log.logDelete(PATH, PRIMARY, false);

        log.close();

        File logFile = new File(LOG_FILE);

        // When there are multiple loggers, closing one must not force flushing.
        assert !logFile.exists();

        IgfsLogger sameLog1 = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        assert sameLog0 == sameLog1;

        sameLog0.close();

        assert !logFile.exists();

        sameLog1.close();

        // When we cloe the last logger, it must flush data to disk.
        assert logFile.exists();

        logFile.delete();

        IgfsLogger sameLog2 = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        // This time we expect new logger instance to be created.
        assert sameLog0 != sameLog2;

        sameLog2.close();

        // As we do not add any records to the logger, we do not expect flushing.
        assert !logFile.exists();
    }

    /**
     * Test read operations logging.
     *
     * @throws Exception If failed.
     */
    public void testLogRead() throws Exception {
        IgfsLogger log = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        log.logOpen(1, PATH, PRIMARY, 2, 3L);
        log.logRandomRead(1, 4L, 5);
        log.logSeek(1, 6L);
        log.logSkip(1, 7L);
        log.logMark(1, 8L);
        log.logReset(1);
        log.logCloseIn(1, 9L, 10L, 11);

        log.close();

        checkLog(
            new SB().a(U.jvmPid() + d() + TYPE_OPEN_IN + d() + PATH_STR_ESCAPED + d() + PRIMARY + d() + 1 + d() + 2 +
                d() + 3 + d(14)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_RANDOM_READ + d(3) + 1 + d(7) + 4 + d() + 5 + d(8)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_SEEK + d(3) + 1 + d(7) + 6 + d(9)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_SKIP + d(3) + 1 + d(9) + 7 + d(7)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_MARK + d(3) + 1 + d(10) + 8 + d(6)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_RESET + d(3) + 1 + d(16)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_CLOSE_IN + d(3) + 1 + d(11) + 9 + d() + 10 + d() + 11 + d(3)).toString()
        );
    }

    /**
     * Test write operations logging.
     *
     * @throws Exception If failed.
     */
    public void testLogWrite() throws Exception {
        IgfsLogger log = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        log.logCreate(1, PATH, PRIMARY, true, 2, new Integer(3).shortValue(), 4L);
        log.logAppend(2, PATH, PRIMARY, 8);
        log.logCloseOut(2, 9L, 10L, 11);

        log.close();

        checkLog(
            new SB().a(U.jvmPid() + d() + TYPE_OPEN_OUT + d() + PATH_STR_ESCAPED + d() + PRIMARY + d() + 1 + d() +
                2 + d(2) + 0 + d() + 1 + d() + 3 + d() + 4 + d(10)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_OPEN_OUT + d() + PATH_STR_ESCAPED + d() + PRIMARY + d() + 2 + d() +
                8 + d(2) + 1 + d(13)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_CLOSE_OUT + d(3) + 2 + d(11) + 9 + d() + 10 + d() + 11 + d(3))
                .toString()
        );
    }

    /**
     * Test miscellaneous operations logging.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testLogMisc() throws Exception {
        IgfsLogger log = IgfsLogger.logger(ENDPOINT, IGFS_NAME, LOG_DIR, 10);

        String newFile = "/dir3/file.test";
        String file1 = "/dir3/file1.test";
        String file2 = "/dir3/file1.test";

        log.logMakeDirectory(PATH, PRIMARY);
        log.logRename(PATH, PRIMARY, new IgfsPath(newFile));
        log.logListDirectory(PATH, PRIMARY, new String[] { file1, file2 });
        log.logDelete(PATH, PRIMARY, false);

        log.close();

        checkLog(
            new SB().a(U.jvmPid() + d() + TYPE_DIR_MAKE + d() + PATH_STR_ESCAPED + d() + PRIMARY + d(17)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_RENAME + d() + PATH_STR_ESCAPED + d() + PRIMARY + d(15) + newFile +
                d(2)).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_DIR_LIST + d() + PATH_STR_ESCAPED + d() + PRIMARY + d(17) + file1 +
                DELIM_FIELD_VAL + file2).toString(),
            new SB().a(U.jvmPid() + d() + TYPE_DELETE + d(1) + PATH_STR_ESCAPED + d() + PRIMARY + d(16) + 0 +
                d()).toString()
        );
    }

    /**
     * Create IGFS file with the given path.
     *
     * @param path File path.
     * @return IGFS file instance.
     */
    private IgfsFile file(String path) {
        return new IgfsFileImpl(new IgfsPath(path), new IgfsFileInfo(), 64 * 1024 * 1024);
    }

    /**
     * Ensure that log file has only the following lines.
     *
     * @param lines Expected lines.
     */
    private void checkLog(String... lines) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(LOG_FILE)));

        List<String> logLines = new ArrayList<>(lines.length);

        String nextLogLine;

        while ((nextLogLine = br.readLine()) != null)
            logLines.add(nextLogLine);

        U.closeQuiet(br);

        assertEquals(lines.length + 1, logLines.size());

        assertEquals(logLines.get(0), HDR);

        for (int i = 0; i < lines.length; i++) {
            String logLine = logLines.get(i + 1);

            logLine = logLine.substring(logLine.indexOf(DELIM_FIELD, logLine.indexOf(DELIM_FIELD) + 1) + 1);

            assertEquals(lines[i], logLine);
        }
    }

    /**
     * Return single field delimiter.
     *
     * @return Single field delimiter.
     */
    private String d() {
        return d(1);
    }

    /**
     * Return a bunch of field delimiters.
     *
     * @param cnt Amount of field delimiters.
     * @return Field delimiters.
     */
    private String d(int cnt) {
        SB buf = new SB();

        for (int i = 0; i < cnt; i++)
            buf.a(DELIM_FIELD);

        return buf.toString();
    }
}