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

package org.apache.ignite.cdc;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;

/**
 * CDC(Capture Data Change) application.
 */
public class IgniteCDC implements Runnable {
    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** Events consumers. */
    private final CDCConsumer consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Watch utils. */
    private final WatchUtils wu;

    /** Work dir. */
    private File workDir;

    /** Consistent id directory name. */
    private String consIdDir;

    /** Binary metadata directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** WAL segments. */
    private volatile File[] segments = new File[0];

    /** WAL segment position. */
    private Optional<WALPointer> pos = Optional.empty();

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.cfg = cfg;
        this.consumer = consumer;
        this.workDir = new File(workDirectory(cfg));
        this.log = logger(cfg, workDir.toString());
        this.factory = new IgniteWalIteratorFactory(log);
        this.wu = new WatchUtils(log);

        if (cfg.getConsistentId() != null)
            consIdDir = U.maskForFileName(cfg.getConsistentId().toString());
    }

    /** {@inheritDoc} */
    @Override public void run() {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info(consumer.toString());
            log.info("--------------------------------");
        }

        consumer.start(cfg, log);

        final Path[] nodeWalDir = new Path[1];

        findNodeWalDir(dir -> nodeWalDir[0] = dir);

        if (log.isInfoEnabled())
            log.info("Node WAL directory found[dir=" + nodeWalDir[0] + ']');

        watchForNewWALFiles(nodeWalDir);

        try {
            int segmentIdx = 0;

            while (segments.length == 0) {
                if (log.isDebugEnabled())
                    log.debug("No WAL segments found");

                Thread.sleep(250);
            }

            log.info("Moving to the next WAL segment[segment=" + segments[segmentIdx].getName() + ']');

            while (true) {
                segmentIdx = readAvailableEvents(segmentIdx);

                Thread.sleep(250);
            }
        }
        catch (InterruptedException | IgniteCheckedException e) {
            log.info("IgniteCDC stopped.");
        }
    }

    /** @param nodeWalDir */
    private void watchForNewWALFiles(Path[] nodeWalDir) {
        Thread newFileThread = new Thread(() ->
            wu.waitFor(nodeWalDir[0], p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches(), walFile -> {
                File[] newWalFiles = new File[segments.length + 1];

                System.arraycopy(segments, 0, newWalFiles, 0, segments.length);

                newWalFiles[segments.length] = walFile.toFile();

                Arrays.sort(newWalFiles);

                segments = newWalFiles;

                return true;
            })
        );

        newFileThread.start();
    }

    /**
     * Reads available events from {@code idx} file of {@link #segments}.
     * @param idx Index of the WAL segment.
     * @return Last readed position.
     * @throws IgniteCheckedException In case of error.
     */
    private int readAvailableEvents(int idx)
        throws IgniteCheckedException {

        if (log.isDebugEnabled())
            log.debug("Fetching new events from WAL[segment=" + segments[idx].getName() + ']');

        int read = readFromFile(segments[idx]);

        if (read == 0 && idx < segments.length - 1) {
            read = readFromFile(segments[idx+1]);

            if (read > 0) {
                log.info("Moving to the next WAL segment[segment=" + segments[idx+1].getName() + ']');

                return idx + 1;
            }
        }

        return idx;
    }

    private int readFromFile(File file) throws IgniteCheckedException {
        IteratorParametersBuilder builder = new IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(true)
            .filesOrDirs(file);

        pos.ifPresent(ptr -> builder.from((FileWALPointer)ptr));

        int read = 0;

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                WALRecord rec = it.next().get2();

                read++;

                consumer.onRecord(rec);
            }

            if (read > 0)
                pos = it.lastRead().map(WALPointer::next);
        }
        return read;
    }

    /**
     * Finds node wal directory.
     *
     * @param callback Callback to be notified.
     */
    private void findNodeWalDir(Consumer<Path> callback) {
        wu.waitFor(workDir.toPath(), work -> {
            if (log.isDebugEnabled())
                log.debug("Work directory found[dir=" + work + ']');

            wu.waitFor(Paths.get(work.toAbsolutePath().toString(), DFLT_STORE_DIR), db -> {
                if (log.isDebugEnabled())
                    log.debug("DB directory found[dir=" + work + ']');

                wu.waitFor(Paths.get(work.toAbsolutePath().toString(), DFLT_WAL_PATH), wal -> {
                    if (log.isDebugEnabled())
                        log.debug("WAL directory found[dir=" + wal + ']');

                    Consumer<Path> preCallback = p -> resolveDirs();

                    if (consIdDir != null)
                        wu.waitFor(Paths.get(wal.toAbsolutePath().toString(), consIdDir),
                            preCallback.andThen(callback));
                    else {
                        if (log.isInfoEnabled()) {
                            log.info("Consistent id not specified. Waiting for first WAL directory available[parent=" +
                                wal + ']');
                        }

                        wu.waitFor(wal, p -> !p.endsWith(DFLT_WAL_ARCHIVE_PATH), p -> {
                            consIdDir = p.subpath(p.getNameCount() - 1, p.getNameCount()).toString();

                            if (log.isInfoEnabled())
                                log.info("Found consistenId directory[consId=" + consIdDir + ']');

                            preCallback.andThen(callback).accept(p);
                        });
                    }
                });
            });
        });
    }

    /** Resolves directories required for WAL iteration. */
    private void resolveDirs() {
        try {
            binaryMeta =
                new File(U.resolveWorkDirectory(workDir.toString(), DFLT_BINARY_METADATA_PATH, false), consIdDir);

            marshaller = U.resolveWorkDirectory(workDir.toString(), DFLT_MARSHALLER_PATH, false);

            if (log.isInfoEnabled()) {
                log.info("Using BinaryMeta directory[dir=" + binaryMeta + ']');
                log.info("Using Marshaller directory[dir=" + marshaller + ']');
            }
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Initialize logger.
     *
     * @param cfg Configuration.
     * @throws IgniteCheckedException
     */
    private static IgniteLogger logger(IgniteConfiguration cfg, String workDir) {
        try {
            UUID appLogId = UUID.randomUUID();

            return IgnitionEx.IgniteNamedInstance.initLogger(cfg.getGridLogger(), appLogId, workDir);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resolves work directory.
     *
     * @param cfg
     * @return
     * @throws IgniteCheckedException
     */
    private static String workDirectory(IgniteConfiguration cfg) {
        try {
            String igniteHome = cfg.getIgniteHome();

            // Set Ignite home.
            if (igniteHome == null)
                igniteHome = U.getIgniteHome();
            else
                // If user provided IGNITE_HOME - set it as a system property.
                U.setIgniteHome(igniteHome);

            String userProvidedWorkDir = cfg.getWorkDirectory();

            // Correctly resolve work directory and set it back to configuration.
            return U.workDirectory(userProvidedWorkDir, igniteHome);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
