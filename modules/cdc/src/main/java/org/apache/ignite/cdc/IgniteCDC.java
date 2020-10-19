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
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_CDC_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.NODE_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.filename.PdsConsistentIdProcessor.UUID_STR_PATTERN;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_NAME_PATTERN;

/**
 * CDC(Capture Data Change) application.
 */
public class IgniteCDC {
    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Events consumers. */
    private final CDCConsumer consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** Watch utils. */
    private final WatchUtils wu;

    /** CDC directory. */
    private Path cdcDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** CDC state. */
    private CDCState state;

    /** Save state to start from. */
    private WALPointer initState;

    /** Thread that waits for creation of the new WAL segments. */
    private Thread segmentThread;

    /** Previous segments. */
    private Path prevSegment;

    /**
     * @param cfg Ignite configuration.
     * @param consumer Event consumer.
     */
    public IgniteCDC(IgniteConfiguration cfg, CDCConsumer consumer) {
        this.log = logger(cfg, workDir(cfg));
        this.cfg = cfg;
        this.factory = new IgniteWalIteratorFactory(log);
        this.consumer = consumer;
        this.wu = new WatchUtils(log);

        if (!CU.isPersistenceEnabled(cfg))
            throw new IllegalArgumentException("Persistence disabled. IgniteCDC can't run!");

        if (cfg.getConsistentId() == null) {
            log.warning("Consistent ID is not set, it is recommended to set consistent ID for production " +
                "clusters (use IgniteConfiguration.setConsistentId property)");
        }
    }

    /** Starts CDC. */
    public void start() {
        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info("Consumer    -\t" + consumer.toString());
        }

        try {
            initDirs();
        }
        catch (IgniteCheckedException | IOException | InterruptedException e) {
            throw new IgniteException(e);
        }

        if (log.isInfoEnabled()) {
            log.info("CDC dir     -\t" + cdcDir);
            log.info("Binary meta -\t" + binaryMeta);
            log.info("Marshaller  -\t" + marshaller);
            log.info("--------------------------------");
        }

        state = new CDCState(cdcDir.resolve("state"), consumer.id());

        initState = state.load();

        if (initState != null && log.isInfoEnabled())
            log.info("Loaded initial state[state=" + initState + ']');

        segmentThread = new Thread(() -> {
            consumer.start(cfg, log);

            try {
                Predicate<Path> walFilesOnly = p -> WAL_NAME_PATTERN.matcher(p.getFileName().toString()).matches();

                Comparator<Path> sortByNumber = Comparator.comparingLong(this::segmentNumber);

                wu.waitFor(cdcDir, walFilesOnly, sortByNumber, segment -> {
                    try {
                        readSegment(segment);

                        return true;
                    }
                    catch (IgniteCheckedException | IOException e) {
                        throw new IgniteException(e);
                    }
                });
            }
            catch (IgniteException err) {
                if (!X.hasCause(err, ClosedByInterruptException.class))
                    throw err;
            }
            catch (InterruptedException ignore) {
                if (log.isInfoEnabled())
                    log.info("Segment wait thread interrupted.");
            }
            finally {
                consumer.stop();
            }
        }, "wait-cdc-segments");

        segmentThread.start();
    }

    /** Reads all available from segment. */
    private void readSegment(Path segment) throws IgniteCheckedException, IOException {
        IgniteWalIteratorFactory.IteratorParametersBuilder builder = new IgniteWalIteratorFactory.IteratorParametersBuilder()
            .log(log)
            .binaryMetadataFileStoreDir(binaryMeta)
            .marshallerMappingFileStoreDir(marshaller)
            .keepBinary(true)
            .filesOrDirs(segment.toFile());

        if (initState != null) {
            long segmentIdx = segmentNumber(segment);

            if (segmentIdx > initState.index()) {
                log.error("Found segment greater then saved state. Some events are missed. Exiting!" +
                    "[state=" + initState + ",segment=" + segmentIdx + ']');

                throw new IgniteException("Some data missed.");
            }
            else if (segmentIdx < initState.index()) {
                if (log.isInfoEnabled()) {
                    log.info("Deleting segment. Saved state has greater index.[segment=" +
                        segmentIdx + ",state=" + initState.index() + ']');
                }

                // WAL segment is a hard link to a segment file in a specifal CDC folder.
                // So we can safely delete it after success processing.
                Files.delete(segment);
            }
            else {
                builder.from(initState);

                initState = null;
            }
        }

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                consumer.onRecord(it.next().get2());

                state.save(it.lastRead().get());

                // Can delete after new file state save.
                if (prevSegment != null) {
                    // WAL segment is a hard link to a segment file in a specifal CDC folder.
                    // So we can safely delete it after success processing.
                    Files.delete(prevSegment);

                    prevSegment = null;
                }
            }
        }

        prevSegment = segment;
    }

    /** Founds required directories. */
    private void initDirs() throws IgniteCheckedException, IOException, InterruptedException {
        String workDir = workDir(cfg);

        cdcDir = initCdcDir(workDir);

        String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

        if (log.isDebugEnabled())
            log.debug("Found WAL archive[dir=" + cdcDir + ']');

        Files.createDirectories(cdcDir.resolve("state"));

        binaryMeta = new File(U.resolveWorkDirectory(workDir, DFLT_BINARY_METADATA_PATH, false), consIdDir);

        marshaller = U.resolveWorkDirectory(workDir, DFLT_MARSHALLER_PATH, false);

        if (log.isDebugEnabled()) {
            log.debug("Using BinaryMeta directory[dir=" + binaryMeta + ']');
            log.debug("Using Marshaller directory[dir=" + marshaller + ']');
        }
    }

    /**
     * @param workDir Working directory.
     * @return WAL archive directory.
     */
    private Path initCdcDir(String workDir) throws InterruptedException {
        Path cdcParent;

        if (cfg.getDataStorageConfiguration() != null &&
            !F.isEmpty(cfg.getDataStorageConfiguration().getCdcPath())) {
            cdcParent = Paths.get(cfg.getDataStorageConfiguration().getCdcPath());

            if (!cdcParent.isAbsolute())
                cdcParent = Paths.get(workDir, cfg.getDataStorageConfiguration().getCdcPath());
        }
        else
            cdcParent = Paths.get(workDir).resolve(DFLT_CDC_PATH);

        if (log.isDebugEnabled())
            log.debug("Archive root[dir=" + cdcParent + ']');

        final Path[] cdcDir = new Path[1];

        wu.waitFor(cdcParent,
            dir -> dir.getName(dir.getNameCount() - 1).toString().matches(NODE_PATTERN + UUID_STR_PATTERN),
            dir -> cdcDir[0] = dir);

        return cdcDir[0];
    }

    /**
     * Initialize logger.
     *
     * @param cfg Configuration.
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
     * @return Working directory
     */
    public static String workDir(IgniteConfiguration cfg) {
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

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long segmentNumber(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /** {@inheritDoc} */
    public void interrupt() {
        if (segmentThread != null)
            segmentThread.interrupt();
    }

    /** Waits for CDC to be stopped. */
    public void join() throws InterruptedException {
        if (segmentThread != null)
            segmentThread.join();
    }
}
