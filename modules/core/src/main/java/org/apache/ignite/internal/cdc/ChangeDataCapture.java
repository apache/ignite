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

package org.apache.ignite.internal.cdc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cdc.ChangeDataCaptureConfiguration;
import org.apache.ignite.cdc.ChangeDataCaptureConsumer;
import org.apache.ignite.cdc.ChangeDataCaptureEvent;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.resource.GridResourceIoc;
import org.apache.ignite.internal.processors.resource.GridResourceLoggerInjector;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.startup.cmdline.ChangeDataCaptureCommandLineStartup;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;

/**
 * CDC(Change Data Capture) application.
 * Application run independently of Ignite node process and provide ability for the {@link ChangeDataCaptureConsumer}
 * to consume events({@link ChangeDataCaptureEvent}) from WAL segments.
 * User should responsible {@link ChangeDataCaptureConsumer} implementation with custom consumption logic.
 *
 * Ignite node should be explicitly configured for using {@link ChangeDataCapture}.
 * <ol>
 *     <li>Set {@link DataStorageConfiguration#setCdcEnabled(boolean)} to true.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setCdcWalPath(String)} to path to the directory to store
 *     WAL segments for CDC.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setWalForceArchiveTimeout(long)} to configure timeout for
 *     force WAL rollover, so new events will be available for consumptions with the predicted time.</li>
 * </ol>
 *
 * When {@link DataStorageConfiguration#getCdcWalPath()} is true then Ignite node on each WAL segment rollover creates
 * hard link to archive WAL segment in {@link DataStorageConfiguration#getCdcWalPath()} directory.
 * {@link ChangeDataCapture} application takes segment file and consumes events from it.
 * After successful consumption (see {@link ChangeDataCaptureConsumer#onEvents(Iterator)})
 * WAL segment will be deleted from directory.
 *
 * Several Ignite nodes can be started on the same host.
 * If your deployment done with custom consistent id then you should specify it via
 * {@link IgniteConfiguration#setConsistentId(Serializable)} in provided {@link IgniteConfiguration}.
 *
 * Application works as follows:
 * <ol>
 *     <li>Search node work directory based on provided {@link IgniteConfiguration}.</li>
 *     <li>Await for creation of CDC directory if it not exists.</li>
 *     <li>Acquire file lock to ensure exclusive consumption.</li>
 *     <li>Loads state of consumption if it exists.</li>
 *     <li>Infinetely wait for new available segment and process it.</li>
 * </ol>
 *
 * @see DataStorageConfiguration#setCdcEnabled(boolean)
 * @see DataStorageConfiguration#setCdcWalPath(String)
 * @see DataStorageConfiguration#setWalForceArchiveTimeout(long)
 * @see ChangeDataCaptureCommandLineStartup
 * @see ChangeDataCaptureConsumer
 * @see DataStorageConfiguration#DFLT_WAL_CDC_PATH
 */
public class ChangeDataCapture implements Runnable {
    /** */
    public static final String ERR_MSG = "Persistence disabled. Capture Data Change can't run!";

    /** State dir. */
    public static final String STATE_DIR = "state";

    /** Ignite configuration. */
    private final IgniteConfiguration cfg;

    /** Spring resource context. */
    private final GridSpringResourceContext ctx;

    /** CDC configuration. */
    private final ChangeDataCaptureConfiguration cdcCfg;

    /** WAL iterator factory. */
    private final IgniteWalIteratorFactory factory;

    /** Events consumer. */
    private final WALRecordsConsumer<?, ?> consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** CDC directory. */
    private Path cdcDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** CDC state. */
    private ChangeDataCaptureConsumerState state;

    /** Save state to start from. */
    private WALPointer initState;

    /** Stopped flag. */
    private volatile boolean stopped;

    /** Previous segments. */
    private final List<Path> prevSegments = new ArrayList<>();

    /**
     * @param cfg Ignite configuration.
     * @param ctx Spring resource context.
     * @param cdcCfg CDC configuration.
     */
    public ChangeDataCapture(
        IgniteConfiguration cfg,
        GridSpringResourceContext ctx,
        ChangeDataCaptureConfiguration cdcCfg) {
        this.cfg = new IgniteConfiguration(cfg);
        this.ctx = ctx;
        this.cdcCfg = cdcCfg;

        consumer = new WALRecordsConsumer<>(cdcCfg.getConsumer());

        initWorkDir(this.cfg);

        log = logger(this.cfg);

        factory = new IgniteWalIteratorFactory(log);
    }

    /** Runs CDC. */
    @Override public void run() {
        synchronized (this) {
            if (stopped)
                return;
        }

        try {
            runX();
        }
        catch (Throwable e) {
            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    /** Runs CDC application with possible exception. */
    public void runX() throws Exception {
        if (!CU.isPersistenceEnabled(cfg)) {
            log.error(ERR_MSG);

            throw new IllegalArgumentException(ERR_MSG);
        }

        if (log.isInfoEnabled()) {
            log.info("Starting Ignite CDC Application.");
            log.info("Consumer     -\t" + consumer.toString());
            log.info("ConsistentId -\t" + cfg.getConsistentId());
        }

        PdsFolderSettings<ChangeDataCaptureFileLockHolder> settings =
            new PdsFolderResolver<>(cfg, log, null, this::tryLock).resolve();

        if (settings == null)
            throw new RuntimeException("Can't find PDS folder!");

        ChangeDataCaptureFileLockHolder lock = settings.getLockedFileLockHolder();

        if (lock == null) {
            File consIdDir = new File(settings.persistentStoreRootPath(), settings.folderName());

            lock = tryLock(consIdDir);

            if (lock == null)
                throw new RuntimeException("Can't lock CDC dir " + settings.consistentId());
        }

        try {
            init();

            if (log.isInfoEnabled()) {
                log.info("CDC dir     -\t" + cdcDir);
                log.info("Binary meta -\t" + binaryMeta);
                log.info("Marshaller  -\t" + marshaller);
                log.info("--------------------------------");
            }

            state = new ChangeDataCaptureConsumerState(cdcDir.resolve(STATE_DIR));

            initState = state.load();

            if (initState != null && log.isInfoEnabled())
                log.info("Loaded initial state[state=" + initState + ']');

            consumer.start(log);

            try {
                consumeWalSegmentsUntilStopped();
            }
            finally {
                consumer.stop();

                if (log.isInfoEnabled())
                    log.info("Ignite CDC Application stoped.");
            }
        }
        finally {
            U.closeQuiet(lock);
        }
    }

    /** Searches required directories. */
    private void init() throws IOException, IgniteCheckedException {
        String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

        Files.createDirectories(cdcDir.resolve(STATE_DIR));

        binaryMeta = CacheObjectBinaryProcessorImpl.binaryWorkDir(cfg.getWorkDirectory(), consIdDir);

        marshaller = MarshallerContextImpl.mappingFileStoreWorkDir(cfg.getWorkDirectory());

        if (log.isDebugEnabled()) {
            log.debug("Using BinaryMeta directory[dir=" + binaryMeta + ']');
            log.debug("Using Marshaller directory[dir=" + marshaller + ']');
        }

        injectResources(consumer.getCdcConsumer());
    }

    /** Waits and consumes new WAL segments until stoped. */
    public void consumeWalSegmentsUntilStopped() {
        try {
            Set<Path> seen = new HashSet<>();

            AtomicLong lastSgmnt = new AtomicLong(-1);

            while (!stopped) {
                try (Stream<Path> cdcFiles = Files.walk(cdcDir, 1)) {
                    Set<Path> exists = new HashSet<>();

                    cdcFiles
                        .peek(exists::add) // Store files that exists in cdc dir.
                        // Need unseen WAL segments only.
                        .filter(p -> WAL_SEGMENT_FILE_FILTER.accept(p.toFile()) && !seen.contains(p))
                        .peek(seen::add) // Adds to seen.
                        .sorted(Comparator.comparingLong(this::segmentIndex)) // Sort by segment index.
                        .peek(p -> {
                            long nextSgmnt = segmentIndex(p);

                            assert lastSgmnt.get() == -1 || nextSgmnt - lastSgmnt.get() == 1;

                            lastSgmnt.set(nextSgmnt);
                        })
                        .forEach(this::consumeSegment); // Consuming segments.

                    seen.removeIf(p -> !exists.contains(p)); // Clean up seen set.
                }

                if (!stopped)
                    U.sleep(cdcCfg.getCheckFrequency());
            }
        }
        catch (IOException | IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Reads all available records from segment. */
    private void consumeSegment(Path segment) {
        if (log.isInfoEnabled())
            log.info("Processing WAL segment[segment=" + segment + ']');

        IgniteWalIteratorFactory.IteratorParametersBuilder builder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .log(log)
                .binaryMetadataFileStoreDir(binaryMeta)
                .marshallerMappingFileStoreDir(marshaller)
                .keepBinary(cdcCfg.isKeepBinary())
                .filesOrDirs(segment.toFile())
                .addFilter((type, ptr) -> type == DATA_RECORD_V2);

        if (initState != null) {
            long segmentIdx = segmentIndex(segment);

            if (segmentIdx > initState.index()) {
                log.error("Found segment greater then saved state. Some events are missed. Exiting!" +
                    "[state=" + initState + ",segment=" + segmentIdx + ']');

                throw new IgniteException("Some data missed.");
            }

            if (segmentIdx < initState.index()) {
                if (log.isInfoEnabled()) {
                    log.info("Deleting segment. Saved state has greater index.[segment=" +
                        segmentIdx + ",state=" + initState.index() + ']');
                }

                // WAL segment is a hard link to a segment file in the special CDC folder.
                // So, we can safely delete it after processing.
                try {
                    Files.delete(segment);

                    return;
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }

            builder.from(initState);

            initState = null;
        }

        try (WALIterator it = factory.iterator(builder)) {
            while (it.hasNext()) {
                boolean commit = consumer.onRecords(F.iterator(it.iterator(), IgniteBiTuple::get2, true));

                if (commit) {
                    assert it.lastRead().isPresent();

                    state.save(it.lastRead().get());

                    // Can delete after new file state save.
                    if (!prevSegments.isEmpty()) {
                        // WAL segment is a hard link to a segment file in a specifal CDC folder.
                        // So we can safely delete it after success processing.
                        for (Path prevSegment : prevSegments)
                            Files.delete(prevSegment);

                        prevSegments.clear();
                    }
                }
            }
        } catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }

        prevSegments.add(segment);
    }

    /**
     * Try locks CDC directory.
     *
     * @param dbStoreDirWithSubdirectory Root PDS directory.
     * @return Lock or null if lock failed.
     */
    private ChangeDataCaptureFileLockHolder tryLock(File dbStoreDirWithSubdirectory) {
        if (!dbStoreDirWithSubdirectory.exists()) {
            log.warning(dbStoreDirWithSubdirectory + " not exists.");

            return null;
        }

        File cdcRoot = new File(cfg.getDataStorageConfiguration().getCdcWalPath());

        if (!cdcRoot.isAbsolute())
            cdcRoot = new File(cfg.getWorkDirectory(), cfg.getDataStorageConfiguration().getCdcWalPath());

        if (!cdcRoot.exists()) {
            log.warning(cdcRoot + " not exists.");

            return null;
        }

        Path cdcDir = Paths.get(cdcRoot.getAbsolutePath(), dbStoreDirWithSubdirectory.getName());

        if (!Files.exists(cdcDir)) {
            log.warning(cdcDir + " not exists.");

            return null;
        }

        this.cdcDir = cdcDir;

        ChangeDataCaptureFileLockHolder lock = new ChangeDataCaptureFileLockHolder(cdcDir.toString(), () -> "cdc.lock", log);

        try {
            lock.tryLock(cdcCfg.getLockTimeout());

            return lock;
        }
        catch (IgniteCheckedException e) {
            U.closeQuiet(lock);

            if (log.isInfoEnabled())
                log.info("Unable to acquire lock to file [" + cdcRoot + "], reason: " + e.getMessage());

            return null;
        }
    }

    /**
     * Initialize logger.
     *
     * @param cfg Configuration.
     */
    private static IgniteLogger logger(IgniteConfiguration cfg) {
        try {
            UUID nodeId = cfg.getNodeId() != null ? cfg.getNodeId() : UUID.randomUUID();

            return IgnitionEx.IgniteNamedInstance.initLogger(cfg.getGridLogger(), "ignite-cdc", nodeId, cfg.getWorkDirectory());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Resolves work directory. */
    private static void initWorkDir(IgniteConfiguration cfg) {
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
            cfg.setWorkDirectory(U.workDirectory(userProvidedWorkDir, igniteHome));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param segment WAL segment file.
     * @return Segment index.
     */
    public long segmentIndex(Path segment) {
        String fn = segment.getFileName().toString();

        return Long.parseLong(fn.substring(0, fn.indexOf('.')));
    }

    /** Stops the application. */
    public void stop() {
        synchronized (this) {
            if (log.isInfoEnabled())
                log.info("Stopping CDC");

            stopped = true;
        }
    }

    /** */
    private void injectResources(ChangeDataCaptureConsumer dataConsumer) throws IgniteCheckedException {
        GridResourceIoc ioc = new GridResourceIoc();

        ioc.inject(
            dataConsumer,
            LoggerResource.class,
            new GridResourceLoggerInjector(log),
            null,
            null
        );

        if (ctx != null) {
            ioc.inject(
                dataConsumer,
                SpringResource.class,
                ctx.springBeanInjector(),
                null,
                null
            );

            ioc.inject(
                dataConsumer,
                SpringApplicationContextResource.class,
                ctx.springContextInjector(),
                null,
                null
            );
        }
    }
}
