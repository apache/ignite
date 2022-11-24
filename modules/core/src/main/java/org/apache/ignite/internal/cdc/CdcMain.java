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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.CdcCacheEvent;
import org.apache.ignite.cdc.CdcConfiguration;
import org.apache.ignite.cdc.CdcConsumer;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cdc.WalRecordsConsumer.DataEntryIterator;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderResolver;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.platform.PlatformType;
import org.apache.ignite.startup.cmdline.CdcCommandLineStartup;

import static org.apache.ignite.internal.IgniteKernal.NL;
import static org.apache.ignite.internal.IgniteKernal.SITE;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.internal.IgnitionEx.initializeDefaultMBeanServer;
import static org.apache.ignite.internal.binary.BinaryUtils.METADATA_FILE_SUFFIX;
import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordType.DATA_RECORD_V2;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.UTILITY_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager.WAL_SEGMENT_FILE_FILTER;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Change Data Capture (CDC) application.
 * The application runs independently of Ignite node process and provides the ability
 * for the {@link CdcConsumer} to consume events({@link CdcEvent}) from WAL segments.
 * The user should provide {@link CdcConsumer} implementation with custom consumption logic.
 *
 * Ignite node should be explicitly configured for using {@link CdcMain}.
 * <ol>
 *     <li>Set {@link DataRegionConfiguration#setCdcEnabled(boolean)} to true.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setCdcWalPath(String)} to path to the directory
 *     to store WAL segments for CDC.</li>
 *     <li>Optional: Set {@link DataStorageConfiguration#setWalForceArchiveTimeout(long)} to configure timeout for
 *     force WAL rollover, so new events will be available for consumptions with the predicted time.</li>
 * </ol>
 *
 * When {@link DataStorageConfiguration#getCdcWalPath()} is true then Ignite node on each WAL segment
 * rollover creates hard link to archive WAL segment in
 * {@link DataStorageConfiguration#getCdcWalPath()} directory. {@link CdcMain} application takes
 * segment file and consumes events from it.
 * After successful consumption (see {@link CdcConsumer#onEvents(Iterator)}) WAL segment will be deleted
 * from directory.
 *
 * Several Ignite nodes can be started on the same host.
 * If your deployment done with custom consistent id then you should specify it via
 * {@link IgniteConfiguration#setConsistentId(Serializable)} in provided {@link IgniteConfiguration}.
 *
 * Application works as follows:
 * <ol>
 *     <li>Searches node work directory based on provided {@link IgniteConfiguration}.</li>
 *     <li>Awaits for the creation of CDC directory if it not exists.</li>
 *     <li>Acquires file lock to ensure exclusive consumption.</li>
 *     <li>Loads state of consumption if it exists.</li>
 *     <li>Infinitely waits for new available segment and processes it.</li>
 * </ol>
 *
 * @see DataRegionConfiguration#setCdcEnabled(boolean)
 * @see DataStorageConfiguration#setCdcWalPath(String)
 * @see DataStorageConfiguration#setWalForceArchiveTimeout(long)
 * @see CdcCommandLineStartup
 * @see CdcConsumer
 * @see DataStorageConfiguration#DFLT_WAL_CDC_PATH
 */
public class CdcMain implements Runnable {
    /** */
    public static final String ERR_MSG = "Persistence and CDC disabled. Capture Data Change can't run!";

    /** State dir. */
    public static final String STATE_DIR = "state";

    /** Current segment index metric name. */
    public static final String CUR_SEG_IDX = "CurrentSegmentIndex";

    /** Committed segment index metric name. */
    public static final String COMMITTED_SEG_IDX = "CommittedSegmentIndex";

    /** Committed segment offset metric name. */
    public static final String COMMITTED_SEG_OFFSET = "CommittedSegmentOffset";

    /** Last segment consumption time. */
    public static final String LAST_SEG_CONSUMPTION_TIME = "LastSegmentConsumptionTime";

    /** Metadata update time. */
    public static final String META_UPDATE = "MetadataUpdateTime";

    /** Binary metadata metric name. */
    public static final String BINARY_META_DIR = "BinaryMetaDir";

    /** Marshaller metric name. */
    public static final String MARSHALLER_DIR = "MarshallerDir";

    /** Cdc directory metric name. */
    public static final String CDC_DIR = "CdcDir";

    /** Ignite configuration. */
    private final IgniteConfiguration igniteCfg;

    /** Spring resource context. */
    private final GridSpringResourceContext ctx;

    /** CDC metrics registry. */
    private MetricRegistry mreg;

    /** Current segment index metric. */
    private AtomicLongMetric curSegmentIdx;

    /** Committed state segment index metric. */
    private AtomicLongMetric committedSegmentIdx;

    /** Committed state segment offset metric. */
    private AtomicLongMetric committedSegmentOffset;

    /** Time of last segment consumption. */
    private AtomicLongMetric lastSegmentConsumptionTs;

    /** Metadata update time. */
    private HistogramMetricImpl metaUpdate;

    /** Change Data Capture configuration. */
    protected final CdcConfiguration cdcCfg;

    /** Events consumer. */
    private final WalRecordsConsumer<?, ?> consumer;

    /** Logger. */
    private final IgniteLogger log;

    /** Change Data Capture directory. */
    private Path cdcDir;

    /** Database directory. */
    private File dbDir;

    /** Binary meta directory. */
    private File binaryMeta;

    /** Marshaller directory. */
    private File marshaller;

    /** Standalone kernal context. */
    private StandaloneGridKernalContext kctx;

    /** Change Data Capture state. */
    private CdcConsumerState state;

    /** Save state to start from. */
    private T2<WALPointer, Integer> walState;

    /** Types state. */
    private Map<Integer, Long> typesState;

    /** Mappings state. */
    private Set<T2<Integer, Byte>> mappingsState;

    /** Caches state. */
    private Map<Integer, Long> cachesState;

    /** Stopped flag. */
    private volatile boolean stopped;

    /** Already processed segments. */
    private final Set<Path> processedSegments = new HashSet<>();

    /**
     * @param cfg Ignite configuration.
     * @param ctx Spring resource context.
     * @param cdcCfg Change Data Capture configuration.
     */
    public CdcMain(
        IgniteConfiguration cfg,
        GridSpringResourceContext ctx,
        CdcConfiguration cdcCfg
    ) {
        igniteCfg = new IgniteConfiguration(cfg);
        this.ctx = ctx;
        this.cdcCfg = cdcCfg;

        try {
            U.initWorkDir(igniteCfg);

            log = U.initLogger(igniteCfg, "ignite-cdc");
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        consumer = new WalRecordsConsumer<>(cdcCfg.getConsumer(), log);
    }

    /** Runs Change Data Capture. */
    @Override public void run() {
        synchronized (this) {
            if (stopped)
                return;
        }

        try {
            runX();
        }
        catch (Throwable e) {
            log.error("Cdc error", e);

            throw new IgniteException(e);
        }
    }

    /** Runs Change Data Capture application with possible exception. */
    public void runX() throws Exception {
        ackAsciiLogo();

        if (!CU.isCdcEnabled(igniteCfg)) {
            log.error(ERR_MSG);

            throw new IllegalArgumentException(ERR_MSG);
        }

        try (CdcFileLockHolder lock = lockPds()) {
            String consIdDir = cdcDir.getName(cdcDir.getNameCount() - 1).toString();

            Files.createDirectories(cdcDir.resolve(STATE_DIR));

            binaryMeta = CacheObjectBinaryProcessorImpl.binaryWorkDir(igniteCfg.getWorkDirectory(), consIdDir);

            marshaller = MarshallerContextImpl.mappingFileStoreWorkDir(igniteCfg.getWorkDirectory());

            if (log.isInfoEnabled()) {
                log.info("Change Data Capture [dir=" + cdcDir + ']');
                log.info("Ignite node Binary meta [dir=" + binaryMeta + ']');
                log.info("Ignite node Marshaller [dir=" + marshaller + ']');
            }

            kctx = startStandaloneKernal();

            initMetrics();

            try {
                kctx.resource().injectGeneric(consumer.consumer());

                state = createState(cdcDir.resolve(STATE_DIR));

                walState = state.loadWalState();
                typesState = state.loadTypesState();
                mappingsState = state.loadMappingsState();
                cachesState = state.loadCaches();

                if (walState != null) {
                    committedSegmentIdx.value(walState.get1().index());
                    committedSegmentOffset.value(walState.get1().fileOffset());
                }

                consumer.start(mreg, kctx.metric().registry(metricName("cdc", "consumer")));

                try {
                    consumeWalSegmentsUntilStopped();
                }
                finally {
                    consumer.stop();

                    if (log.isInfoEnabled())
                        log.info("Ignite Change Data Capture Application stopped.");
                }
            }
            finally {
                for (GridComponent comp : kctx)
                    comp.stop(false);
            }
        }
    }

    /** Creates consumer state. */
    protected CdcConsumerState createState(Path stateDir) {
        return new CdcConsumerState(log, stateDir);
    }

    /**
     * @return Kernal instance.
     * @throws IgniteCheckedException If failed.
     */
    private StandaloneGridKernalContext startStandaloneKernal() throws IgniteCheckedException {
        StandaloneGridKernalContext kctx = new StandaloneGridKernalContext(log, binaryMeta, marshaller) {
            @Override protected IgniteConfiguration prepareIgniteConfiguration() {
                IgniteConfiguration cfg = super.prepareIgniteConfiguration();

                cfg.setIgniteInstanceName(cdcInstanceName(igniteCfg.getIgniteInstanceName()));

                if (!F.isEmpty(cdcCfg.getMetricExporterSpi()))
                    cfg.setMetricExporterSpi(cdcCfg.getMetricExporterSpi());

                initializeDefaultMBeanServer(cfg);

                return cfg;
            }
        };

        kctx.resource().setSpringContext(ctx);

        for (GridComponent comp : kctx)
            comp.start();

        mreg = kctx.metric().registry("cdc");

        return kctx;
    }

    /** Initialize metrics. */
    private void initMetrics() {
        mreg.objectMetric(BINARY_META_DIR, String.class, "Binary meta directory").value(binaryMeta.getAbsolutePath());
        mreg.objectMetric(MARSHALLER_DIR, String.class, "Marshaller directory").value(marshaller.getAbsolutePath());
        mreg.objectMetric(CDC_DIR, String.class, "CDC directory").value(cdcDir.toFile().getAbsolutePath());

        curSegmentIdx = mreg.longMetric(CUR_SEG_IDX, "Current segment index");
        committedSegmentIdx = mreg.longMetric(COMMITTED_SEG_IDX, "Committed segment index");
        committedSegmentOffset = mreg.longMetric(COMMITTED_SEG_OFFSET, "Committed segment offset");
        lastSegmentConsumptionTs =
            mreg.longMetric(LAST_SEG_CONSUMPTION_TIME, "Last time of consumption of WAL segment");
        metaUpdate = mreg.histogram(META_UPDATE, new long[] {100, 500, 1000}, "Metadata update time");
    }

    /**
     * @return CDC lock holder for specifi folder.
     * @throws IgniteCheckedException If failed.
     */
    private CdcFileLockHolder lockPds() throws IgniteCheckedException {
        PdsFolderSettings<CdcFileLockHolder> settings =
            new PdsFolderResolver<>(igniteCfg, log, igniteCfg.getConsistentId(), this::tryLock).resolve();

        if (settings == null) {
            throw new IgniteException("Can't find the folder to read WAL segments from! " +
                "[workDir=" + igniteCfg.getWorkDirectory() + ", consistentId=" + igniteCfg.getConsistentId() + ']');
        }

        CdcFileLockHolder lock = settings.getLockedFileLockHolder();

        if (lock == null) {
            File consIdDir = settings.persistentStoreNodePath();

            lock = tryLock(consIdDir);

            if (lock == null) {
                throw new IgniteException(
                    "Can't acquire lock for Change Data Capture folder [dir=" + consIdDir.getAbsolutePath() + ']'
                );
            }
        }

        return lock;
    }

    /** Waits and consumes new WAL segments until stopped. */
    public void consumeWalSegmentsUntilStopped() {
        try {
            Set<Path> seen = new HashSet<>();

            AtomicLong lastSgmnt = new AtomicLong(-1);

            while (!stopped) {
                if (!consumer.alive()) {
                    log.warning("Consumer is not alive. Ignite Change Data Capture Application will be stopped.");

                    return;
                }

                try (Stream<Path> cdcFiles = Files.list(cdcDir)) {
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

                    if (lastSgmnt.get() == -1) //Forcefully updating metadata if no new segments found.
                        updateMetadata();
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
        updateMetadata();

        if (log.isInfoEnabled())
            log.info("Processing WAL segment [segment=" + segment + ']');

        IgniteWalIteratorFactory.IteratorParametersBuilder builder =
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .log(log)
                .binaryMetadataFileStoreDir(binaryMeta)
                .marshallerMappingFileStoreDir(marshaller)
                .keepBinary(cdcCfg.isKeepBinary())
                .filesOrDirs(segment.toFile())
                .addFilter((type, ptr) -> type == DATA_RECORD_V2);

        if (igniteCfg.getDataStorageConfiguration().getPageSize() != 0)
            builder.pageSize(igniteCfg.getDataStorageConfiguration().getPageSize());

        long segmentIdx = segmentIndex(segment);

        lastSegmentConsumptionTs.value(System.currentTimeMillis());

        curSegmentIdx.value(segmentIdx);

        if (walState != null) {
            if (segmentIdx > walState.get1().index()) {
                throw new IgniteException("Found segment greater then saved state. Some events are missed. Exiting! " +
                    "[state=" + walState + ", segment=" + segmentIdx + ']');
            }

            if (segmentIdx < walState.get1().index()) {
                if (log.isInfoEnabled()) {
                    log.info("Already processed segment found. Skipping and deleting the file [segment=" +
                        segmentIdx + ", state=" + walState.get1().index() + ']');
                }

                // WAL segment is a hard link to a segment file in the special Change Data Capture folder.
                // So, we can safely delete it after processing.
                try {
                    Files.delete(segment);

                    return;
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }
            }

            builder.from(walState.get1());
        }

        try (DataEntryIterator iter = new DataEntryIterator(new IgniteWalIteratorFactory(log).iterator(builder))) {
            if (walState != null) {
                iter.init(walState.get2());

                walState = null;
            }

            boolean interrupted = Thread.interrupted();

            while (iter.hasNext() && !interrupted) {
                boolean commit = consumer.onRecords(iter);

                if (commit) {
                    T2<WALPointer, Integer> curState = iter.state();

                    assert curState != null;

                    if (log.isDebugEnabled())
                        log.debug("Saving state [curState=" + curState + ']');

                    state.saveWal(curState);

                    committedSegmentIdx.value(curState.get1().index());
                    committedSegmentOffset.value(curState.get1().fileOffset());

                    // Can delete after new file state save.
                    if (!processedSegments.isEmpty()) {
                        // WAL segment is a hard link to a segment file in a specifal Change Data Capture folder.
                        // So we can safely delete it after success processing.
                        for (Path processedSegment : processedSegments) {
                            // Can't delete current segment, because state points to it.
                            if (processedSegment.equals(segment))
                                continue;

                            Files.delete(processedSegment);
                        }

                        processedSegments.clear();
                    }
                }

                interrupted = Thread.interrupted();
            }

            if (interrupted)
                throw new IgniteException("Change Data Capture Application interrupted");

            processedSegments.add(segment);
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }
    }

    /** Metadata update. */
    private void updateMetadata() {
        long start = System.currentTimeMillis();

        updateMappings();

        updateTypes();

        updateCaches();

        metaUpdate.value(System.currentTimeMillis() - start);
    }

    /** Search for new or changed {@link BinaryType} and notifies the consumer. */
    private void updateTypes() {
        try {
            File[] files = binaryMeta.listFiles();

            if (files == null)
                return;

            Iterator<BinaryType> changedTypes = Arrays.stream(files)
                .filter(p -> p.toString().endsWith(METADATA_FILE_SUFFIX))
                .map(f -> {
                    int typeId = BinaryUtils.typeId(f.getName());
                    long lastModified = f.lastModified();

                    // Filter out files already in `typesState` with the same last modify date.
                    if (typesState.containsKey(typeId) && lastModified == typesState.get(typeId))
                        return null;

                    typesState.put(typeId, lastModified);

                    try {
                        kctx.cacheObjects().cacheMetadataLocally(binaryMeta, typeId);
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }

                    return kctx.cacheObjects().metadata(typeId);
                })
                .filter(Objects::nonNull)
                .iterator();

            if (!changedTypes.hasNext())
                return;

            consumer.onTypes(changedTypes);

            if (changedTypes.hasNext())
                throw new IllegalStateException("Consumer should handle all changed types");

            state.saveTypes(typesState);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** Search for new or changed {@link TypeMapping} and notifies the consumer. */
    private void updateMappings() {
        try {
            File[] files = marshaller.listFiles(BinaryUtils::notTmpFile);

            if (files == null)
                return;

            Iterator<TypeMapping> changedMappings = Arrays.stream(files)
                .map(f -> {
                    String fileName = f.getName();

                    int typeId = BinaryUtils.mappedTypeId(fileName);
                    byte platformId = BinaryUtils.mappedFilePlatformId(fileName);

                    T2<Integer, Byte> state = new T2<>(typeId, platformId);

                    if (mappingsState.contains(state))
                        return null;

                    mappingsState.add(state);

                    return (TypeMapping)new TypeMappingImpl(
                        typeId,
                        BinaryUtils.readMapping(f),
                        platformId == 0 ? PlatformType.JAVA : PlatformType.DOTNET);
                })
                .filter(Objects::nonNull)
                .iterator();

            if (!changedMappings.hasNext())
                return;

            consumer.onMappings(changedMappings);

            if (changedMappings.hasNext())
                throw new IllegalStateException("Consumer should handle all changed mappings");

            state.saveMappings(mappingsState);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** Search for new or changed {@link CdcCacheEvent} and notifies the consumer. */
    private void updateCaches() {
        try {
            if (!dbDir.exists())
                return;

            File[] files = dbDir.listFiles();

            if (files == null)
                return;

            Set<Integer> destroyed = new HashSet<>(cachesState.keySet());

            Iterator<CdcCacheEvent> cacheEvts = Arrays.stream(files)
                .filter(f -> f.isDirectory() &&
                    (f.getName().startsWith(CACHE_DIR_PREFIX) || f.getName().startsWith(CACHE_GRP_DIR_PREFIX)) &&
                    !f.getName().equals(CACHE_DIR_PREFIX + UTILITY_CACHE_NAME))
                .filter(File::exists)
                // Cache group directory can contain several cache data files.
                // See GridLocalConfigManager#cacheConfigurationFile(CacheConfiguration<?, ?>)
                .flatMap(cacheDir -> Arrays.stream(cacheDir.listFiles(f -> f.getName().endsWith(CACHE_DATA_FILENAME))))
                .map(f -> {
                    try {
                        CdcCacheEvent evt = GridLocalConfigManager.readCacheData(
                            f,
                            MarshallerUtils.jdkMarshaller(kctx.igniteInstanceName()),
                            igniteCfg
                        );

                        destroyed.remove(evt.cacheId());

                        Long lastModified0 = cachesState.get(evt.cacheId());

                        if (lastModified0 != null && lastModified0 == f.lastModified())
                            return null;

                        cachesState.put(evt.cacheId(), f.lastModified());

                        return evt;
                    }
                    catch (IgniteCheckedException e) {
                        throw new IgniteException(e);
                    }
                })
                .filter(Objects::nonNull)
                .iterator();

            consumer.onCacheEvents(cacheEvts);

            if (cacheEvts.hasNext())
                throw new IllegalStateException("Consumer should handle all cache change events");

            if (!destroyed.isEmpty()) {
                Iterator<Integer> destroyedIter = destroyed.iterator();

                consumer.onCacheDestroyEvents(destroyedIter);

                if (destroyedIter.hasNext())
                    throw new IllegalStateException("Consumer should handle all cache destroy events");
            }

            state.saveCaches(cachesState);
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Try locks Change Data Capture directory.
     *
     * @param dbStoreDirWithSubdirectory Root PDS directory.
     * @return Lock or null if lock failed.
     */
    private CdcFileLockHolder tryLock(File dbStoreDirWithSubdirectory) {
        if (!dbStoreDirWithSubdirectory.exists()) {
            log.warning("DB store directory not exists. Should be created by Ignite Node " +
                " [dir=" + dbStoreDirWithSubdirectory + ']');

            return null;
        }

        File cdcRoot = new File(igniteCfg.getDataStorageConfiguration().getCdcWalPath());

        if (!cdcRoot.isAbsolute()) {
            cdcRoot = new File(
                igniteCfg.getWorkDirectory(),
                igniteCfg.getDataStorageConfiguration().getCdcWalPath()
            );
        }

        if (!cdcRoot.exists()) {
            log.warning("CDC root directory not exists. Should be created by Ignite Node. " +
                "Is Change Data Capture enabled in IgniteConfiguration? [dir=" + cdcRoot + ']');

            return null;
        }

        Path cdcDir = Paths.get(cdcRoot.getAbsolutePath(), dbStoreDirWithSubdirectory.getName());

        if (!Files.exists(cdcDir)) {
            log.warning("CDC directory not exists. Should be created by Ignite Node. " +
                "Is Change Data Capture enabled in IgniteConfiguration? [dir=" + cdcDir + ']');

            return null;
        }

        this.cdcDir = cdcDir;
        this.dbDir = dbStoreDirWithSubdirectory;

        CdcFileLockHolder lock = new CdcFileLockHolder(cdcDir.toString(), "cdc.lock", log);

        try {
            lock.tryLock(cdcCfg.getLockTimeout());

            return lock;
        }
        catch (IgniteCheckedException e) {
            U.closeQuiet(lock);

            if (log.isInfoEnabled()) {
                log.info("Unable to acquire lock to lock CDC folder [dir=" + cdcRoot + "]" + NL +
                    "Reason: " + e.getMessage());
            }

            return null;
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
                log.info("Stopping Change Data Capture service instance");

            stopped = true;
        }
    }

    /** */
    private void ackAsciiLogo() {
        String ver = "ver. " + ACK_VER_STR;

        if (log.isInfoEnabled()) {
            log.info(NL + NL +
                ">>>    __________  ________________    ________  _____" + NL +
                ">>>   /  _/ ___/ |/ /  _/_  __/ __/   / ___/ _ \\/ ___/" + NL +
                ">>>  _/ // (7 7    // /  / / / _/    / /__/ // / /__  " + NL +
                ">>> /___/\\___/_/|_/___/ /_/ /___/    \\___/____/\\___/  " + NL +
                ">>> " + NL +
                ">>> " + ver + NL +
                ">>> " + COPYRIGHT + NL +
                ">>> " + NL +
                ">>> Ignite documentation: " + "http://" + SITE + NL +
                ">>> Consumer: " + U.toStringSafe(consumer.consumer()) + NL +
                ">>> ConsistentId: " + igniteCfg.getConsistentId() + NL
            );
        }

        if (log.isQuiet()) {
            U.quiet(false,
                "   __________  ________________    ________  _____",
                "  /  _/ ___/ |/ /  _/_  __/ __/   / ___/ _ \\/ ___/",
                " _/ // (7 7    // /  / / / _/    / /__/ // / /__  ",
                "/___/\\___/_/|_/___/ /_/ /___/    \\___/____/\\___/  ",
                "",
                ver,
                COPYRIGHT,
                "",
                "Ignite documentation: " + "http://" + SITE,
                "Consumer: " + U.toStringSafe(consumer.consumer()),
                "ConsistentId: " + igniteCfg.getConsistentId(),
                "",
                "Quiet mode.");

            String fileName = log.fileName();

            if (fileName != null)
                U.quiet(false, "  ^-- Logging to file '" + fileName + '\'');

            if (log instanceof GridLoggerProxy)
                U.quiet(false, "  ^-- Logging by '" + ((GridLoggerProxy)log).getLoggerInfo() + '\'');

            U.quiet(false,
                "  ^-- To see **FULL** console log here add -DIGNITE_QUIET=false or \"-v\" to ignite-cdc.{sh|bat}",
                "");
        }
    }

    /** */
    public static String cdcInstanceName(String igniteInstanceName) {
        return "cdc-" + igniteInstanceName;
    }
}
