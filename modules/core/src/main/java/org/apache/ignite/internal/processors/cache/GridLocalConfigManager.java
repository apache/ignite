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

package org.apache.ignite.internal.processors.cache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;

import static java.nio.file.Files.newDirectoryStream;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.TMP_SUFFIX;

/**
 * Responsible for restoring local cache configurations (both from static configuration and persistence).
 * Keep stop sequence of caches and caches which were presented on node before node join.
 */
public class GridLocalConfigManager {
    /** */
    private final boolean startClientCaches =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_START_CACHES_ON_JOIN, false);

    /** Listeners of configuration changes e.g. overwrite or remove actions. */
    private final List<BiConsumer<String, File>> lsnrs = new CopyOnWriteArrayList<>();

    /** Caches stop sequence. */
    private final Deque<String> stopSeq = new LinkedList<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Node's local caches on start (both from static configuration and from persistent caches). */
    private Set<String> localCachesOnStart;

    /** Cache processor. */
    private final GridCacheProcessor cacheProcessor;

    /** Absolute directory for file page store. Includes consistent id based folder. */
    private final File storeWorkDir;

    /** Marshaller. */
    private final Marshaller marshaller;

    /** Context. */
    private final GridKernalContext ctx;

    /** Lock which guards configuration changes. */
    private final ReentrantReadWriteLock chgLock = new ReentrantReadWriteLock();

    /**
     * @param cacheProcessor Cache processor.
     * @param kernalCtx Kernal context.
     */
    public GridLocalConfigManager(
        GridCacheProcessor cacheProcessor,
        GridKernalContext kernalCtx
    ) throws IgniteCheckedException {
        this.cacheProcessor = cacheProcessor;
        ctx = kernalCtx;
        log = ctx.log(getClass());
        marshaller = MarshallerUtils.jdkMarshaller(ctx.igniteInstanceName());

        PdsFolderSettings<?> folderSettings = ctx.pdsFolderResolver().resolveFolders();

        if (!ctx.clientNode() && folderSettings.persistentStoreRootPath() != null) {
            storeWorkDir = folderSettings.persistentStoreNodePath();

            U.ensureDirectory(storeWorkDir, "page store work directory", log);
        }
        else
            storeWorkDir = null;
    }

    /**
     * @param ccfgs List of cache configurations to process.
     * @param ccfgCons Consumer which accepts found configurations files.
     */
    public void readConfigurationFiles(
        List<CacheConfiguration<?, ?>> ccfgs,
        BiConsumer<CacheConfiguration<?, ?>, File> ccfgCons
    ) {
        chgLock.writeLock().lock();

        try {
            for (CacheConfiguration<?, ?> ccfg : ccfgs) {
                File cacheDir = cacheWorkDir(ccfg);

                if (!cacheDir.exists())
                    continue;

                File[] ccfgFiles = cacheDir.listFiles((dir, name) -> name.endsWith(CACHE_DATA_FILENAME));

                if (ccfgFiles == null)
                    continue;

                for (File ccfgFile : ccfgFiles)
                    ccfgCons.accept(ccfg, ccfgFile);
            }
        }
        finally {
            chgLock.writeLock().unlock();
        }
    }

    /**
     * @return Saved cache configurations.
     * @throws IgniteCheckedException If failed.
     */
    public Map<String, StoredCacheData> readCacheConfigurations() throws IgniteCheckedException {
        if (ctx.clientNode())
            return Collections.emptyMap();

        File[] files = storeWorkDir.listFiles();

        if (files == null)
            return Collections.emptyMap();

        Map<String, StoredCacheData> ccfgs = new HashMap<>();

        Arrays.sort(files);

        for (File file : files) {
            if (file.isDirectory())
                readCacheConfigurations(file, ccfgs);
        }

        return ccfgs;
    }

    /**
     * @param conf File with stored cache data.
     * @return Cache data.
     * @throws IgniteCheckedException If failed.
     */
    public StoredCacheData readCacheData(File conf) throws IgniteCheckedException {
        return readCacheData(conf, marshaller, ctx.config());
    }

    /**
     * @param conf File with stored cache data.
     * @return Cache data.
     * @throws IgniteCheckedException If failed.
     */
    public static StoredCacheData readCacheData(
        File conf,
        Marshaller marshaller,
        IgniteConfiguration cfg
    ) throws IgniteCheckedException {
        try (InputStream stream = new BufferedInputStream(new FileInputStream(conf))) {
            return marshaller.unmarshal(stream, U.resolveClassLoader(cfg));
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteCheckedException("An error occurred during cache configuration loading from file [file=" +
                conf.getAbsolutePath() + "]", e);
        }
    }

    /**
     * @param conf File to store cache data.
     * @param cacheData Cache data file.
     * @throws IgniteCheckedException If failed.
     */
    public void writeCacheData(StoredCacheData cacheData, File conf) throws IgniteCheckedException {
        // Pre-existing file will be truncated upon stream open.
        try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(conf))) {
            marshaller.marshal(cacheData, stream);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("An error occurred during cache configuration writing to file [file=" +
                conf.getAbsolutePath() + "]", e);
        }
    }

    /**
     * Save cache configuration to persistent store if necessary.
     *
     * @param cacheData Stored cache data.
     * @param overwrite Overwrite existing.
     */
    public void saveCacheConfiguration(
        StoredCacheData cacheData,
        boolean overwrite
    ) throws IgniteCheckedException {
        assert cacheData != null;

        CacheConfiguration<?, ?> ccfg = cacheData.config();

        if (!CU.storeCacheConfig(cacheProcessor.context(), ccfg))
            return;

        File cacheWorkDir = cacheWorkDir(ccfg);

        FilePageStoreManager.checkAndInitCacheWorkDir(cacheWorkDir, log);

        assert cacheWorkDir.exists() : "Work directory does not exist: " + cacheWorkDir;

        File file = cacheConfigurationFile(ccfg);
        Path filePath = file.toPath();

        chgLock.readLock().lock();

        try {
            if (overwrite || !Files.exists(filePath) || Files.size(filePath) == 0) {
                File tmp = new File(file.getParent(), file.getName() + TMP_SUFFIX);

                if (tmp.exists() && !tmp.delete()) {
                    log.warning("Failed to delete temporary cache config file" +
                        "(make sure Ignite process has enough rights):" + file.getName());
                }

                writeCacheData(cacheData, tmp);

                if (Files.exists(filePath) && Files.size(filePath) > 0) {
                    for (BiConsumer<String, File> lsnr : lsnrs)
                        lsnr.accept(ccfg.getName(), file);
                }

                Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        catch (IOException ex) {
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, ex));

            throw new IgniteCheckedException("Failed to persist cache configuration: " + ccfg.getName(), ex);
        }
        finally {
            chgLock.readLock().unlock();
        }
    }

    /**
     * Remove cache configuration from persistent store.
     *
     * @param cacheData Stored cache data.
     */
    public void removeCacheData(StoredCacheData cacheData) throws IgniteCheckedException {
        chgLock.readLock().lock();

        try {
            CacheConfiguration<?, ?> ccfg = cacheData.config();
            File file = cacheConfigurationFile(ccfg);

            if (file.exists()) {
                for (BiConsumer<String, File> lsnr : lsnrs)
                    lsnr.accept(ccfg.getName(), file);

                if (!file.delete())
                    throw new IgniteCheckedException("Failed to delete cache configuration: " + ccfg.getName());
            }
        }
        finally {
            chgLock.readLock().unlock();
        }
    }

    /**
     *
     */
    public Collection<String> stopSequence() {
        return stopSeq;
    }

    /**
     * @return Caches to be started when this node starts.
     */
    public Set<String> localCachesOnStart() {
        return localCachesOnStart;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public CacheJoinNodeDiscoveryData restoreCacheConfigurations() throws IgniteCheckedException {
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches = new HashMap<>();

        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates = new HashMap<>();

        restoreCaches(caches, templates, ctx.config());

        CacheJoinNodeDiscoveryData discoData = new CacheJoinNodeDiscoveryData(
            IgniteUuid.randomUuid(),
            caches,
            templates,
            startAllCachesOnClientStart()
        );

        localCachesOnStart = new HashSet<>(discoData.caches().keySet());

        return discoData;
    }

    /**
     * @param lsnr Instance of listener to add.
     */
    public void addConfigurationChangeListener(BiConsumer<String, File> lsnr) {
        assert chgLock.isWriteLockedByCurrentThread();

        lsnrs.add(lsnr);
    }

    /**
     * @param lsnr Instance of listener to remove.
     */
    public void removeConfigurationChangeListener(BiConsumer<String, File> lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * Delete caches' configuration data files of cache group.
     *
     * @param ctx Cache group context.
     * @throws IgniteCheckedException If fails.
     */
    public void removeCacheGroupConfigurationData(CacheGroupContext ctx) throws IgniteCheckedException {
        File cacheGrpDir = cacheWorkDir(ctx.sharedGroup(), ctx.cacheOrGroupName());

        if (cacheGrpDir != null && cacheGrpDir.exists()) {
            DirectoryStream.Filter<Path> cacheCfgFileFilter = new DirectoryStream.Filter<Path>() {
                @Override public boolean accept(Path path) {
                    return Files.isRegularFile(path) && path.getFileName().toString().endsWith(CACHE_DATA_FILENAME);
                }
            };

            try (DirectoryStream<Path> dirStream = newDirectoryStream(cacheGrpDir.toPath(), cacheCfgFileFilter)) {
                for (Path path: dirStream)
                    Files.deleteIfExists(path);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to delete cache configurations of group: " + ctx.toString(), e);
            }
        }
    }

    /**
     * @param grpDir Group directory.
     * @param ccfgs Cache configurations.
     * @throws IgniteCheckedException If failed.
     */
    private void readCacheGroupCaches(File grpDir, Map<String, StoredCacheData> ccfgs) throws IgniteCheckedException {
        File[] files = grpDir.listFiles();

        if (files == null)
            return;

        for (File file : files) {
            if (!file.isDirectory() && file.getName().endsWith(CACHE_DATA_FILENAME) && file.length() > 0)
                readAndAdd(
                    ccfgs,
                    file,
                    cacheName -> "Cache with name=" + cacheName + " is already registered, " +
                        "skipping config file " + file.getName() + " in group directory " + grpDir.getName()
                );
        }
    }

    /**
     * @param dir Cache (group) directory.
     * @param ccfgs Cache configurations.
     * @throws IgniteCheckedException If failed.
     */
    public void readCacheConfigurations(File dir, Map<String, StoredCacheData> ccfgs) throws IgniteCheckedException {
        if (dir.getName().startsWith(CACHE_DIR_PREFIX)) {
            File conf = new File(dir, CACHE_DATA_FILENAME);

            if (conf.exists() && conf.length() > 0) {
                readAndAdd(
                    ccfgs,
                    conf,
                    cache -> "Cache with name=" + cache + " is already registered, skipping config file " + dir.getName()
                );
            }
        }
        else if (dir.getName().startsWith(CACHE_GRP_DIR_PREFIX))
            readCacheGroupCaches(dir, ccfgs);
    }

    /**
     * @param ccfgs Loaded configurations.
     * @param file Storead cache data file.
     * @param msg Warning message producer.
     * @throws IgniteCheckedException If failed.
     */
    private void readAndAdd(
        Map<String, StoredCacheData> ccfgs,
        File file,
        Function<String, String> msg
    ) throws IgniteCheckedException {
        StoredCacheData cacheData = readCacheData(file, marshaller, ctx.config());

        String cacheName = cacheData.config().getName();

        // In-memory CDC stored data must be removed on node failover.
        if (inMemoryCdcCache(cacheData.config())) {
            removeCacheData(cacheData);

            U.warn(
                log,
                "Stored data for in-memory CDC cache removed[name=" + cacheName + ", file=" + file.getName() + ']'
            );

            return;
        }

        if (!ccfgs.containsKey(cacheName))
            ccfgs.put(cacheName, cacheData);
        else
            U.warn(log, msg.apply(cacheName));
    }

    /**
     * @param cfg Cache configuration.
     * @return {@code True} if cache placed in in-memory and CDC enabled data region.
     */
    private boolean inMemoryCdcCache(CacheConfiguration<?, ?> cfg) {
        DataRegionConfiguration drCfg =
            CU.findDataRegion(ctx.config().getDataStorageConfiguration(), cfg.getDataRegionName());

        return drCfg != null && !drCfg.isPersistenceEnabled() && drCfg.isCdcEnabled();
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache configuration file with respect to {@link CacheConfiguration#getGroupName} value.
     */
    private File cacheConfigurationFile(CacheConfiguration<?, ?> ccfg) {
        File cacheWorkDir = cacheWorkDir(ccfg);

        return ccfg.getGroupName() == null ? new File(cacheWorkDir, CACHE_DATA_FILENAME) :
            new File(cacheWorkDir, ccfg.getName() + CACHE_DATA_FILENAME);
    }

    /**
     * @param ccfg Cache configuration.
     * @return Store dir for given cache.
     */
    public File cacheWorkDir(CacheConfiguration<?, ?> ccfg) {
        return FilePageStoreManager.cacheWorkDir(storeWorkDir, FilePageStoreManager.cacheDirName(ccfg));
    }

    /**
     * @param isSharedGroup {@code True} if cache is sharing the same `underlying` cache.
     * @param cacheOrGroupName Cache name.
     * @return Store directory for given cache.
     */
    public File cacheWorkDir(boolean isSharedGroup, String cacheOrGroupName) {
        return FilePageStoreManager.cacheWorkDir(storeWorkDir, FilePageStoreManager.cacheDirName(isSharedGroup, cacheOrGroupName));
    }

    /**
     * @return {@code True} if need locally start all existing caches on client node start.
     */
    private boolean startAllCachesOnClientStart() {
        return startClientCaches && ctx.clientNode();
    }

    /**
     * @param caches Caches accumulator.
     * @param templates Templates accumulator.
     * @param igniteCfg Ignite configuration.
     */
    private void restoreCaches(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates,
        IgniteConfiguration igniteCfg
    ) throws IgniteCheckedException {
        CacheConfiguration[] cfgs = igniteCfg.getCacheConfiguration();

        for (int i = 0; i < cfgs.length; i++) {
            CacheConfiguration<?, ?> cfg = new CacheConfiguration(cfgs[i]);

            // Replace original configuration value.
            cfgs[i] = cfg;

            addCacheFromConfiguration(cfg, false, caches, templates);
        }

        if ((CU.isPersistenceEnabled(igniteCfg) && ctx.cache().context().pageStore() != null) || CU.isCdcEnabled(igniteCfg)) {
            Map<String, StoredCacheData> storedCaches = readCacheConfigurations();

            if (!F.isEmpty(storedCaches)) {
                List<String> skippedConfigs = new ArrayList<>();

                for (StoredCacheData storedCacheData : storedCaches.values()) {
                    // Backward compatibility for old stored caches data.
                    if (storedCacheData.hasOldCacheConfigurationFormat()) {
                        storedCacheData = new StoredCacheData(storedCacheData);

                        T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg =
                            cacheProcessor.splitter().split(storedCacheData.config());

                        storedCacheData.config(splitCfg.get1());
                        storedCacheData.cacheConfigurationEnrichment(splitCfg.get2());

                        // Overwrite with new format.
                        saveCacheConfiguration(storedCacheData, true);
                    }

                    String cacheName = storedCacheData.config().getName();

                    CacheType type = ctx.cache().cacheType(cacheName);

                    if (!caches.containsKey(cacheName))
                        // No static cache - add the configuration.
                        addStoredCache(caches, storedCacheData, cacheName, type, true, false);
                    else {
                        // A static cache with the same name already exists.
                        CacheConfiguration cfg = caches.get(cacheName).cacheData().config();
                        CacheConfiguration cfgFromStore = storedCacheData.config();

                        validateCacheConfigurationOnRestore(cfg, cfgFromStore);

                        addStoredCache(caches, storedCacheData, cacheName, type, true,
                            cacheProcessor.keepStaticCacheConfiguration());

                        if (!cacheProcessor.keepStaticCacheConfiguration() && type == CacheType.USER)
                            skippedConfigs.add(cacheName);

                    }
                }

                if (!F.isEmpty(skippedConfigs)) {
                    U.warn(log, "Static configuration for the following caches will be ignored because a persistent " +
                        "cache with the same name already exist (see " +
                        "https://apacheignite.readme.io/docs/cache-configuration for more information): " +
                        skippedConfigs);
                }
            }
        }
    }

    /**
     * Add stored cache data to caches storage.
     *
     * @param caches Cache storage.
     * @param cacheData Cache data to add.
     * @param cacheName Cache name.
     * @param cacheType Cache type.
     * @param isStaticallyConfigured Statically configured flag.
     */
    private void addStoredCache(
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        StoredCacheData cacheData,
        String cacheName,
        CacheType cacheType,
        boolean persistedBefore,
        boolean isStaticallyConfigured
    ) {
        if (!caches.containsKey(cacheName)) {
            if (!cacheType.userCache())
                stopSeq.addLast(cacheName);
            else
                stopSeq.addFirst(cacheName);
        }

        caches.put(cacheName, new CacheJoinNodeDiscoveryData.CacheInfo(cacheData, cacheType, cacheData.sql(),
            persistedBefore ? 1 : 0, isStaticallyConfigured));
    }

    /**
     * @param cfg Cache configuration.
     * @param sql SQL flag.
     * @param caches Caches map.
     * @param templates Templates map.
     * @throws IgniteCheckedException If failed.
     */
    private void addCacheFromConfiguration(
        CacheConfiguration<?, ?> cfg,
        boolean sql,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> caches,
        Map<String, CacheJoinNodeDiscoveryData.CacheInfo> templates
    ) throws IgniteCheckedException {
        String cacheName = cfg.getName();

        CU.validateCacheName(cacheName);

        cacheProcessor.cloneCheckSerializable(cfg);

        CacheObjectContext cacheObjCtx = ctx.cacheObjects().contextForCache(cfg);

        // Initialize defaults.
        cacheProcessor.initialize(cfg, cacheObjCtx);

        StoredCacheData cacheData = new StoredCacheData(cfg);

        cacheData.sql(sql);

        T2<CacheConfiguration, CacheConfigurationEnrichment> splitCfg = cacheProcessor.splitter().split(cfg);

        cacheData.config(splitCfg.get1());
        cacheData.cacheConfigurationEnrichment(splitCfg.get2());

        cfg = splitCfg.get1();

        if (GridCacheUtils.isCacheTemplateName(cacheName))
            templates.put(cacheName, new CacheJoinNodeDiscoveryData.CacheInfo(cacheData, CacheType.USER, false, 0, true));
        else {
            if (caches.containsKey(cacheName)) {
                throw new IgniteCheckedException("Duplicate cache name found (check configuration and " +
                    "assign unique name to each cache): " + cacheName);
            }

            CacheType cacheType = ctx.cache().cacheType(cacheName);

            if (cacheType != CacheType.USER && cfg.getDataRegionName() == null)
                cfg.setDataRegionName(cacheProcessor.context().database().systemDateRegionName());

            addStoredCache(caches, cacheData, cacheName, cacheType, false, true);
        }
    }

    /**
     * Validates cache configuration against stored cache configuration when persistence is enabled.
     *
     * @param cfg Configured cache configuration.
     * @param cfgFromStore Stored cache configuration
     * @throws IgniteCheckedException If validation failed.
     */
    private void validateCacheConfigurationOnRestore(CacheConfiguration cfg, CacheConfiguration cfgFromStore)
        throws IgniteCheckedException {
        assert cfg != null && cfgFromStore != null;

        if ((cfg.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT ||
            cfgFromStore.getAtomicityMode() == TRANSACTIONAL_SNAPSHOT)
            && cfg.getAtomicityMode() != cfgFromStore.getAtomicityMode()) {
            throw new IgniteCheckedException("Cannot start cache. Statically configured atomicity mode differs from " +
                "previously stored configuration. Please check your configuration: [cacheName=" + cfg.getName() +
                ", configuredAtomicityMode=" + cfg.getAtomicityMode() +
                ", storedAtomicityMode=" + cfgFromStore.getAtomicityMode() + "]");
        }

        boolean staticCfgVal = cfg.isEncryptionEnabled();

        boolean storedVal = cfgFromStore.isEncryptionEnabled();

        if (storedVal != staticCfgVal) {
            throw new IgniteCheckedException("Encrypted flag value differs. Static config value is '" + staticCfgVal +
                "' and value stored on the disk is '" + storedVal + "'");
        }
    }
}
