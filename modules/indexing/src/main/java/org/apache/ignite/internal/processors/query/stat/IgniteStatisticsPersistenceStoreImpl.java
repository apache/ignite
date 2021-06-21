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

package org.apache.ignite.internal.processors.query.stat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageLifecycleListener;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadOnlyMetastorage;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.ReadWriteMetastorage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;

/**
 * Sql statistics storage in metastore.
 * Will store all statistics related objects with prefix "stats."
 * Store only partition level statistics.
 */
public class IgniteStatisticsPersistenceStoreImpl implements IgniteStatisticsStore, MetastorageLifecycleListener {
    /** In local meta store it store partitions statistics by path: stats.<SCHEMA>.<OBJECT>.<partId> */
    private static final String META_SEPARATOR = ".";

    /** Local metastore statistics prefix. */
    private static final String META_STAT_PREFIX = "stats";

    /** Statistics obsolescence keys prefix. */
    private static final String STAT_OBS_PREFIX = META_STAT_PREFIX + META_SEPARATOR + "obs";

    /** Statistics data keys prefix. */
    private static final String STAT_DATA_PREFIX = META_STAT_PREFIX + META_SEPARATOR + "data";

    /** Key to store version of stored statistics. */
    private static final String META_VERSION_KEY = META_STAT_PREFIX + META_SEPARATOR + "version";

    /** Actual statistics version. */
    public static final Integer VERSION = 2;

    /** Logger. */
    private final IgniteLogger log;

    /** Database shared manager. */
    private final IgniteCacheDatabaseSharedManager db;

    /** Metastorage. */
    private volatile ReadWriteMetastorage metastore;

    /**
     * Constructor.
     *
     * @param subscriptionProcessor Grid subscription processor to track metastorage availability.
     * @param db Database shared manager to lock db while reading/writing metastorage.
     * @param logSupplier Logger getting function.
     */
    public IgniteStatisticsPersistenceStoreImpl(
        GridInternalSubscriptionProcessor subscriptionProcessor,
        IgniteCacheDatabaseSharedManager db,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.db = db;
        subscriptionProcessor.registerMetastorageListener(this);
        this.log = logSupplier.apply(IgniteStatisticsPersistenceStoreImpl.class);
    }

    /**
     * Get partition id from storage key.
     *
     * @param metaKey Meta key to get partition id from.
     * @return Partition id.
     */
    private int getPartitionId(String metaKey) {
        int partIdx = metaKey.lastIndexOf(META_SEPARATOR);
        String partIdStr = metaKey.substring(partIdx + 1);

        return Integer.parseInt(partIdStr);
    }

    /**
     * Get statistics key from metastore path.
     *
     * @param metaKey Metastore path to get statistics key from.
     * @return Statistics key.
     */
    private StatisticsKey getStatsKey(String metaKey) {
        int objIdx = metaKey.indexOf(META_SEPARATOR, STAT_DATA_PREFIX.length() + 1);
        int partIdx = metaKey.indexOf(META_SEPARATOR, objIdx + 1);

        return new StatisticsKey(metaKey.substring(STAT_DATA_PREFIX.length() + 1, objIdx),
            metaKey.substring(objIdx + 1, partIdx));
    }

    /**
     * Generate partition statistics storage prefix.
     *
     * @param key Statistics key.
     * @return Prefix for partition level statistics.
     */
    private String getPartKeyPrefix(StatisticsKey key) {
        return STAT_DATA_PREFIX + META_SEPARATOR + key.schema() + META_SEPARATOR + key.obj() + META_SEPARATOR;
    }

    /**
     * Get statistics key from obsolescence metastore path.
     *
     * @param metaKey Obsolescence metastore path to get statistics key from.
     * @return Statistics key.
     */
    private static StatisticsKey getObsolescenceStatsKey(String metaKey) {
        int objIdx = metaKey.indexOf(META_SEPARATOR, STAT_OBS_PREFIX.length() + 1);
        int partIdx = metaKey.indexOf(META_SEPARATOR, objIdx + 1);

        return new StatisticsKey(metaKey.substring(STAT_DATA_PREFIX.length() + 1, objIdx),
            metaKey.substring(objIdx + 1, partIdx));
    }

    /**
     *
     * @param metaKey
     * @return
     */
    private static Integer getObsolescenceStatsPartId(String metaKey) {
        int sepId = metaKey.lastIndexOf(META_SEPARATOR);

        return Integer.valueOf(metaKey.substring(sepId + 1));
    }

    /**
     * Generate obsolescence partition statistics storage prefix.
     *
     * @param key Statistics key.
     * @return Prefix for obsolescence partition level statistics.
     */
    private String getObsolescencePartKeyPrefix(StatisticsKey key) {
        return STAT_OBS_PREFIX + META_SEPARATOR + key.schema() + META_SEPARATOR + key.obj() + META_SEPARATOR;
    }

    /** {@inheritDoc} */
    @Override public void onReadyForRead(ReadOnlyMetastorage metastorage) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onReadyForReadWrite(ReadWriteMetastorage metastorage) throws IgniteCheckedException {
        this.metastore = metastorage;
        Integer storeVer;

        try {
            storeVer = (Integer)readMeta(META_VERSION_KEY);
        }
        catch (Exception e) {
            if (log.isInfoEnabled())
                log.info("Unable to read statistics version due to " + e.getMessage());

            storeVer = null;
        }

        if (!VERSION.equals(storeVer)) {
            if (storeVer == null) {
                if (log.isDebugEnabled())
                    log.debug("No statistics version found.");
            }
            else {
                if (log.isInfoEnabled()) {
                    log.info(String.format("Found inconsistent statistics version %d instead of %d. " +
                        "Collected local statistics will be cleaned.", storeVer, VERSION));
                }
            }

            clearAllStatistics();

            writeMeta(META_VERSION_KEY, VERSION);
        }
        else {
            try {
                checkLocalStatistics();
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to read statistics due to %s, clearing local statistics store.",
                    e.getMessage()));

                clearAllStatistics();

                writeMeta(META_VERSION_KEY, VERSION);
            }
        }
    }

    /**
     * Check local statistics saved in the metastorage, drop corrupted.
     */
    private void checkLocalStatistics() throws IgniteCheckedException {
        Set<StatisticsKey> brokenObjects = new HashSet<>();

        iterateMeta(STAT_DATA_PREFIX, (keyStr, statMsg) -> {
            StatisticsKey key = getStatsKey(keyStr);

            if (!brokenObjects.contains(key)) {
                try {
                    ObjectPartitionStatisticsImpl statistics = StatisticsUtils
                        .toObjectPartitionStatistics(null, (StatisticsObjectData)statMsg);
                }
                catch (Exception e) {
                    if (!brokenObjects.contains(key))
                        log.warning("Unable to read statistics by key " + key
                            + ". Statistics for this object will be removed.", e);
                    else if (log.isDebugEnabled())
                        log.debug("Unable to read statistics by key " + key);

                    brokenObjects.add(key);
                }
            }

            if (log.isDebugEnabled())
                log.debug("Local statistics for object " + key + " loaded");
        }, true);

        if (!brokenObjects.isEmpty())
            log.warning(String.format("Removing statistics by %d objects.", brokenObjects.size()));

        for (StatisticsKey key : brokenObjects)
            clearLocalPartitionsStatistics(key);
    }

    /** {@inheritDoc} */
    @Override public void clearAllStatistics() {
        if (!checkMetastore("Unable to clear all statistics."))
            return;

        try {
            iterateMeta(META_STAT_PREFIX, (k, v) -> {
                try {
                    metastore.remove(k);

                    if (log.isTraceEnabled())
                        log.trace("Statistics by key " + k + " removed.");
                }
                catch (IgniteCheckedException e) {
                    log.warning("Error during clearing statistics by key " + k, e);
                }
            }, false);

            iterateMeta(STAT_OBS_PREFIX, (k, v) -> {
                try {
                    metastore.remove(k);
                }
                catch (IgniteCheckedException e) {
                    log.warning("Error during clearing statistics obsolescence info by key " + k, e);
                }
            }, false);
        }
        catch (IgniteCheckedException e) {
            log.warning("Error during clearing statistics", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> getAllLocalPartitionsStatistics(
        String schema
    ) {
        String prefix = (schema == null) ? STAT_DATA_PREFIX : STAT_DATA_PREFIX + META_SEPARATOR + schema;

        Map<StatisticsKey, Collection<ObjectPartitionStatisticsImpl>> res = new HashMap<>();

        try {
            iterateMeta(prefix, (k, v) -> {
                StatisticsKey key = getStatsKey(k);
                StatisticsObjectData statData = (StatisticsObjectData)v;
                try {
                    ObjectPartitionStatisticsImpl stat = StatisticsUtils.toObjectPartitionStatistics(null, statData);

                    res.computeIfAbsent(key, k1 -> new ArrayList<>()).add(stat);
                }
                catch (IgniteCheckedException e) {
                    log.warning(String.format(
                        "Error during reading statistics %s.%s by key %s",
                        key.schema(), key.obj(), k
                    ));
                }
            }, true);
        } catch (IgniteCheckedException e) {
            log.warning("Unable to read local partition statistcs", e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void replaceLocalPartitionsStatistics(
        StatisticsKey key,
        Collection<ObjectPartitionStatisticsImpl> statistics
    ) {
        if (!checkMetastore("Unable to save local partitions statistics: %s.%s for %d partitions", key.schema(),
            key.obj(), statistics.size()))
            return;
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(), null);

        Map<Integer, ObjectPartitionStatisticsImpl> partStatistics = statistics.stream().collect(
            Collectors.toMap(ObjectPartitionStatisticsImpl::partId, s -> s));

        String objPrefix = getPartKeyPrefix(key);
        try {
            iterateMeta(objPrefix, (k, v) -> {
                ObjectPartitionStatisticsImpl newStats = partStatistics.remove(getPartitionId(k));
                try {
                    if (newStats == null) {
                        if (log.isTraceEnabled())
                            log.trace("Removing statistics by key" + k);
                        metastore.remove(k);
                    }
                    else {
                        if (log.isTraceEnabled())
                            log.trace("Rewriting statistics by key " + k);

                        metastore.write(k, StatisticsUtils.toObjectData(keyMsg, StatisticsType.PARTITION, newStats));
                    }
                }
                catch (IgniteCheckedException e) {
                    log.warning(String.format("Error during saving statistics %s.%s to %s", key.schema(), key.obj(), k),
                        e);
                }
            }, false);
            if (!partStatistics.isEmpty()) {
                for (Map.Entry<Integer, ObjectPartitionStatisticsImpl> entry : partStatistics.entrySet())
                    writeMeta(objPrefix + entry.getKey(), StatisticsUtils.toObjectData(keyMsg, StatisticsType.PARTITION,
                        entry.getValue()));
            }
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error during saving statistics %s.%s", key.schema(), key.obj()), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ObjectPartitionStatisticsImpl> getLocalPartitionsStatistics(StatisticsKey key) {
        if (!checkMetastore("Unable to get local partitions statistics %s.%s", key.schema(), key.obj()))
            return Collections.emptyList();

        List<ObjectPartitionStatisticsImpl> res = new ArrayList<>();
        try {
            iterateMeta(getPartKeyPrefix(key), (k, v) -> {
                try {
                    ObjectPartitionStatisticsImpl partStats = StatisticsUtils
                        .toObjectPartitionStatistics(null, (StatisticsObjectData)v);
                    res.add(partStats);
                }
                catch (IgniteCheckedException e) {
                    log.warning(String.format(
                        "Error during reading statistics %s.%s by key %s",
                        key.schema(), key.obj(), k
                    ));
                }
            }, true);
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error during reading statistics %s.%s", key.schema(), key.obj()), e);
        }
        return res;
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key) {
        if (!checkMetastore("Unable to clear local partitions statistics %s.%s", key.schema(), key.obj()))
            return;

        try {
            iterateMeta(getPartKeyPrefix(key), (k, v) -> {
                try {
                    metastore.remove(k);
                }
                catch (IgniteCheckedException e) {
                    log.warning(String.format("Error during clearing statistics %s.%s", key.schema(), key.obj()), e);
                }
            }, false);
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error during clearing statistics %s.%s", key.schema(), key.obj()), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void saveLocalPartitionStatistics(StatisticsKey key, ObjectPartitionStatisticsImpl stat) {
        if (!checkMetastore("Unable to store local partition statistics %s.%s:%d", key.schema(), key.obj(),
            stat.partId()))
            return;

        String partKey = getPartKeyPrefix(key) + stat.partId();

        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(), null);

        try {
            StatisticsObjectData statsMsg = StatisticsUtils.toObjectData(keyMsg, StatisticsType.PARTITION, stat);

            if (log.isTraceEnabled())
                log.trace("Writing statistics by key " + partKey);

            writeMeta(partKey, statsMsg);
        }
        catch (IgniteCheckedException e) {
            log.warning(
                String.format(
                    "Error while storing local partition statistics %s.%s:%d",
                    key.schema(), key.obj(), stat.partId()
                ),
                e
            );
        }
    }

    /** {@inheritDoc} */
    @Override public ObjectPartitionStatisticsImpl getLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (!checkMetastore("Unable to get local partition statistics: %s.%s:%d", key.schema(),
            key.obj(), partId))
            return null;

        String metaKey = getPartKeyPrefix(key) + partId;
        try {
            return StatisticsUtils.toObjectPartitionStatistics(null, (StatisticsObjectData) readMeta(metaKey));
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error while reading local partition statistics %s.%s:%d",
                key.schema(), key.obj(), partId), e);
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionStatistics(StatisticsKey key, int partId) {
        if (!checkMetastore("Unable to clean local partition statistics: %s.%s:%d", key.schema(),
            key.obj(), partId))
            return;

        String metaKey = getPartKeyPrefix(key) + partId;
        try {
            removeMeta(metaKey);
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error while clearing local partition statistics %s.%s:%d",
                key.schema(), key.obj(), partId), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void clearLocalPartitionsStatistics(StatisticsKey key, Collection<Integer> partIds) {
        if (!checkMetastore("Unable to clean local partitions statistics: %s.%s:%s", key.schema(),
            key.obj(), partIds))
            return;

        String metaKeyPrefix = getPartKeyPrefix(key);
        Collection<String> metaKeys = new ArrayList<>(partIds.size());
        for (Integer partId : partIds)
            metaKeys.add(metaKeyPrefix + partId);

        try {
            removeMeta(metaKeys);
        }
        catch (IgniteCheckedException e) {
            log.warning(String.format("Error while clearing local partitions statistics %s.%s %s",
                key.schema(), key.obj(), partIds), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void saveObsolescenceInfo(
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> obsolescence
    ) {
        for (Map.Entry<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> objObs : obsolescence.entrySet()) {
            String keyPrefix = getObsolescencePartKeyPrefix(objObs.getKey());

            try {
                objObs.getValue().forEach((k, v) -> writeMeta(keyPrefix + k, v));
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Error while saving statistics obs %s - %s", objObs.getKey(), e.getMessage()));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void clearObsolescenceInfo(StatisticsKey key, Collection<Integer> partIds) {
        String keyPrefix = getObsolescencePartKeyPrefix(key);
        List<String> keysToRmv = new ArrayList<>();
        if (partIds == null) {
            try {
                iterateMeta(keyPrefix, (k, v) -> keysToRmv.add(k), false);
            }
            catch (IgniteCheckedException e) {
                if (log.isInfoEnabled())
                    log.info(String.format("Unable to clean statistics obsolescence keys in %s due to %s", key,
                        e.getMessage()));
            }
        }
        else
            partIds.forEach(partId -> keysToRmv.add(keyPrefix + partId));

        try {
            removeMeta(keysToRmv);
        }
        catch (IgniteCheckedException e) {
            log.info(String.format("Unable to clean statistics obsolescence keys in %s due to %s", key,
                e.getMessage()));
        }
    }

    /** {@inheritDoc} */
    @Override public Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> loadAllObsolescence() {
        Map<StatisticsKey, IntMap<ObjectPartitionStatisticsObsolescence>> res = new HashMap<>();

        try {
            iterateMeta(STAT_OBS_PREFIX, (k, v) -> {
                StatisticsKey key = getObsolescenceStatsKey(k);
                Integer partId = getObsolescenceStatsPartId(k);

                res.computeIfAbsent(key, key1 -> new IntHashMap<>()).put(partId, (ObjectPartitionStatisticsObsolescence)v);
            }, true);
        }
        catch (IgniteCheckedException e) {
            log.info(String.format("Unable to load statistics obsolescence keys due to %s", e.getMessage()));
        }

        return res;
    }

    /**
     * Check metastore availability.
     *
     * @param msg Message to log if metastore unavailable.
     * @param args Arguments to format message.
     * @return {@code true} if metastore available, {@code false} - otherwise.
     */
    private boolean checkMetastore(String msg, Object... args) {
        if (metastore == null) {
            if (log.isInfoEnabled())
                log.info("Metastore doesn't available: " + String.format(msg, args));

            return false;
        }

        return true;
    }

    /**
     * Write object to local metastore.
     *
     * @param key Path to write.
     * @param obj Object to write.
     * @throws IgniteCheckedException Throws in case of errors.
     */
    private void writeMeta(String key, Serializable obj) throws IgniteCheckedException {
        assert obj != null;

        if (!checkMetastore("Unable to save metadata to %s", key))
            return;

        db.checkpointReadLock();
        try {
            metastore.write(key, obj);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * Read object from local metastore.
     *
     * @param key Key to read object by.
     * @return Read object or {@code null} if there are no such object.
     * @throws IgniteCheckedException Throws in case of errors.
     */
    private Serializable readMeta(String key) throws IgniteCheckedException {
        assert key != null;

        db.checkpointReadLock();
        try {
            return metastore.read(key);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * Remove object by key from local metastore.
     *
     * @param key Key to remove object by.
     * @throws IgniteCheckedException Throws in case of errors.
     */
    private void removeMeta(String key) throws IgniteCheckedException {
        db.checkpointReadLock();
        try {
            metastore.remove(key);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * Remove objects from local metastore.
     *
     * @param keys Collection of keys to remove objects by.
     * @throws IgniteCheckedException In case of errors.
     */
    private void removeMeta(Collection<String> keys) throws IgniteCheckedException {
        db.checkpointReadLock();
        try {
            for (String key : keys)
                metastore.remove(key);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }

    /**
     * Scan local metastore for the given prefix and pass all object to specified consumer.
     *
     * @param keyPrefix Key prefix to scan by.
     * @param cb Bi consumer.
     * @param unmarshall If {@code true} - unmarshall objects before passing it to consumer.
     * @throws IgniteCheckedException In case of errors.
     */
    private void iterateMeta(String keyPrefix, BiConsumer<String, ? super Serializable> cb, boolean unmarshall)
        throws IgniteCheckedException {
        assert metastore != null;

        db.checkpointReadLock();
        try {
            metastore.iterate(keyPrefix, cb, unmarshall);
        }
        finally {
            db.checkpointReadUnlock();
        }
    }
}
