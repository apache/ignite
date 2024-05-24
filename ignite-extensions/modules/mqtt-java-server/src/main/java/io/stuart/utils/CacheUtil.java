/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.utils;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;

import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.consts.ParamConst;
import io.stuart.entities.auth.MqttAcl;
import io.stuart.entities.auth.MqttAdmin;
import io.stuart.entities.auth.MqttUser;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttAwaitMessageKey;
import io.stuart.entities.cache.MqttConnection;
import io.stuart.entities.cache.MqttListener;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttMessageKey;
import io.stuart.entities.cache.MqttNode;
import io.stuart.entities.cache.MqttRetainMessage;
import io.stuart.entities.cache.MqttRouter;
import io.stuart.entities.cache.MqttRouterKey;
import io.stuart.entities.cache.MqttSession;
import io.stuart.entities.cache.MqttTrie;
import io.stuart.entities.cache.MqttTrieKey;
import io.stuart.entities.cache.MqttWillMessage;
import io.stuart.ext.collections.ExpiringMap;
import io.stuart.ext.collections.ExpiringMap.Builder;
import net.jodah.expiringmap.ExpirationListener;
import net.jodah.expiringmap.ExpirationPolicy;

public class CacheUtil {

    public static IgniteConfiguration igniteCfg(IgniteConfiguration baseCfg, boolean persistenceEnabled, boolean compactFooter, int... inclEvtTypes) {
        // initialize ignite configuration
        IgniteConfiguration igniteCfg = baseCfg;

        // set ignite configuration properties
        igniteCfg.setDataStorageConfiguration(dataStorageCfg(persistenceEnabled));
        igniteCfg.setBinaryConfiguration(binaryCfg(compactFooter));
        igniteCfg.setIncludeEventTypes(inclEvtTypes);
        igniteCfg.setPeerClassLoadingEnabled(true);

        // return ignite configuration
        return igniteCfg;
    }

    public static <K, V> CacheConfiguration<K, V> memCacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups,
            Class<?>... indexedTypes) {

        // return memory data region cache configuration
        return memCacheCfg(name, cacheMode, atomicityMode, writeSyncMode(), backups, indexedTypes);
    }

    public static <K, V> CacheConfiguration<K, V> memCacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode,
            CacheWriteSynchronizationMode writeSync, int backups, Class<?>... indexedTypes) {

        // return memory data region cache configuration
        return cacheCfg(name, CacheConst.MEM_DATA_REGION_NAME, cacheMode, atomicityMode, writeSync, backups, indexedTypes);
    }

    public static <K, V> CacheConfiguration<K, V> cacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups,
            Class<?>... indexedTypes) {

        // return cache configuration
        return cacheCfg(name, cacheMode, atomicityMode, writeSyncMode(), backups, indexedTypes);
    }

    public static <K, V> CacheConfiguration<K, V> cacheCfg(String name, CacheMode cacheMode, CacheAtomicityMode atomicityMode,
            CacheWriteSynchronizationMode writeSync, int backups, Class<?>... indexedTypes) {

        // return cache configuration
        return cacheCfg(name, null, cacheMode, atomicityMode, writeSync, backups, indexedTypes);
    }

    public static CollectionConfiguration collectionCfg(boolean collocated, CacheAtomicityMode atomicityMode, CacheMode cacheMode, int backups,
            long offHeapMaxMemory) {

        // new collection configuration
        CollectionConfiguration cfg = new CollectionConfiguration();

        // set collection configuration properties
        cfg.setCollocated(collocated);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setCacheMode(cacheMode);
        cfg.setBackups(backups);

        // return collection configuration
        return cfg;
    }

    public static <K, V> ExpiringMap<K, V> expiringMap(long duration, ExpirationListener<K, V> listener, int maxSize) {
        return expiringMap(duration, TimeUnit.SECONDS, ExpirationPolicy.CREATED, listener, false, maxSize);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ExpiringMap<K, V> expiringMap(long duration, TimeUnit unit, ExpirationPolicy policy, ExpirationListener<K, V> listener,
            boolean variableExpiration, int maxSize) {

        // get expiring map builder
        Builder<K, V> builder = (Builder<K, V>) ExpiringMap.builder();

        // set duration and time unit
        builder.expiration(duration, unit);
        // set policy
        builder.expirationPolicy(policy);
        // set listener
        builder.expirationListener(listener);

        if (variableExpiration) {
            // set variable expiration
            builder.variableExpiration();
        }

        if (maxSize > 0 && maxSize <= Integer.MAX_VALUE) {
            // set max size
            builder.maxSize(maxSize);
        }

        // return expiring map
        return builder.build();
    }

    private static WALMode walMode() {
        // get wal mode
        String walMode = Config.getStorageWalMode();

        if (ParamConst.STORAGE_WAL_MODE_FSYNC.equalsIgnoreCase(walMode)) {
            return WALMode.FSYNC;
        } else if (ParamConst.STORAGE_WAL_MODE_LOG_ONLY.equals(walMode)) {
            return WALMode.LOG_ONLY;
        } else if (ParamConst.STORAGE_WAL_MODE_BACKGROUND.equalsIgnoreCase(walMode)) {
            return WALMode.BACKGROUND;
        } else {
            return WALMode.LOG_ONLY;
        }
    }

    private static CacheWriteSynchronizationMode writeSyncMode() {
        // get write synchronization mode
        String writeSyncMode = Config.getStorageWriteSyncMode();

        if (ParamConst.STORAGE_WRITE_SYNC_MODE_PRIMARY_SYNC.equalsIgnoreCase(writeSyncMode)) {
            return CacheWriteSynchronizationMode.PRIMARY_SYNC;
        } else if (ParamConst.STORAGE_WRITE_SYNC_MODE_FULL_SYNC.equalsIgnoreCase(writeSyncMode)) {
            return CacheWriteSynchronizationMode.FULL_SYNC;
        } else {
            return CacheWriteSynchronizationMode.PRIMARY_SYNC;
        }

        // TODO CacheWriteSynchronizationMode.FULL_ASYNC: Some scenarios are not
        // applicable, optimized separately for different caches
    }

    private static DataStorageConfiguration dataStorageCfg(boolean persistenceEnabled) {
        // initialize data storage configuration
        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        // set default data region enable persistence
        dataStorageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistenceEnabled);
        // set memory data region configuration
        dataStorageCfg.setDataRegionConfigurations(memDataRegionCfg());

        // set data storage path
        dataStorageCfg.setStoragePath(Config.getStorageDataDir());
        // set data wal path
        dataStorageCfg.setWalPath(Config.getStorageWalDir());
        // true: set wal archive path is not equal to the data wal path
        dataStorageCfg.setWalArchivePath(Config.getStorageWalArchiveDir());

        // get storage wal mode
        WALMode walMode = walMode();
        // set storage wal mode
        dataStorageCfg.setWalMode(walMode);
        // check: is background mode?
        if (walMode == WALMode.BACKGROUND) {
            // set wal flush frequency
            dataStorageCfg.setWalFlushFrequency(Config.getStorageWalFlushFrequencyMs());
        }

        // return data storage configuration
        return dataStorageCfg;
    }

    private static DataRegionConfiguration memDataRegionCfg() {
        // initialize memory data region configuration
        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        // set data region name
        dataRegionCfg.setName(CacheConst.MEM_DATA_REGION_NAME);
        // set disable persistence
        dataRegionCfg.setPersistenceEnabled(false);

        // return memory data region configuration
        return dataRegionCfg;
    }

    private static BinaryConfiguration binaryCfg(boolean compactFooter) {
        // initialize binary configuration
        BinaryConfiguration binaryCfg = new BinaryConfiguration();

        // sometimes apache ignite server throw BinaryObjectException:
        // Cannot find schema for object with compact footer
        // solution: switch off compact footer(false)
        binaryCfg.setCompactFooter(compactFooter);
        // set binary type configurations
        // avoid BinaryInvalidTypeException
        binaryCfg.setTypeConfigurations(binaryTypes());

        // return binary configuration
        return binaryCfg;
    }

    private static Set<BinaryTypeConfiguration> binaryTypes() {
        Set<BinaryTypeConfiguration> types = new HashSet<>();

        // mqtt node binary type
        BinaryTypeConfiguration mqttNode = new BinaryTypeConfiguration();
        mqttNode.setTypeName(MqttNode.class.getName());
        types.add(mqttNode);

        // mqtt listener binary type
        BinaryTypeConfiguration mqttListener = new BinaryTypeConfiguration();
        mqttListener.setTypeName(MqttListener.class.getName());
        types.add(mqttListener);

        // mqtt connection binary type
        BinaryTypeConfiguration mqttConnection = new BinaryTypeConfiguration();
        mqttConnection.setTypeName(MqttConnection.class.getName());
        types.add(mqttConnection);

        // mqtt session binary type
        BinaryTypeConfiguration mqttSession = new BinaryTypeConfiguration();
        mqttSession.setTypeName(MqttSession.class.getName());
        types.add(mqttSession);

        // mqtt router key binary type
        BinaryTypeConfiguration mqttRouterKey = new BinaryTypeConfiguration();
        mqttRouterKey.setTypeName(MqttRouterKey.class.getName());
        types.add(mqttRouterKey);

        // mqtt router binary type
        BinaryTypeConfiguration mqttRouter = new BinaryTypeConfiguration();
        mqttRouter.setTypeName(MqttRouter.class.getName());
        types.add(mqttRouter);

        // mqtt trie key binary type
        BinaryTypeConfiguration mqttTrieKey = new BinaryTypeConfiguration();
        mqttTrieKey.setTypeName(MqttTrieKey.class.getName());
        types.add(mqttTrieKey);

        // mqtt trie binary type
        BinaryTypeConfiguration mqttTrie = new BinaryTypeConfiguration();
        mqttTrie.setTypeName(MqttTrie.class.getName());
        types.add(mqttTrie);

        // mqtt await message key binary type
        BinaryTypeConfiguration mqttAwaitMessageKey = new BinaryTypeConfiguration();
        mqttAwaitMessageKey.setTypeName(MqttAwaitMessageKey.class.getName());
        types.add(mqttAwaitMessageKey);

        // mqtt await message binary type
        BinaryTypeConfiguration mqttAwaitMessage = new BinaryTypeConfiguration();
        mqttAwaitMessage.setTypeName(MqttAwaitMessage.class.getName());
        types.add(mqttAwaitMessage);

        // mqtt message key binary type
        BinaryTypeConfiguration mqttMessageKey = new BinaryTypeConfiguration();
        mqttMessageKey.setTypeName(MqttMessageKey.class.getName());
        types.add(mqttMessageKey);

        // mqtt message binary type
        BinaryTypeConfiguration mqttMessage = new BinaryTypeConfiguration();
        mqttMessage.setTypeName(MqttMessage.class.getName());
        types.add(mqttMessage);

        // mqtt retain message binary type
        BinaryTypeConfiguration mqttRetainMessage = new BinaryTypeConfiguration();
        mqttRetainMessage.setTypeName(MqttRetainMessage.class.getName());
        types.add(mqttRetainMessage);

        // mqtt will message binary type
        BinaryTypeConfiguration mqttWillMessage = new BinaryTypeConfiguration();
        mqttWillMessage.setTypeName(MqttWillMessage.class.getName());
        types.add(mqttWillMessage);

        // mqtt user binary type
        BinaryTypeConfiguration mqttUser = new BinaryTypeConfiguration();
        mqttUser.setTypeName(MqttUser.class.getName());
        types.add(mqttUser);

        // mqtt acl binary type
        BinaryTypeConfiguration mqttAcl = new BinaryTypeConfiguration();
        mqttAcl.setTypeName(MqttAcl.class.getName());
        types.add(mqttAcl);

        // mqtt admin binary type
        BinaryTypeConfiguration mqttAdmin = new BinaryTypeConfiguration();
        mqttAdmin.setTypeName(MqttAdmin.class.getName());
        types.add(mqttAdmin);

        return types;
    }

    private static <K, V> CacheConfiguration<K, V> cacheCfg(String name, String dataRegionName, CacheMode cacheMode, CacheAtomicityMode atomicityMode,
            CacheWriteSynchronizationMode writeSync, int backups, Class<?>... indexedTypes) {

        // new cache configuration
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>();

        // set cache configuration properties
        cfg.setName(name);
        cfg.setCacheMode(cacheMode);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setWriteSynchronizationMode(writeSync);
        cfg.setBackups(backups);
        cfg.setStoreKeepBinary(true);
        cfg.setMaxQueryIteratorsCount(Integer.MAX_VALUE);

        if (dataRegionName != null && !dataRegionName.isEmpty()) {
            cfg.setDataRegionName(dataRegionName);
        }

        if (indexedTypes != null && indexedTypes.length > 0 && indexedTypes.length % 2 == 0) {
            cfg.setIndexedTypes(indexedTypes);
        }

        return cfg;
    }

}
