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

package org.apache.ignite.internal.storage;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.client.Conditions;
import org.apache.ignite.metastorage.client.Entry;
import org.apache.ignite.metastorage.client.EntryEvent;
import org.apache.ignite.metastorage.client.Operation;
import org.apache.ignite.metastorage.client.Operations;
import org.apache.ignite.metastorage.client.WatchEvent;
import org.apache.ignite.metastorage.client.WatchListener;
import org.jetbrains.annotations.NotNull;

/**
 * Distributed configuration storage.
 */
public class DistributedConfigurationStorage implements ConfigurationStorage {
    /** Prefix that we add to configuration keys to distinguish them in meta storage. Must end with dot. */
    private static final String DISTRIBUTED_PREFIX = "dst-cfg.";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DistributedConfigurationStorage.class);

    /**
     * Key for CAS-ing configuration keys to meta storage. This key is expected to be the first key in lexicographical
     * order of distributed configuration keys.
     */
    private static final ByteArray MASTER_KEY = new ByteArray(DISTRIBUTED_PREFIX);

    /**
     * This key is expected to be the last key in lexicographical order of distributed configuration keys. It is
     * possible because keys are in lexicographical order in meta storage and adding {@code (char)('.' + 1)} to the end
     * will produce all keys with prefix {@link DistributedConfigurationStorage#DISTRIBUTED_PREFIX}
     */
    private static final ByteArray DST_KEYS_END_RANGE =
            new ByteArray(DISTRIBUTED_PREFIX.substring(0, DISTRIBUTED_PREFIX.length() - 1) + (char)('.' + 1));

    /** Id of watch that is responsible for configuration update. */
    private CompletableFuture<Long> watchId;

    /** MetaStorage manager */
    private final MetaStorageManager metaStorageMgr;

    /** Change listeners. */
    private final List<ConfigurationStorageListener> listeners = new CopyOnWriteArrayList<>();

    /** Storage version. It stores actual meta storage revision, that is applied to configuration manager. */
    private final AtomicLong ver = new AtomicLong(0L);

    /**
     * Constructor.
     *
     * @param metaStorageMgr MetaStorage Manager.
     */
    public DistributedConfigurationStorage(MetaStorageManager metaStorageMgr) {
        this.metaStorageMgr = metaStorageMgr;
    }

    /** {@inheritDoc} */
    @Override public synchronized Data readAll() throws StorageException {
        HashMap<String, Serializable> data = new HashMap<>();

        Iterator<Entry> entries = allDistributedConfigKeys().iterator();

        long maxRevision = 0L;

        if (!entries.hasNext())
            return new Data(data, ver.get());

        Entry entryForMasterKey = entries.next();

        // First key must be the masterKey because it's supposed to be the first in lexicographical order
        assert entryForMasterKey.key().equals(MASTER_KEY);

        while (entries.hasNext()) {
            Entry entry = entries.next();

            data.put(entry.key().toString().substring((DISTRIBUTED_PREFIX).length()), (Serializable)ByteUtils.fromBytes(entry.value()));

            // Move to stream
            if (maxRevision < entry.revision())
                maxRevision = entry.revision();

        }

        if (!data.isEmpty()) {
            assert maxRevision == entryForMasterKey.revision();

            assert maxRevision >= ver.get();

            return new Data(data, maxRevision);
        }

        return new Data(data, ver.get());
    }

    /** {@inheritDoc} */
    @Override public synchronized CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long sentVersion) {
        assert sentVersion <= ver.get();

        if (sentVersion != ver.get())
            // This means that sentVersion is less than version and other node has already updated configuration and
            // write should be retried. Actual version will be set when watch and corresponding configuration listener
            // updates configuration and notifyApplied is triggered afterwards.
            return CompletableFuture.completedFuture(false);

        HashSet<Operation> operations = new HashSet<>();

        for (Map.Entry<String, Serializable> entry : newValues.entrySet()) {
            ByteArray key = new ByteArray(DISTRIBUTED_PREFIX + entry.getKey());

            if (entry.getValue() != null)
                // TODO: investigate overhead when serialize int, long, double, boolean, string, arrays of above
                // TODO: https://issues.apache.org/jira/browse/IGNITE-14698
                operations.add(Operations.put(key, ByteUtils.toBytes(entry.getValue())));
            else
                operations.add(Operations.remove(key));
        }

        operations.add(Operations.put(MASTER_KEY, ByteUtils.longToBytes(sentVersion)));

        if (sentVersion == 0) {
            return metaStorageMgr.invoke(
                Conditions.notExists(MASTER_KEY),
                operations,
                Collections.singleton(Operations.noop()));
        }
        else {
            return metaStorageMgr.invoke(
                Conditions.revision(MASTER_KEY).eq(ver.get()),
                operations,
                Collections.singleton(Operations.noop()));
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void addListener(ConfigurationStorageListener listener) {
        listeners.add(listener);

        if (watchId == null) {
            // TODO: registerWatchByPrefix could throw OperationTimeoutException and CompactedException and we should
            // TODO: properly handle such cases https://issues.apache.org/jira/browse/IGNITE-14604
            watchId = metaStorageMgr.registerWatchByPrefix(MASTER_KEY, new WatchListener() {
                @Override public boolean onUpdate(@NotNull WatchEvent events) {
                    HashMap<String, Serializable> data = new HashMap<>();

                    long maxRevision = 0L;

                    Entry entryForMasterKey = null;

                    for (EntryEvent event : events.entryEvents()) {
                        Entry e = event.newEntry();

                        if (!e.key().equals(MASTER_KEY)) {
                            data.put(e.key().toString().substring((DISTRIBUTED_PREFIX).length()),
                                (Serializable)ByteUtils.fromBytes(e.value()));

                            if (maxRevision < e.revision())
                                maxRevision = e.revision();
                        } else
                            entryForMasterKey = e;
                    }

                    // Contract of meta storage ensures that all updates of one revision will come in one batch.
                    // Also masterKey should be updated every time when we update cfg.
                    // That means that masterKey update must be included in the batch.
                    assert entryForMasterKey != null;

                    assert maxRevision == entryForMasterKey.revision();

                    assert maxRevision >= ver.get();

                    long finalMaxRevision = maxRevision;

                    listeners.forEach(listener -> listener.onEntriesChanged(new Data(data, finalMaxRevision)));

                    return true;
                }

                @Override public void onError(@NotNull Throwable e) {
                    // TODO: need to handle this case and there should some mechanism for registering new watch as far as
                    // TODO: onError unregisters failed watch https://issues.apache.org/jira/browse/IGNITE-14604
                    LOG.error("Meta storage listener issue", e);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void removeListener(ConfigurationStorageListener listener) {
        listeners.remove(listener);

        if (listeners.isEmpty()) {
            try {
                metaStorageMgr.unregisterWatch(watchId.get());
            }
            catch (InterruptedException | ExecutionException e) {
                LOG.error("Failed to unregister watch in meta storage.", e);
            }

            watchId = null;
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void notifyApplied(long storageRevision) {
        assert ver.get() <= storageRevision;

        ver.set(storageRevision);

        // TODO: Also we should persist version,
        // TODO: this should be done when nodes restart is introduced.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-14697
    }

    /** {@inheritDoc} */
    @Override public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    /**
     * Method that returns all distributed configuration keys from meta storage filtered out by the current applied
     * revision as an upper bound. Applied revision is a revision of the last successful vault update.
     * <p>
     * This is possible to distinguish cfg keys from meta storage because we add special prefix {@link
     * DistributedConfigurationStorage#DISTRIBUTED_PREFIX} to all configuration keys that we put to meta storage.
     *
     * @return Cursor built upon all distributed configuration entries.
     */
    private Cursor<Entry> allDistributedConfigKeys() {
        // TODO: rangeWithAppliedRevision could throw OperationTimeoutException and CompactedException and we should
        // TODO: properly handle such cases https://issues.apache.org/jira/browse/IGNITE-14604
        return metaStorageMgr.rangeWithAppliedRevision(MASTER_KEY, DST_KEYS_END_RANGE);
    }
}
