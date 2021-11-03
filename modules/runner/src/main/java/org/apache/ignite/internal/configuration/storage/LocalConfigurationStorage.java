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

package org.apache.ignite.internal.configuration.storage;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.NotNull;

/**
 * Local configuration storage.
 */
public class LocalConfigurationStorage implements ConfigurationStorage {
    /** Prefix that we add to configuration keys to distinguish them in metastorage. */
    private static final String LOC_PREFIX = "loc-cfg.";

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(LocalConfigurationStorage.class);

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Configuration changes listener. */
    private ConfigurationStorageListener lsnr;

    /** Storage version. */
    private final AtomicLong ver = new AtomicLong(0L);

    /** Start key in range for searching local configuration keys. */
    private static final ByteArray LOC_KEYS_START_RANGE = ByteArray.fromString(LOC_PREFIX);

    /** End key in range for searching local configuration keys. */
    private static final ByteArray LOC_KEYS_END_RANGE = ByteArray.fromString(incrementLastChar(LOC_PREFIX));

    /**
     * Constructor.
     *
     * @param vaultMgr Vault manager.
     */
    public LocalConfigurationStorage(VaultManager vaultMgr) {
        this.vaultMgr = vaultMgr;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Map<String, ? extends Serializable> readAllLatest(String prefix) {
        var rangeStart = new ByteArray(LOC_PREFIX + prefix);

        var rangeEnd = new ByteArray(incrementLastChar(LOC_PREFIX + prefix));

        return readAll(rangeStart, rangeEnd).values();
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Data readAll() throws StorageException {
        return readAll(LOC_KEYS_START_RANGE, LOC_KEYS_END_RANGE);
    }

    /**
     * Retrieves all data, which keys lie in between {@code [rangeStart, rangeEnd)}.
     */
    private Data readAll(ByteArray rangeStart, ByteArray rangeEnd) {
        var data = new HashMap<String, Serializable>();

        try (Cursor<VaultEntry> cursor = vaultMgr.range(rangeStart, rangeEnd)) {
            for (VaultEntry entry : cursor) {
                String key = entry.key().toString().substring(LOC_PREFIX.length());

                byte[] value = entry.value();

                // vault iterator should not return nulls as values
                assert value != null;

                data.put(key, (Serializable) ByteUtils.fromBytes(value));
            }
        } catch (Exception e) {
            throw new StorageException("Exception when closing a Vault cursor", e);
        }

        // TODO: Need to restore version from pds when restart will be developed
        // TODO: https://issues.apache.org/jira/browse/IGNITE-14697
        return new Data(data, ver.get());
    }

    /** {@inheritDoc} */
    @Override
    public synchronized CompletableFuture<Boolean> write(
            Map<String, ? extends Serializable> newValues, long sentVersion
    ) {
        assert lsnr != null : "Configuration listener must be initialized before write.";

        if (sentVersion != ver.get()) {
            return CompletableFuture.completedFuture(false);
        }

        Map<ByteArray, byte[]> data = new HashMap<>();

        for (Map.Entry<String, ? extends Serializable> e : newValues.entrySet()) {
            ByteArray key = ByteArray.fromString(LOC_PREFIX + e.getKey());

            data.put(key, e.getValue() == null ? null : ByteUtils.toBytes(e.getValue()));
        }

        Data entries = new Data(newValues, ver.incrementAndGet());

        // read the 'lsnr' field into a local variable, just in case, to avoid possible race condition on reading
        // it in a lambda below.
        ConfigurationStorageListener localLsnr = lsnr;

        return vaultMgr.putAll(data)
                .thenCompose(v -> localLsnr.onEntriesChanged(entries))
                .thenApply(v -> true);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void registerConfigurationListener(@NotNull ConfigurationStorageListener lsnr) {
        if (this.lsnr == null) {
            this.lsnr = lsnr;
        } else {
            LOG.warn("Configuration listener has already been set.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    /**
     * Increments the last character of the given string.
     */
    private static String incrementLastChar(String str) {
        char lastChar = str.charAt(str.length() - 1);

        return str.substring(0, str.length() - 1) + (char) (lastChar + 1);
    }
}
