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

package org.apache.ignite.internal.vault.inmemory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory representation of the Vault Service.
 */
public class InMemoryVaultService implements VaultService {
    /** Map to store values. */
    private final NavigableMap<ByteArray, byte[]> storage = new TreeMap<>();

    /** Mutex. */
    private final Object mux = new Object();

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        // TODO: IGNITE-15161 Implement component's stop.
        close();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public CompletableFuture<VaultEntry> get(@NotNull ByteArray key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(new VaultEntry(key, storage.get(key)));
        }
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public CompletableFuture<Void> put(@NotNull ByteArray key, byte @Nullable [] val) {
        synchronized (mux) {
            storage.put(key, val);

            return CompletableFuture.completedFuture(null);
        }
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public CompletableFuture<Void> remove(@NotNull ByteArray key) {
        synchronized (mux) {
            storage.remove(key);

            return CompletableFuture.completedFuture(null);
        }
    }

    /** {@inheritDoc} */
    @Override
    @NotNull
    public Cursor<VaultEntry> range(@NotNull ByteArray fromKey, @NotNull ByteArray toKey) {
        Iterator<VaultEntry> it;

        if (fromKey.compareTo(toKey) >= 0) {
            it = Collections.emptyIterator();
        } else {
            synchronized (mux) {
                it = storage.subMap(fromKey, toKey).entrySet().stream()
                        .map(e -> new VaultEntry(new ByteArray(e.getKey()), e.getValue()))
                        .collect(Collectors.toList())
                        .iterator();
            }
        }

        return new Cursor<>() {
            @Override
            public void close() {
            }

            @NotNull
            @Override
            public Iterator<VaultEntry> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public VaultEntry next() {
                return it.next();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        synchronized (mux) {
            for (var entry : vals.entrySet()) {
                if (entry.getValue() == null) {
                    storage.remove(entry.getKey());
                } else {
                    storage.put(entry.getKey(), entry.getValue());
                }
            }

            return CompletableFuture.completedFuture(null);
        }
    }
}
