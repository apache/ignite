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

package org.apache.ignite.internal.vault.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.vault.common.Entry;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;

/**
 * Simple in-memory representation of vault. Only for test purposes.
 */
public class VaultServiceImpl implements VaultService {
    /** Map to store values. */
    private final NavigableMap<ByteArray, byte[]> storage;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Default constructor.
     */
    public VaultServiceImpl() {
        this.storage = new TreeMap<>();
    }

    /** {@inheritDoc} */
    @Override @NotNull public CompletableFuture<Entry> get(@NotNull ByteArray key) {
        synchronized (mux) {
            return CompletableFuture.completedFuture(new Entry(key, storage.get(key)));
        }
    }

    /** {@inheritDoc} */
    @Override @NotNull public CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] val) {
        synchronized (mux) {
            storage.put(key, val);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    @Override @NotNull public CompletableFuture<Void> remove(@NotNull ByteArray key) {
        synchronized (mux) {
            storage.remove(key);

            return CompletableFuture.allOf();
        }
    }

    /** {@inheritDoc} */
    //TODO: use Cursor instead of Iterator https://issues.apache.org/jira/browse/IGNITE-14654
    @Override @NotNull public Iterator<Entry> range(@NotNull ByteArray fromKey, @NotNull ByteArray toKey) {
        synchronized (mux) {
            ArrayList<Entry> res = new ArrayList<>();

            for (Map.Entry<ByteArray, byte[]> e : storage.subMap(fromKey, toKey).entrySet())
                res.add(new Entry(new ByteArray(e.getKey().bytes()), e.getValue().clone()));

            return res.iterator();
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        synchronized (mux) {
            vals.forEach((k, v) -> {
                if (v == null)
                    storage.remove(k);
            });

            storage.putAll(vals.entrySet()
                .stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            return CompletableFuture.allOf();
        }
    }
}
