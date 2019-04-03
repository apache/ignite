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

package org.apache.ignite.ml.inference.storage.model;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of {@link ModelStorageProvider} based on local {@link ConcurrentHashMap}.
 */
public class LocalModelStorageProvider implements ModelStorageProvider {
    /** Storage of the files and directories. */
    private final ConcurrentMap<String, FileOrDirectory> storage = new ConcurrentHashMap<>();

    /** Storage of the locks. */
    private final ConcurrentMap<String, WeakReference<Lock>> locks = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public FileOrDirectory get(String key) {
        return storage.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(String key, FileOrDirectory file) {
        storage.put(key, file);
    }

    /** {@inheritDoc} */
    @Override public void remove(String key) {
        storage.remove(key);
    }

    /** {@inheritDoc} */
    @Override public Lock lock(String key) {
        Lock lock = new ReentrantLock();
        return locks.computeIfAbsent(key, k -> new WeakReference<>(lock)).get();
    }
}
