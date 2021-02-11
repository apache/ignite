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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
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
    private final Map<String, WeakReferenceWithCleanUp> locks = new HashMap<>();

    /** Reference queue with reference to be cleaned up. */
    private final ReferenceQueue<Lock> refQueue = new ReferenceQueue<>();

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
    @Override public synchronized Lock lock(String key) {
        WeakReferenceWithCleanUp ref = locks.get(key);
        try {
            if (ref != null) {
                Lock lockInRef = ref.get();

                // Reference is not empty and it couldn't be emptied because object is reachable from "lockInRef".
                if (lockInRef != null)
                    return lockInRef;
            }

            // If reference doesn't exists or it's empty we create a new one.
            Lock lock = new ReentrantLock();
            locks.put(key, new WeakReferenceWithCleanUp(key, lock));
            return lock;
        }
        finally {
            // At this point we already replaced all keys we wanted to replace, so all empty references could be safely
            // deleted.
            while ((ref = (WeakReferenceWithCleanUp)refQueue.poll()) != null) {
                // We double check that we don't replaced the key value already.
                locks.remove(ref.key, ref);
            }
        }
    }

    /**
     * Weak reference with clean up. Allows to clean up key associated with weak reference content.
     */
    private class WeakReferenceWithCleanUp extends WeakReference<Lock> {
        /** Key to be cleaned up. */
        private final String key;

        /**
         * Constructs a new instance of weak reference with clean up.
         *
         * @param key Key to be cleaned up.
         * @param referent Reference containing a lock.
         */
        public WeakReferenceWithCleanUp(String key, Lock referent) {
            super(referent, refQueue);
            this.key = key;
        }
    }
}
