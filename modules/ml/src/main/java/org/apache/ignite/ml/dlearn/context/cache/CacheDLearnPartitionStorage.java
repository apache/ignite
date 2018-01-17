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

package org.apache.ignite.ml.dlearn.context.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;

/**
 * D-learn partition storage based on Ignite cache.
 */
public class CacheDLearnPartitionStorage implements DLearnPartitionStorage {
    /**
     * Storage.
     */
    private final IgniteCache<DLearnContextPartitionKey, byte[]> learningCtxCache;

    /** */
    private final UUID learningCtxId;

    /** */
    private final int part;

    /** */
    public CacheDLearnPartitionStorage(IgniteCache<DLearnContextPartitionKey,
        byte[]> learningCtxCache, UUID learningCtxId, int part) {
        this.learningCtxCache = learningCtxCache;
        this.learningCtxId = learningCtxId;
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public <T> void put(String key, T val) {
        learningCtxCache.put(new DLearnContextPartitionKey(part, learningCtxId, key), serialize(val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T get(String key) {
        return (T) deserialize(learningCtxCache.localPeek(new DLearnContextPartitionKey(part, learningCtxId, key)));
    }

    /** {@inheritDoc} */
    @Override public void remove(String key) {
        learningCtxCache.remove(new DLearnContextPartitionKey(part, learningCtxId, key));
    }

    /** */
    private byte[] serialize(Object obj) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);

            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private Object deserialize(byte[] arr) {
        if (arr == null)
            return null;

        try (ByteArrayInputStream bais = new ByteArrayInputStream(arr);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
