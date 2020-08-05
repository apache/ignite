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

package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Serves for managing encryption keys and related datastructure located in the heap.
 */
class CacheGroupEncryptionKeys {
    /** Group encryption keys. */
    private final Map<Integer, List<GroupKey>> grpKeys = new ConcurrentHashMap<>();

    /** WAL segments encrypted with previous encrypted keys, mapped to cache group encryption key IDs. */
    private final Queue<T3<Long, Integer, Integer>> trackedWalSegments = new ConcurrentLinkedQueue<>();

    /** Encryption spi. */
    private final EncryptionSpi encSpi;

    /**
     * @param encSpi Encryption spi.
     */
    CacheGroupEncryptionKeys(EncryptionSpi encSpi) {
        this.encSpi = encSpi;
    }

    /**
     * Returns group encryption key, that was set for writing.
     *
     * @param grpId Cache group ID.
     * @return Group encryption key with ID, that was set for writing.
     */
    GroupKey getActiveKey(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (F.isEmpty(keys))
            return null;

        return keys.get(0);
    }

    /**
     * Returns group encryption key with specified ID.
     *
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Group encryption key.
     */
    GroupKey getKey(int grpId, int keyId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (keys == null)
            return null;

        for (GroupKey groupKey : keys) {
            if (groupKey.unsignedId() == keyId)
                return groupKey;
        }

        return null;
    }

    /**
     * Gets the existing encryption key IDs for the specified cache group.
     *
     * @param grpId Cache group ID.
     * @return List of the key IDs.
     */
    List<Integer> keyIds(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (keys == null)
            return null;

        List<Integer> keyIds = new ArrayList<>(keys.size());

        for (GroupKey groupKey : keys)
            keyIds.add(groupKey.unsignedId());

        return keyIds;
    }

    /**
     * @return Cache group IDs for which encryption keys are stored.
     */
    Set<Integer> groupIds() {
        return grpKeys.keySet();
    }

    /**
     * @return Local encryption keys.
     */
    @Nullable HashMap<Integer, GroupKeyEncrypted> getAll() {
        if (F.isEmpty(grpKeys))
            return null;

        HashMap<Integer, GroupKeyEncrypted> keys = new HashMap<>();

        for (Map.Entry<Integer, List<GroupKey>> entry : grpKeys.entrySet()) {
            int grpId = entry.getKey();
            GroupKey grpKey = entry.getValue().get(0);

            keys.put(grpId, new GroupKeyEncrypted(grpKey.unsignedId(), encSpi.encryptKey(grpKey.key())));
        }

        return keys;
    }

    /**
     * @param grpId Cache group ID.
     *
     * @return Local encryption keys used for specified cache group.
     */
    List<GroupKeyEncrypted> getAll(int grpId) {
        List<GroupKey> grpKeys = this.grpKeys.get(grpId);

        if (F.isEmpty(grpKeys))
            return null;

        List<GroupKeyEncrypted> encryptedKeys = new ArrayList<>(grpKeys.size());

        for (GroupKey grpKey : grpKeys)
            encryptedKeys.add(new GroupKeyEncrypted(grpKey.unsignedId(), encSpi.encryptKey(grpKey.key())));

        return encryptedKeys;
    }

    /**
     * Put new encryption key and set it for writing.
     *
     * @param grpId Cache group ID.
     * @param newEncKey New encrypted key for writing.
     * @return Previous encryption key for writing.
     */
    GroupKey changeActiveKey(int grpId, GroupKeyEncrypted newEncKey) {
        assert newEncKey != null;

        List<GroupKey> keys = grpKeys.computeIfAbsent(grpId, list -> new CopyOnWriteArrayList<>());

        GroupKey prevKey = F.first(keys);

        GroupKey newKey = new GroupKey(newEncKey.id(), encSpi.decryptKey(newEncKey.key()));

        keys.add(0, newKey);

        // Remove the duplicate key from the tail of the list if exists.
        keys.subList(1, keys.size()).remove(newKey);

        return prevKey;
    }

    /**
     * Put new unused key.
     *
     * @param grpId Cache group ID.
     * @param newEncKey New encrypted key for writing.
     */
    void addKey(int grpId, GroupKeyEncrypted newEncKey) {
        grpKeys.get(grpId).add(new GroupKey(newEncKey.id(), encSpi.decryptKey(newEncKey.key())));
    }

    /**
     * @param grpId Cache group ID.
     * @param encryptedKeys Encrypted keys.
     */
    void setGroupKeys(int grpId, List<GroupKeyEncrypted> encryptedKeys) {
        List<GroupKey> keys = new CopyOnWriteArrayList<>();

        for (GroupKeyEncrypted grpKey : encryptedKeys)
            keys.add(new GroupKey(grpKey.id(), encSpi.decryptKey(grpKey.key())));

        grpKeys.put(grpId, keys);
    }

    /**
     * Remove encrytion keys associated with the specified cache group.
     *
     * @param grpId Cache group ID.
     */
    void remove(int grpId) {
        grpKeys.remove(grpId);
    }

    /**
     * @param grpId Cache group ID.
     * @param ids Key IDs for deletion.
     * @return {@code True} if the keys have been deleted.
     */
    boolean removeKeysById(int grpId, Set<Integer> ids) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (F.isEmpty(keys))
            return false;

        return keys.removeIf(key -> ids.contains(key.unsignedId()));
    }

    /**
     * Remove unused keys.
     *
     * @param grpId Cache group ID.
     * @return Removed key IDs,
     */
    Set<Integer> removeUnusedKeys(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);
        Set<Integer> rmvKeyIds = U.newHashSet(keys.size() - 1);

        rmvKeyIds.addAll(F.viewReadOnly(keys.subList(1, keys.size()), GroupKey::unsignedId));

        for (T3<Long, Integer, Integer> entry : trackedWalSegments) {
            if (entry.get2() != grpId)
                continue;

            rmvKeyIds.remove(entry.get3());
        }

        if (keys.removeIf(key -> rmvKeyIds.contains(key.unsignedId())))
            return rmvKeyIds;

        return Collections.emptySet();
    }

    /**
     * @return WAL segments encrypted with previous encrypted keys, mapped to cache group encryption key IDs.
     */
    Serializable trackedWalSegments() {
        return (Serializable)Collections.unmodifiableCollection(trackedWalSegments);
    }

    /**
     * @param segments WAL segments, mapped to cache group encryption key IDs.
     */
    void trackedWalSegments(Collection<T3<Long, Integer, Integer>> segments) {
        trackedWalSegments.addAll(segments);
    }

    /**
     * Associate WAL segment index with the specified key ID
     * to prevent deletion of that encryption key before deleting the segment.
     *
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @param walIdx WAL segment index.
     */
    void reserveWalKey(int grpId, int keyId, long walIdx) {
        trackedWalSegments.add(new T3<>(walIdx, grpId, keyId));
    }

    /**
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Wal segment index or null if there no segment associated with the specified cache group ID and key ID.
     */
    @Nullable Long reservedSegment(int grpId, int keyId) {
        for (T3<Long, Integer, Integer> entry : trackedWalSegments) {
            if (entry.get2() != grpId)
                continue;

            if (entry.get3() == keyId)
                return entry.get1();
        }

        return null;
    }

    /**
     * Remove all of the segments that are not greater than the specified index.
     *
     * @param walIdx WAL segment index.
     * @return Map of group IDs with key IDs that were associated with removed WAL segments.
     */
    Map<Integer, Set<Integer>> releaseWalKeys(long walIdx) {
        Map<Integer, Set<Integer>> rmvKeys = new HashMap<>();
        Iterator<T3<Long, Integer, Integer>> iter = trackedWalSegments.iterator();

        while (iter.hasNext()) {
            T3<Long, Integer, Integer> entry = iter.next();

            if (entry.get1() > walIdx)
                break;

            iter.remove();

            rmvKeys.computeIfAbsent(entry.get2(), v -> new HashSet<>()).add(entry.get3());
        }

        return rmvKeys;
    }
}
