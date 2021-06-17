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
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Serves for managing encryption keys and related datastructure located in the heap.
 */
class CacheGroupEncryptionKeys {
    /** Group encryption keys. */
    private final Map<Integer, List<GroupKey>> grpKeys = new ConcurrentHashMap<>();

    /**
     * WAL segments encrypted with previous encryption keys prevent keys from being deleted
     * until the associated segment is deleted.
     */
    private final Collection<TrackedWalSegment> trackedWalSegments = new ConcurrentLinkedQueue<>();

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
    @Nullable GroupKey getActiveKey(int grpId) {
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
    @Nullable GroupKey getKey(int grpId, int keyId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (keys == null)
            return null;

        for (GroupKey grpKey : keys) {
            if (grpKey.unsignedId() == keyId)
                return grpKey;
        }

        return null;
    }

    /**
     * Gets the existing encryption key IDs for the specified cache group.
     *
     * @param grpId Cache group ID.
     * @return List of the key IDs.
     */
    @Nullable List<Integer> keyIds(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (keys == null)
            return null;

        List<Integer> keyIds = new ArrayList<>(keys.size());

        for (GroupKey grpKey : keys)
            keyIds.add(grpKey.unsignedId());

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

        HashMap<Integer, GroupKeyEncrypted> keys = U.newHashMap(grpKeys.size());

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
    @Nullable List<GroupKeyEncrypted> getAll(int grpId) {
        List<GroupKey> grpKeys = this.grpKeys.get(grpId);

        if (F.isEmpty(grpKeys))
            return null;

        List<GroupKeyEncrypted> encryptedKeys = new ArrayList<>(grpKeys.size());

        for (GroupKey grpKey : grpKeys)
            encryptedKeys.add(new GroupKeyEncrypted(grpKey.unsignedId(), encSpi.encryptKey(grpKey.key())));

        return encryptedKeys;
    }

    /**
     * Sets new encryption key for writing.
     *
     * @param grpId Cache group ID.
     * @param keyId ID of the existing encryption key to be set for writing..
     * @return Previous encryption key used for writing.
     */
    GroupKey changeActiveKey(int grpId, int keyId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        assert !F.isEmpty(keys) : "grpId=" + grpId;

        GroupKey prevKey = keys.get(0);

        assert prevKey.unsignedId() != keyId : "keyId=" + keyId;

        GroupKey newKey = null;

        for (ListIterator<GroupKey> itr = keys.listIterator(keys.size()); itr.hasPrevious(); ) {
            GroupKey key = itr.previous();

            if (key.unsignedId() != keyId)
                continue;

            newKey = key;

            break;
        }

        assert newKey != null : "exp=" + keyId + ", act=" + keys;

        keys.add(0, newKey);

        // Remove the duplicate key(s) from the tail of the list.
        keys.subList(1, keys.size()).removeIf(k -> k.unsignedId() == keyId);

        return prevKey;
    }

    /**
     * Adds new encryption key.
     *
     * @param grpId Cache group ID.
     * @param newEncKey New encrypted key for writing.
     * @return {@code True} If a key has been added, {@code False} if the specified key is already present.
     */
    boolean addKey(int grpId, GroupKeyEncrypted newEncKey) {
        List<GroupKey> keys = grpKeys.computeIfAbsent(grpId, v -> new CopyOnWriteArrayList<>());

        GroupKey grpKey = new GroupKey(newEncKey.id(), encSpi.decryptKey(newEncKey.key()));

        if (!keys.contains(grpKey))
            return keys.add(grpKey);

        return false;
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
     * @return List of encryption keys of the removed cache group.
     */
    List<GroupKey> remove(int grpId) {
        return grpKeys.remove(grpId);
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

        return keys.subList(1, keys.size()).removeIf(key -> ids.contains(key.unsignedId()));
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

        for (TrackedWalSegment segment : trackedWalSegments) {
            if (segment.grpId != grpId)
                continue;

            rmvKeyIds.remove(segment.keyId);
        }

        if (keys.removeIf(key -> rmvKeyIds.contains(key.unsignedId())))
            return rmvKeyIds;

        return Collections.emptySet();
    }

    /**
     * @return A collection of tracked (encrypted with previous encryption keys) WAL segments.
     */
    Collection<TrackedWalSegment> trackedWalSegments() {
        return Collections.unmodifiableCollection(trackedWalSegments);
    }

    /**
     * @param segments WAL segments, mapped to cache group encryption key IDs.
     */
    void trackedWalSegments(Collection<TrackedWalSegment> segments) {
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
        trackedWalSegments.add(new TrackedWalSegment(walIdx, grpId, keyId));
    }

    /**
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Wal segment index or null if there no segment associated with the specified cache group ID and key ID.
     */
    @Nullable Long reservedSegment(int grpId, int keyId) {
        for (TrackedWalSegment segment : trackedWalSegments) {
            if (segment.grpId != grpId)
                continue;

            if (segment.keyId == keyId)
                return segment.idx;
        }

        return null;
    }

    /**
     * @return {@code True} if any key reserved for WAL reading can be removed.
     */
    boolean isReleaseWalKeysRequired(long walIdx) {
        Iterator<TrackedWalSegment> iter = trackedWalSegments.iterator();

        return iter.hasNext() && iter.next().idx <= walIdx;
    }

    /**
     * Remove all of the segments that are not greater than the specified index.
     *
     * @param walIdx WAL segment index.
     * @return Map of group IDs with key IDs that were associated with removed WAL segments.
     */
    Map<Integer, Set<Integer>> releaseWalKeys(long walIdx) {
        Map<Integer, Set<Integer>> rmvKeys = new HashMap<>();
        Iterator<TrackedWalSegment> iter = trackedWalSegments.iterator();

        while (iter.hasNext()) {
            TrackedWalSegment segment = iter.next();

            if (segment.idx > walIdx)
                break;

            iter.remove();

            rmvKeys.computeIfAbsent(segment.grpId, v -> new HashSet<>()).add(segment.keyId);
        }

        return rmvKeys;
    }

    /**
     * A WAL segment encrypted with a specific encryption key ID.
     */
    protected static final class TrackedWalSegment implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** WAL segment index. */
        private final long idx;

        /** Cache group ID. */
        private final int grpId;

        /** Encryption key ID. */
        private final int keyId;

        /**
         * @param idx WAL segment index.
         * @param grpId Cache group ID.
         * @param keyId Encryption key ID.
         */
        public TrackedWalSegment(long idx, int grpId, int keyId) {
            this.idx = idx;
            this.grpId = grpId;
            this.keyId = keyId;
        }
    }
}
