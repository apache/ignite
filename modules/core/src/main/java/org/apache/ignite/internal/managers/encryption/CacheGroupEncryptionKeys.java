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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.pagemem.wal.record.MasterKeyChangeRecord;
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

    /** WAL segments encrypted with previous encrypted keys, mapped to cache group encryption key identifiers. */
    private final Map<Long, Map<Integer, Set<Integer>>> trackedWalSegments = new ConcurrentSkipListMap<>();

    /** Encryption spi. */
    private final EncryptionSpi encSpi;

    /**
     * @param encSpi Encryption spi.
     */
    CacheGroupEncryptionKeys(EncryptionSpi encSpi) {
        this.encSpi = encSpi;
    }

    /**
     * Returns group encryption key.
     *
     * @param grpId Cache group ID.
     * @return Group encryption key with identifier, that was set for writing.
     */
    GroupKey get(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (F.isEmpty(keys))
            return null;

        return keys.get(0);
    }

    /**
     * Returns group encryption key with specified identifier.
     *
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Group encryption key.
     */
    GroupKey get(int grpId, int keyId) {
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
     * Gets information about existing encryption keys for the specified cache group.
     *
     * @param grpId Cache group ID.
     * @return Map of the key identifier with hash code of encryption key.
     */
    Map<Integer, Integer> info(int grpId) {
        List<GroupKey> keys = grpKeys.get(grpId);

        if (keys == null)
            return null;

        Map<Integer, Integer> keysInfo = new LinkedHashMap<>();

        for (GroupKey groupKey : keys)
            keysInfo.put(groupKey.unsignedId(), Arrays.hashCode(U.toBytes(groupKey.key())));

        return keysInfo;
    }

    /**
     * @return Local encryption keys.
     */
    @Nullable HashMap<Integer, GroupKeyEncrypted> getAll() {
        if (F.isEmpty(grpKeys))
            return null;

        HashMap<Integer, GroupKeyEncrypted> Keys = new HashMap<>();

        for (Map.Entry<Integer, List<GroupKey>> entry : grpKeys.entrySet()) {
            int grpId = entry.getKey();
            GroupKey grpKey = entry.getValue().get(0);

            Keys.put(grpId, new GroupKeyEncrypted(grpKey.unsignedId(), encSpi.encryptKey(grpKey.key())));
        }

        return Keys;
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
     * @param grpId Cache group ID.
     * @param newKey New encrypted key for writing.
     * @return Previous encryption key for writing.
     */
    GroupKey put(int grpId, GroupKeyEncrypted newKey) {
        assert newKey != null;

        List<GroupKey> keys = grpKeys.computeIfAbsent(grpId, list -> new CopyOnWriteArrayList<>());

        if (keys == null)
            return null;

        GroupKey prevKey = F.first(keys);

        keys.add(0, new GroupKey(newKey.id(), encSpi.decryptKey(newKey.key())));

        return prevKey;
    }

    /**
     * @param grpId Cache group ID.
     * @param encryptedKeys Encrypted keys.
     */
    void put(int grpId, List<GroupKeyEncrypted> encryptedKeys) {
        List<GroupKey> keys = new CopyOnWriteArrayList<>();

        for (GroupKeyEncrypted encrKey : encryptedKeys)
            keys.add(new GroupKey(encrKey.id(), encSpi.decryptKey(encrKey.key())));

        grpKeys.put(grpId, keys);
    }

    /**
     * @return Cache group identifiers for which encryption keys are stored.
     */
    Set<Integer> groups() {
        return grpKeys.keySet();
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
     * Convert encryption keys to WAL logical record that stores encryption keys.
     */
    MasterKeyChangeRecord toMasterKeyChangeRecord() {
        List<T3<Integer, Byte, byte[]>> reencryptedKeys = new ArrayList<>();

        for (Map.Entry<Integer, List<GroupKey>> entry : grpKeys.entrySet()) {
            int grpId = entry.getKey();

            for (GroupKey grpKey : entry.getValue()) {
                byte keyId = grpKey.id();
                byte[] encryptedKey = encSpi.encryptKey(grpKey.key());

                reencryptedKeys.add(new T3<>(grpId, keyId, encryptedKey));
            }
        }

        return new MasterKeyChangeRecord(encSpi.getMasterKeyName(), reencryptedKeys);
    }

    /**
     * Load encryption keys from WAL logical record that stores encryption keys.
     *
     * @param rec Logical record that stores encryption keys.
     */
    void fromMasterKeyChangeRecord(MasterKeyChangeRecord rec) {
        for (T3<Integer, Byte, byte[]> entry : rec.getGrpKeys()) {
            int grpId = entry.get1();
            int keyId = entry.get2() & 0xff;
            byte[] key = entry.get3();

            grpKeys.computeIfAbsent(grpId, list ->
                new CopyOnWriteArrayList<>()).add(new GroupKey(keyId, encSpi.decryptKey(key)));
        }
    }

    /**
     * @param grpId Cache group ID.
     * @param ids Key identifiers for deletion.
     * @return {@code True} if the keys have been deleted.
     */
    boolean removeKeysById(int grpId, Set<Integer> ids) {
        return removeKeysById(grpKeys.get(grpId), ids);
    }

    /**
     * @param keys Encryption keys.
     * @param ids Key identifiers for deletion.
     * @return {@code True} if encryption keys have been modified.
     */
    private boolean removeKeysById(List<GroupKey> keys, Set<Integer> ids) {
        List<GroupKey> rmvGrpKeys = new ArrayList<>();

        for (GroupKey groupKey : keys) {
            if (ids.contains(groupKey.unsignedId()))
                rmvGrpKeys.add(groupKey);
        }

        return keys.removeAll(rmvGrpKeys);
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

        for (Map<Integer, Set<Integer>> map : trackedWalSegments.values()) {
            Set<Integer> reservedKeyIds = map.get(grpId);

            if (reservedKeyIds != null)
                rmvKeyIds.removeAll(reservedKeyIds);
        }

        if (removeKeysById(keys, rmvKeyIds))
            return rmvKeyIds;

        return Collections.emptySet();
    }

    /**
     * @return WAL segments encrypted with previous encrypted keys, mapped to cache group encryption key identifiers.
     */
    Serializable trackedWalSegments() {
        return (Serializable)Collections.unmodifiableMap(trackedWalSegments);
    }

    /**
     * @param segments WAL segments, mapped to cache group encryption key identifiers.
     */
    void trackedWalSegments(Map<Long, Map<Integer, Set<Integer>>> segments) {
        trackedWalSegments.putAll(segments);
    }

    /**
     * Associate WAL segment index with the specified key identifier
     * to prevent deletion of that encryption key before deleting the segment.
     *
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @param walIdx WAL segment index.
     */
    void reserveWalKey(int grpId, int keyId, long walIdx) {
        trackedWalSegments.computeIfAbsent(walIdx, map -> new HashMap<>())
            .computeIfAbsent(grpId, set -> new HashSet<>()).add(keyId);
    }

    /**
     * @param grpId Cache group ID.
     * @param keyId Encryption key ID.
     * @return Wal segment index or null if there no segment associated with the specified cache group ID and key ID.
     */
    @Nullable Long reservedSegment(int grpId, int keyId) {
        for (Map.Entry<Long, Map<Integer, Set<Integer>>> entry : trackedWalSegments.entrySet()) {
            Set<Integer> keys = entry.getValue().get(grpId);

            if (keys != null && keys.contains(keyId))
                return entry.getKey();
        }

        return null;
    }

    /**
     * Remove all of the segments that are not greater than the specified index.
     *
     * @param walIdx WAL segment index.
     * @return Map of group IDs with key IDs that were associated with removed WAL segments.
     */
    @Nullable Map<Integer, Set<Integer>> releaseWalKeys(long walIdx) {
        Map<Integer, Set<Integer>> rmvKeys = null;
        Iterator<Map.Entry<Long, Map<Integer, Set<Integer>>>> iter = trackedWalSegments.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<Long, Map<Integer, Set<Integer>>> entry = iter.next();

            if (entry.getKey() > walIdx)
                break;

            iter.remove();

            Map<Integer, Set<Integer>> grpKeys = entry.getValue();

            if (rmvKeys == null) {
                rmvKeys = grpKeys;

                continue;
            }

            for (Map.Entry<Integer, Set<Integer>> e : grpKeys.entrySet()) {
                rmvKeys.merge(e.getKey(), e.getValue(), (set1, set2) -> {
                    set1.addAll(set2);

                    return set1;
                });
            }
        }

        return rmvKeys;
    }
}
