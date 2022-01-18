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

package org.apache.ignite.internal.configuration.notifications;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.configuration.tree.NamedListNode;

/**
 * Container for changes to named list keys.
 */
class NamedListChanges {
    /** Keys that have been created. */
    final Set<String> created;

    /** Keys that have been removed. */
    final Set<String> deleted;

    /** Keys that have been renamed. Mapping: old key -> new key. */
    final Map<String, String> renamed;

    /** Keys that have been updated. */
    final Set<String> updated;

    /**
     * Constructor.
     *
     * @param created Keys that have been created.
     * @param deleted Keys that have been removed.
     * @param renamed Keys that have been renamed. Mapping: old key -> new key.
     * @param updated Keys that have been updated.
     */
    NamedListChanges(
            Set<String> created,
            Set<String> deleted,
            Map<String, String> renamed,
            Set<String> updated
    ) {
        this.created = created;
        this.deleted = deleted;
        this.renamed = renamed;
        this.updated = updated;
    }

    /**
     * Collect changes by keys between the old and new named list.
     *
     * @param oldNamedList Old named list.
     * @param newNamedList New named list.
     * @return New instance.
     */
    static NamedListChanges of(NamedListNode<?> oldNamedList, NamedListNode<?> newNamedList) {
        List<String> oldNames = oldNamedList.namedListKeys();
        List<String> newNames = newNamedList.namedListKeys();

        Set<String> created = new HashSet<>(newNames);
        created.removeAll(oldNames);

        Set<String> deleted = new HashSet<>(oldNames);
        deleted.removeAll(newNames);

        Set<String> updated = new HashSet<>(newNames);
        updated.retainAll(oldNames);

        Map<String, String> renamed = new HashMap<>();
        if (!created.isEmpty() && !deleted.isEmpty()) {
            Map<UUID, String> createdIds = new HashMap<>();

            for (String createdKey : created) {
                createdIds.put(newNamedList.internalId(createdKey), createdKey);
            }

            // Avoiding ConcurrentModificationException.
            for (String deletedKey : Set.copyOf(deleted)) {
                UUID internalId = oldNamedList.internalId(deletedKey);

                String maybeRenamedKey = createdIds.get(internalId);

                if (maybeRenamedKey == null) {
                    continue;
                }

                deleted.remove(deletedKey);
                created.remove(maybeRenamedKey);
                renamed.put(deletedKey, maybeRenamedKey);
            }
        }

        return new NamedListChanges(created, deleted, renamed, updated);
    }
}
