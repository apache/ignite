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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * Path IDs abstraction. Contains path and corresponding IDs.
 */
public class IgfsPathIds {
    /** Original path. */
    private final IgfsPath path;

    /** Path parts. */
    private final String[] parts;

    /** IDs. */
    private final IgniteUuid[] ids;

    /** Surrogate IDs for paths which doesn't exist yet. Initialized on demand. */
    private IgniteUuid[] surrogateIds;

    /** Index of last existing ID. */
    private final int lastExistingIdx;

    /**
     * Constructor.
     *
     * @param path Path.
     * @param parts Path parts.
     * @param ids IDs.
     */
    public IgfsPathIds(IgfsPath path, String[] parts, IgniteUuid[] ids) {
        assert path != null;
        assert parts.length == ids.length;

        this.path = path;
        this.parts = parts;
        this.ids = ids;

        int lastExistingIdx0 = -1;

        for (int i = parts.length - 1; i >= 0; i--) {
            if (ids[i] != null) {
                lastExistingIdx0 = i;

                break;
            }
        }

        assert lastExistingIdx0 >= 0;

        lastExistingIdx = lastExistingIdx0;
    }

    /**
     * Get parent entity.
     *
     * @return Parent entity.
     */
    public IgfsPathIds parent() {
        assert ids.length > 1;

        String[] parentParts = new String[parts.length - 1];
        IgniteUuid[] parentIds = new IgniteUuid[ids.length - 1];

        System.arraycopy(parts, 0, parentParts, 0, parentParts.length);
        System.arraycopy(ids, 0, parentIds, 0, parentIds.length);

        return new IgfsPathIds(path.parent(), parentParts, parentIds);
    }

    /**
     * Get number of elements.
     *
     * @return ID count.
     */
    public int count() {
        return ids.length;
    }

    /**
     * Get original path.
     *
     * @return Path.
     */
    public IgfsPath path() {
        return path;
    }

    /**
     * Get path part at the given index.
     *
     * @param idx Index.
     * @return Path part.
     */
    public String part(int idx) {
        assert idx < parts.length;

        return parts[idx];
    }

    /**
     * Get last part of original path.
     *
     * @return Last part.
     */
    public String lastPart() {
        return parts[parts.length - 1];
    }

    /**
     * Get last ID.
     *
     * @return Last ID.
     */
    public IgniteUuid lastId() {
        return ids[ids.length - 1];
    }

    /**
     * Get last parent ID.
     *
     * @return Last parent ID.
     */
    @Nullable public IgniteUuid lastParentId() {
        return ids[ids.length - 2];
    }

    /**
     * Whether provided index denotes last entry in the path.
     *
     * @param idx Index.
     * @return {@code True} if last.
     */
    public boolean isLastIndex(int idx) {
        return idx == parts.length - 1;
    }

    /**
     * Get path of the last existing element.
     *
     * @return Path of the last existing element.
     */
    public IgfsPath lastExistingPath() {
        IgfsPath path = IgfsPath.ROOT;

        for (int i = 1; i <= lastExistingIdx; i++)
            path = new IgfsPath(path, parts[i]);

        return path;
    }

    /**
     * Whether all parts exists.
     *
     * @return {@code True} if all parts were found.
     */
    public boolean allExists() {
        return parts.length == lastExistingIdx + 1;
    }

    /**
     * Whether last entry exists.
     *
     * @return {@code True} if exists.
     */
    public boolean lastExists() {
        return lastExistingIdx == ids.length - 1;
    }

    /**
     * Whether parent of the last entry exists.
     *
     * @return {@code True} if exists.
     */
    public boolean lastParentExists() {
        return ids.length > 1 && lastExistingIdx == ids.length - 2;
    }

    /**
     * Get ID of the last existing entry.
     *
     * @return ID of the last existing entry.
     */
    public IgniteUuid lastExistingId() {
        return ids[lastExistingIdx];
    }

    /**
     * Get index of the last existing entry.
     *
     * @return Index of the last existing entry.
     */
    public int lastExistingIndex() {
        return lastExistingIdx;
    }

    /**
     * Add existing IDs to provided collection.
     *
     * @param col Collection.
     * @param relaxed Relaxed mode flag.
     */
    @SuppressWarnings("ManualArrayToCollectionCopy")
    public void addExistingIds(Collection<IgniteUuid> col, boolean relaxed) {
        if (relaxed) {
            col.add(ids[lastExistingIdx]);

            if (lastExistingIdx == ids.length - 1 && lastExistingIdx > 0)
                col.add(ids[lastExistingIdx - 1]);
        }
        else {
            for (int i = 0; i <= lastExistingIdx; i++)
                col.add(ids[i]);
        }
    }

    /**
     * Add surrogate IDs to provided collection potentially creating them on demand.
     *
     * @param col Collection.
     */
    @SuppressWarnings("ManualArrayToCollectionCopy")
    public void addSurrogateIds(Collection<IgniteUuid> col) {
        if (surrogateIds == null) {
            surrogateIds = new IgniteUuid[ids.length];

            for (int i = lastExistingIdx + 1; i < surrogateIds.length; i++)
                surrogateIds[i] = IgniteUuid.randomUuid();
        }

        for (int i = lastExistingIdx + 1; i < surrogateIds.length; i++)
            col.add(surrogateIds[i]);
    }

    /**
     * Get ID at the give index.
     *
     * @param idx Index.
     * @return ID.
     */
    public IgniteUuid id(int idx) {
        return idx <= lastExistingIdx ? ids[idx] : surrogateId(idx);
    }

    /**
     * Get surrogate ID at the given index.
     *
     * @param idx Index.
     * @return Surrogate ID.
     */
    public IgniteUuid surrogateId(int idx) {
        assert surrogateIds != null;

        assert idx > lastExistingIdx;
        assert idx < surrogateIds.length;

        return surrogateIds[idx];
    }

    /**
     * Verify that observed paths are found in provided infos in the right order.
     *
     * @param infos Info.
     * @param relaxed Whether to perform check in relaxed mode.
     * @return {@code True} if full integrity is preserved.
     */
    public boolean verifyIntegrity(Map<IgniteUuid, IgfsEntryInfo> infos, boolean relaxed) {
        if (relaxed) {
            // Relaxed mode ensures that the last element is there. If this element is the last in the path, then
            // existence of it's parent and link between them are checked as well.
            IgfsEntryInfo info = infos.get(ids[lastExistingIdx]);

            if (info == null)
                return false;

            if (lastExistingIdx == ids.length - 1 && lastExistingIdx > 0) {
                IgfsEntryInfo parentInfo = infos.get(ids[lastExistingIdx - 1]);

                if (parentInfo == null || !parentInfo.hasChild(parts[lastExistingIdx], ids[lastExistingIdx]))
                    return false;
            }
        }
        else {
            // Strict mode ensures that all participants are in place and are still linked.
            for (int i = 0; i <= lastExistingIdx; i++) {
                IgfsEntryInfo info = infos.get(ids[i]);

                // Check if required ID is there.
                if (info == null)
                    return false;

                // For non-leaf entry we check if child exists.
                if (i < lastExistingIdx) {
                    if (!info.hasChild(parts[i + 1], ids[i + 1]))
                        return false;
                }
            }
        }

        return true;
    }
}
