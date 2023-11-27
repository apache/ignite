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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Comparator;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Container for index rebuild status info.
 */
public class IndexRebuildStatusInfoContainer extends IgniteDataTransferObject {
    /** Empty group name. */
    public static final String EMPTY_GROUP_NAME = "no_group";

    /** */
    private static final long serialVersionUID = 0L;

    /** Group name. */
    private String groupName;

    /** Cache name. */
    private String cacheName;

    /** */
    private int indexBuildPartitionsLeftCount;

    /** Local partitions count. */
    private int totalPartitionsCount;

    /**
     * Empty constructor required for Serializable.
     */
    public IndexRebuildStatusInfoContainer() {
        // No-op.
    }

    /** */
    public IndexRebuildStatusInfoContainer(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        CacheConfiguration<?, ?> cfg = cctx.config();

        groupName = cfg.getGroupName() == null ? EMPTY_GROUP_NAME : cfg.getGroupName();
        cacheName = cfg.getName();
        indexBuildPartitionsLeftCount = cctx.cache().metrics0().getIndexBuildPartitionsLeftCount();
        totalPartitionsCount = cctx.topology().localPartitions().size();
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, groupName);
        U.writeString(out, cacheName);
        out.writeInt(indexBuildPartitionsLeftCount);
        out.writeInt(totalPartitionsCount);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        groupName = U.readString(in);
        cacheName = U.readString(in);
        indexBuildPartitionsLeftCount = in.readInt();
        totalPartitionsCount = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (!(o instanceof IndexRebuildStatusInfoContainer))
            return false;

        IndexRebuildStatusInfoContainer other = (IndexRebuildStatusInfoContainer)o;

        return cacheName.equals(other.cacheName) && groupName.equals(other.groupName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return groupName.hashCode() * 17 + cacheName.hashCode() * 37;
    }

    /**
     * @return Group name.
     */
    public String groupName() {
        return groupName;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Total local node partitions count.
     */
    public int totalPartitionsCount() {
        return totalPartitionsCount;
    }

    /**
     * @return The number of local node partitions that remain to be processed to complete indexing.
     */
    public int indexBuildPartitionsLeftCount() {
        return indexBuildPartitionsLeftCount;
    }

    /**
     * @return default string object representation without {@code IndexRebuildStatusInfoContainer} and brackets.
     */
    @Override public String toString() {
        float progress = (float)(totalPartitionsCount - indexBuildPartitionsLeftCount) / totalPartitionsCount;

        String dfltImpl = S.toString(
            IndexRebuildStatusInfoContainer.class,
            this,
            "progress",
            (int)(Math.max(0, progress) * 100) + "%"
        );

        return dfltImpl.substring(IndexRebuildStatusInfoContainer.class.getSimpleName().length() + 2,
            dfltImpl.length() - 1);
    }

    /**
     * @return Custom comparator.
     */
    public static Comparator<IndexRebuildStatusInfoContainer> comparator() {
        return Comparator.comparing(IndexRebuildStatusInfoContainer::groupName)
            .thenComparing(IndexRebuildStatusInfoContainer::cacheName);
    }
}
