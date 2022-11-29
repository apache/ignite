/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Quick partitions verifier. Warns if partiton counters or size are different among the nodes what can be caused by
 * canceled/failed DataStreamer. Skips checking if the DataStreamer warning is detected.
 */
public class SnapshotPartitionsQuickVerifyHandler extends SnapshotPartitionsVerifyHandler {
    /** */
    public static final String WRN_MSG = "This may happen if DataStreamer with property 'allowOverwrite' set " +
        "to `false` is loading during the snapshot or hadn't successfully finished earlier. However, you will be " +
        "able restore rest the caches from this snapshot.";

    /**
     * @param cctx Shared context.
     */
    public SnapshotPartitionsQuickVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        super(cctx);
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.CREATE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKeyV2, PartitionHashRecordV2> invoke(SnapshotHandlerContext opCtx)
        throws IgniteCheckedException {
        // Return null not to check partitions at all if the streamer warning is detected.
        if (opCtx.streamerWarning())
            return null;

        Map<PartitionKeyV2, PartitionHashRecordV2> res = super.invoke(opCtx);

        assert res != null;

        return res;
    }

    /** {@inheritDoc} */
    @Override public void complete(
        String name,
        Collection<SnapshotHandlerResult<Map<PartitionKeyV2, PartitionHashRecordV2>>> results
    ) throws IgniteCheckedException {
        if (results.stream().anyMatch(r -> r.data() == null))
            return;

        Set<Integer> wrnGrps = new HashSet<>();

        Map<PartitionKeyV2, PartitionHashRecordV2> total = new HashMap<>();

        F.viewReadOnly(results, SnapshotHandlerResult::data).forEach(m -> m.forEach((part, val) -> {
            PartitionHashRecordV2 other = total.putIfAbsent(part, val);

            if (other == null)
                return;

            if (val.size() != other.size() || !val.updateCounter().equals(other.updateCounter()))
                wrnGrps.add(part.groupId());
        }));

        if (!wrnGrps.isEmpty()) {
            throw new SnapshotHandlerWarningException("Cache partitions differ for cache groups " + S.toStringSortedDistinct(wrnGrps)
                + ". " + WRN_MSG);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean skipHash() {
        return true;
    }
}
