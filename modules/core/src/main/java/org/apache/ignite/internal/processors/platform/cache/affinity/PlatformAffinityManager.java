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

package org.apache.ignite.internal.processors.platform.cache.affinity;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * AffinityManager wrapper for platforms.
 */
public class PlatformAffinityManager extends PlatformAbstractTarget {
    /** */
    public static final int OP_IS_ASSIGNMENT_VALID = 1;

    /** Affinity manager. */
    private final GridCacheAffinityManager affMgr;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    public PlatformAffinityManager(PlatformContext platformCtx, int cacheId) {
        super(platformCtx);

        GridCacheContext<Object, Object> ctx = platformCtx.kernalContext().cache().context().cacheContext(cacheId);

        if (ctx == null)
            throw new IgniteException("Cache doesn't exist: " + cacheId);

        affMgr = ctx.affinity();
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        if (type == OP_IS_ASSIGNMENT_VALID)
        {
            AffinityTopologyVersion ver = new AffinityTopologyVersion(reader.readLong(), reader.readInt());
            int part = reader.readInt();
            AffinityTopologyVersion endVer = affMgr.affinityTopologyVersion();

            if (!affMgr.primaryChanged(part, ver, endVer)) {
                return TRUE;
            }

            if (!affMgr.partitionLocalNode(part, endVer)) {
                return FALSE;
            }

            // Special case: late affinity assignment when primary changes to local node due to a node join.
            // Specified partition is local, and near cache entries are valid for primary keys.
            return ver.topologyVersion() == endVer.topologyVersion() ? TRUE : FALSE;
        }

        return super.processInStreamOutLong(type, reader);
    }
}
